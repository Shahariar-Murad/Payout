
from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
import numpy as np

TOLERANCE_MINUTES_DEFAULT = 15

def _to_tz_aware(series: pd.Series, source_tz: str) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce", utc=False)
    if getattr(s.dt, "tz", None) is not None:
        return s.dt.tz_convert("UTC")
    return s.dt.tz_localize(source_tz).dt.tz_convert("UTC")

def _safe_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype(float)

def _clean_id(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()

def bucket_3h(ts_report: pd.Series) -> pd.Series:
    return ts_report.dt.floor("3H")

def plan_category(plan_series: pd.Series) -> pd.Series:
    s = plan_series.astype(str).str.lower()
    return np.where(s.str.contains("futures", na=False), "Futures", "CFD")

def is_automation(internal_status: pd.Series) -> pd.Series:
    s = internal_status.astype(str).str.lower()
    return s.str.contains("automation", na=False)

def extract_rise_id(description: pd.Series) -> pd.Series:
    # Example: "... riseid: 0xABC... on tx: ..."
    pat = r"riseid:\s*(0x[a-fA-F0-9]{40})"
    extracted = description.astype(str).str.extract(pat, expand=False)
    return extracted.astype(str).str.strip().str.upper()

@dataclass
class ChannelResult:
    name: str
    matched: pd.DataFrame
    late_sync: pd.DataFrame
    missing_true: pd.DataFrame
    summary_3h: pd.DataFrame  # by backend time bucket

def reconcile_with_tolerance(
    backend_df: pd.DataFrame,
    wallet_df: pd.DataFrame,
    backend_ts_col: str,
    backend_tz: str,
    backend_id_col: str,
    backend_amount_col: str,
    wallet_ts_col: str,
    wallet_tz: str,
    wallet_id_col: str,
    wallet_amount_col: str,
    report_tz: str,
    report_start: pd.Timestamp,
    report_end: pd.Timestamp,
    tolerance_minutes: int = TOLERANCE_MINUTES_DEFAULT,
    wallet_id_extractor=None,
    channel_name: str = "channel",
) -> ChannelResult:
    b = backend_df.copy()
    w = wallet_df.copy()

    # IDs
    b["txn_id"] = _clean_id(b[backend_id_col])
    if wallet_id_extractor is not None:
        w["txn_id"] = _clean_id(wallet_id_extractor(w))
    else:
        w["txn_id"] = _clean_id(w[wallet_id_col])

    # Times
    b["ts_utc"] = _to_tz_aware(b[backend_ts_col], backend_tz)
    w["ts_utc"] = _to_tz_aware(w[wallet_ts_col], wallet_tz)

    # Amounts
    b["amount_backend"] = _safe_float(b[backend_amount_col])
    w["amount_wallet"] = _safe_float(w[wallet_amount_col])

    # Convert to report tz for windowing and delay calc
    b["ts_report_backend"] = b["ts_utc"].dt.tz_convert(report_tz)
    w["ts_report_wallet"] = w["ts_utc"].dt.tz_convert(report_tz)

    # Window by backend time (business view)
    b_day = b[(b["ts_report_backend"] >= report_start) & (b["ts_report_backend"] < report_end)].copy()

    merged = b_day.merge(
        w[["txn_id", "ts_report_wallet", "amount_wallet"]],
        on="txn_id",
        how="left",
    )

    merged["delay_min"] = (merged["ts_report_backend"] - merged["ts_report_wallet"]).dt.total_seconds() / 60
    merged["amount_diff"] = merged["amount_backend"] - merged["amount_wallet"]

    matched = merged[(merged["ts_report_wallet"].notna()) & (merged["delay_min"].abs() <= tolerance_minutes)].copy()
    late_sync = merged[(merged["ts_report_wallet"].notna()) & (merged["delay_min"].abs() > tolerance_minutes)].copy()
    missing_true = merged[merged["ts_report_wallet"].isna()].copy()

    for df in (matched, late_sync, missing_true):
        df["bucket_3h"] = bucket_3h(df["ts_report_backend"])

    # Summary by 3h buckets (matched only for amounts; missing_count from missing_true)
    summary = (
        matched.groupby("bucket_3h")
        .agg(
            matched_count=("txn_id", "count"),
            backend_total=("amount_backend", "sum"),
            wallet_total=("amount_wallet", "sum"),
            diff_total=("amount_diff", "sum"),
            abs_diff_total=("amount_diff", lambda s: float(np.nansum(np.abs(s)))),
        )
        .reset_index()
    )

    miss_cnt = (
        missing_true.groupby("bucket_3h")
        .agg(missing_count=("txn_id", "count"))
        .reset_index()
    )

    late_cnt = (
        late_sync.groupby("bucket_3h")
        .agg(late_sync_count=("txn_id", "count"))
        .reset_index()
    )

    summary = summary.merge(miss_cnt, on="bucket_3h", how="left").merge(late_cnt, on="bucket_3h", how="left")
    summary["missing_count"] = summary["missing_count"].fillna(0).astype(int)
    summary["late_sync_count"] = summary["late_sync_count"].fillna(0).astype(int)

    # Ensure all buckets in the day appear
    all_buckets = pd.date_range(start=report_start, end=report_end, freq="3H", inclusive="left").tz_convert(report_tz)
    all_df = pd.DataFrame({"bucket_3h": all_buckets})
    summary = all_df.merge(summary, on="bucket_3h", how="left")
    for c in ["matched_count","missing_count","late_sync_count"]:
        summary[c] = summary[c].fillna(0).astype(int)
    for c in ["backend_total","wallet_total","diff_total","abs_diff_total"]:
        summary[c] = summary[c].fillna(0.0)

    summary = summary.sort_values("bucket_3h")

    return ChannelResult(
        name=channel_name,
        matched=matched,
        late_sync=late_sync,
        missing_true=missing_true,
        summary_3h=summary,
    )

def excel_safe(df: pd.DataFrame, report_tz: str) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64tz_dtype(out[col]):
            out[col] = out[col].dt.tz_convert(report_tz).dt.tz_localize(None)
    return out

def export_excel(filepath: str, report_tz: str, rise: ChannelResult, crypto: ChannelResult, segment_summary: pd.DataFrame, counts_3h: pd.DataFrame) -> None:
    with pd.ExcelWriter(filepath, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss") as writer:
        excel_safe(rise.summary_3h, report_tz).to_excel(writer, index=False, sheet_name="Rise_3H")
        excel_safe(crypto.summary_3h, report_tz).to_excel(writer, index=False, sheet_name="Crypto_3H")

        excel_safe(rise.matched, report_tz).to_excel(writer, index=False, sheet_name="Rise_Matched")
        excel_safe(rise.late_sync, report_tz).to_excel(writer, index=False, sheet_name="Rise_LateSync")
        excel_safe(rise.missing_true, report_tz).to_excel(writer, index=False, sheet_name="Rise_Missing")

        excel_safe(crypto.matched, report_tz).to_excel(writer, index=False, sheet_name="Crypto_Matched")
        excel_safe(crypto.late_sync, report_tz).to_excel(writer, index=False, sheet_name="Crypto_LateSync")
        excel_safe(crypto.missing_true, report_tz).to_excel(writer, index=False, sheet_name="Crypto_Missing")

        excel_safe(segment_summary, report_tz).to_excel(writer, index=False, sheet_name="Segment_Summary")
        excel_safe(counts_3h, report_tz).to_excel(writer, index=False, sheet_name="Counts_3H")

        readme = pd.DataFrame([
            {"Key":"Tolerance","Value":f"{TOLERANCE_MINUTES_DEFAULT} minutes (default)"},
            {"Key":"Crypto match","Value":"Backend Transaction ID == Crypto report Tracking ID"},
            {"Key":"Rise match","Value":"Backend Payment method ID == extracted riseid from Rise Description"},
            {"Key":"Futures vs CFD","Value":"Backend Plan contains 'futures' => Futures else CFD"},
            {"Key":"Payout automation","Value":"Backend Internal Status contains 'automation'"},
        ])
        readme.to_excel(writer, index=False, sheet_name="README")
