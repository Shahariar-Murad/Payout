
from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
import numpy as np

def _to_utc(series: pd.Series, source_tz: str) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce", utc=False)
    if getattr(s.dt, "tz", None) is not None:
        return s.dt.tz_convert("UTC")
    return s.dt.tz_localize(source_tz).dt.tz_convert("UTC")

def _clean_id(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()

def _safe_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype(float)

def bucket_3h(ts_report: pd.Series) -> pd.Series:
    return ts_report.dt.floor("3H")

def plan_category(plan: pd.Series) -> pd.Series:
    s = plan.astype(str).str.lower()
    return np.where(s.str.contains("futures", na=False), "Futures", "CFD")

def is_automation(internal_status: pd.Series) -> pd.Series:
    return internal_status.astype(str).str.lower().str.contains("automation", na=False)

@dataclass
class ReconResult:
    matched: pd.DataFrame
    late_sync: pd.DataFrame
    missing_true: pd.DataFrame
    summary_3h: pd.DataFrame

def _build_summary(matched: pd.DataFrame, late_sync: pd.DataFrame, missing_true: pd.DataFrame, report_start: pd.Timestamp, report_end: pd.Timestamp, report_tz: str) -> pd.DataFrame:
    if matched.empty:
        summary = pd.DataFrame(columns=["bucket_3h","matched_count","backend_total","wallet_total","diff_total","abs_diff_total"])
    else:
        summary = (
            matched.groupby("bucket_3h")
            .agg(
                matched_count=("txn_id","count"),
                backend_total=("amount_backend","sum"),
                wallet_total=("amount_wallet","sum"),
                diff_total=("amount_diff","sum"),
                abs_diff_total=("amount_diff", lambda s: float(np.nansum(np.abs(s)))),
            )
            .reset_index()
        )

    miss = missing_true.groupby("bucket_3h").size().reset_index(name="missing_count") if not missing_true.empty else pd.DataFrame(columns=["bucket_3h","missing_count"])
    late = late_sync.groupby("bucket_3h").size().reset_index(name="late_sync_count") if not late_sync.empty else pd.DataFrame(columns=["bucket_3h","late_sync_count"])

    summary = summary.merge(miss, on="bucket_3h", how="outer").merge(late, on="bucket_3h", how="outer")

    all_buckets = pd.date_range(start=report_start, end=report_end, freq="3H", inclusive="left").tz_convert(report_tz)
    all_df = pd.DataFrame({"bucket_3h": all_buckets})
    summary = all_df.merge(summary, on="bucket_3h", how="left").sort_values("bucket_3h")

    for c in ["matched_count","missing_count","late_sync_count"]:
        summary[c] = summary[c].fillna(0).astype(int)
    for c in ["backend_total","wallet_total","diff_total","abs_diff_total"]:
        summary[c] = summary[c].fillna(0.0)

    return summary

def reconcile_exact(
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
    tolerance_minutes: int = 15,
) -> ReconResult:
    b = backend_df.copy()
    w = wallet_df.copy()

    b["txn_id"] = _clean_id(b[backend_id_col])
    w["txn_id"] = _clean_id(w[wallet_id_col])

    b["ts_utc"] = _to_utc(b[backend_ts_col], backend_tz)
    w["ts_utc"] = _to_utc(w[wallet_ts_col], wallet_tz)

    b["ts_report_backend"] = b["ts_utc"].dt.tz_convert(report_tz)
    w["ts_report_wallet"] = w["ts_utc"].dt.tz_convert(report_tz)

    b["amount_backend"] = _safe_float(b[backend_amount_col])
    w["amount_wallet"] = _safe_float(w[wallet_amount_col])

    b_win = b[(b["ts_report_backend"] >= report_start) & (b["ts_report_backend"] < report_end)].copy()

    merged = b_win.merge(
        w[["txn_id","ts_report_wallet","amount_wallet"]],
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

    summary_3h = _build_summary(matched, late_sync, missing_true, report_start, report_end, report_tz)
    return ReconResult(matched, late_sync, missing_true, summary_3h)

def reconcile_rise_substring(
    backend_df: pd.DataFrame,
    rise_df: pd.DataFrame,
    backend_ts_col: str,
    backend_tz: str,
    backend_id_col: str,
    backend_amount_col: str,
    rise_ts_col: str,
    rise_tz: str,
    rise_desc_col: str,
    rise_amount_col: str,
    report_tz: str,
    report_start: pd.Timestamp,
    report_end: pd.Timestamp,
    tolerance_minutes: int = 15,
) -> ReconResult:
    b = backend_df.copy()
    r = rise_df.copy()

    b["txn_id"] = _clean_id(b[backend_id_col])
    r["_desc"] = r[rise_desc_col].astype(str).str.upper()

    b["ts_utc"] = _to_utc(b[backend_ts_col], backend_tz)
    r["ts_utc"] = _to_utc(r[rise_ts_col], rise_tz)

    b["ts_report_backend"] = b["ts_utc"].dt.tz_convert(report_tz)
    r["ts_report_wallet"] = r["ts_utc"].dt.tz_convert(report_tz)

    b["amount_backend"] = _safe_float(b[backend_amount_col])
    r["amount_wallet"] = _safe_float(r[rise_amount_col])

    b_win = b[(b["ts_report_backend"] >= report_start) & (b["ts_report_backend"] < report_end)].copy()

    def _pick(txn_id: str):
        m = r[r["_desc"].str.contains(txn_id, na=False)]
        if len(m) == 0:
            return (pd.NaT, np.nan)
        row = m.iloc[0]
        return (row["ts_report_wallet"], row["amount_wallet"])

    picked = b_win["txn_id"].apply(_pick).tolist()

    ts_list = [x[0] for x in picked]
    amt_list = [x[1] for x in picked]

    # FIX: ensure we always get a Series with datetime64[ns, tz]
    dt = pd.to_datetime(ts_list, errors="coerce", utc=True)
    if isinstance(dt, pd.DatetimeIndex):
        ts_ser = pd.Series(dt).dt.tz_convert(report_tz)
    else:
        ts_ser = pd.Series(dt).dt.tz_convert(report_tz)

    b_win["ts_report_wallet"] = ts_ser.values
    b_win["amount_wallet"] = pd.Series(amt_list, index=b_win.index, dtype="float")

    merged = b_win
    merged["delay_min"] = (merged["ts_report_backend"] - merged["ts_report_wallet"]).dt.total_seconds() / 60
    merged["amount_diff"] = merged["amount_backend"] - merged["amount_wallet"]

    matched = merged[(merged["ts_report_wallet"].notna()) & (merged["delay_min"].abs() <= tolerance_minutes)].copy()
    late_sync = merged[(merged["ts_report_wallet"].notna()) & (merged["delay_min"].abs() > tolerance_minutes)].copy()
    missing_true = merged[merged["ts_report_wallet"].isna()].copy()

    for df in (matched, late_sync, missing_true):
        df["bucket_3h"] = bucket_3h(df["ts_report_backend"])

    summary_3h = _build_summary(matched, late_sync, missing_true, report_start, report_end, report_tz)
    return ReconResult(matched, late_sync, missing_true, summary_3h)
