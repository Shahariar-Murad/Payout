
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
    pat = r"(0x[a-fA-F0-9]{40})"
    return description.astype(str).str.extract(pat, expand=False).astype(str).str.upper()

@dataclass
class ChannelResult:
    name: str
    matched: pd.DataFrame
    late_sync: pd.DataFrame
    missing_true: pd.DataFrame
    summary_3h: pd.DataFrame

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

    b["txn_id"] = _clean_id(b[backend_id_col])
    if wallet_id_extractor:
        w["txn_id"] = _clean_id(wallet_id_extractor(w))
    else:
        w["txn_id"] = _clean_id(w[wallet_id_col])

    b["ts_utc"] = _to_tz_aware(b[backend_ts_col], backend_tz)
    w["ts_utc"] = _to_tz_aware(w[wallet_ts_col], wallet_tz)

    b["amount_backend"] = _safe_float(b[backend_amount_col])
    w["amount_wallet"] = _safe_float(w[wallet_amount_col])

    b["ts_report_backend"] = b["ts_utc"].dt.tz_convert(report_tz)
    w["ts_report_wallet"] = w["ts_utc"].dt.tz_convert(report_tz)

    b_win = b[(b["ts_report_backend"] >= report_start) & (b["ts_report_backend"] < report_end)].copy()

    merged = b_win.merge(
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

    miss_cnt = missing_true.groupby("bucket_3h").size().reset_index(name="missing_count")
    late_cnt = late_sync.groupby("bucket_3h").size().reset_index(name="late_sync_count")

    summary = summary.merge(miss_cnt, on="bucket_3h", how="left").merge(late_cnt, on="bucket_3h", how="left")
    summary[["missing_count","late_sync_count"]] = summary[["missing_count","late_sync_count"]].fillna(0).astype(int)

    return ChannelResult(channel_name, matched, late_sync, missing_true, summary)
