
from dataclasses import dataclass
import pandas as pd
import numpy as np

TOLERANCE_MINUTES = 15

def to_utc(series, tz):
    s = pd.to_datetime(series, errors="coerce")
    if s.dt.tz is not None:
        return s.dt.tz_convert("UTC")
    return s.dt.tz_localize(tz).dt.tz_convert("UTC")

def bucket_3h(ts):
    return ts.dt.floor("3H")

def plan_category(plan):
    s = plan.astype(str).str.lower()
    return np.where(s.str.contains("futures", na=False), "Futures", "CFD")

def is_automation(status):
    return status.astype(str).str.lower().str.contains("automation", na=False)

@dataclass
class ReconResult:
    matched: pd.DataFrame
    late_sync: pd.DataFrame
    missing: pd.DataFrame
    summary_3h: pd.DataFrame

def reconcile(
    backend,
    wallet,
    backend_id_col,
    wallet_desc_col,
    backend_ts_col,
    wallet_ts_col,
    backend_tz,
    wallet_tz,
    report_tz,
    start,
    end,
):
    b = backend.copy()
    w = wallet.copy()

    b["txn_id"] = b[backend_id_col].astype(str).str.strip().str.upper()
    w["desc"] = w[wallet_desc_col].astype(str).str.upper()

    b["ts"] = to_utc(b[backend_ts_col], backend_tz).dt.tz_convert(report_tz)
    w["ts"] = to_utc(w[wallet_ts_col], wallet_tz).dt.tz_convert(report_tz)

    b = b[(b["ts"] >= start) & (b["ts"] < end)].copy()

    def find_wallet(row):
        m = w[w["desc"].str.contains(row["txn_id"], na=False)]
        if len(m):
            return m.iloc[0]
        return pd.Series()

    wallet_match = b.apply(find_wallet, axis=1)
    merged = pd.concat([b.reset_index(drop=True), wallet_match.reset_index(drop=True)], axis=1)

    merged["delay_min"] = (merged["ts"] - merged["ts_y"]).dt.total_seconds() / 60
    merged["amount_diff"] = merged["Disbursement Amount"] - merged["Amount"]

    matched = merged[(merged["Amount"].notna()) & (merged["delay_min"].abs() <= TOLERANCE_MINUTES)]
    late = merged[(merged["Amount"].notna()) & (merged["delay_min"].abs() > TOLERANCE_MINUTES)]
    missing = merged[merged["Amount"].isna()]

    for df in (matched, late, missing):
        df["bucket"] = bucket_3h(df["ts"])

    summary = (
        matched.groupby("bucket")
        .agg(
            matched_count=("txn_id", "count"),
            backend_total=("Disbursement Amount", "sum"),
            wallet_total=("Amount", "sum"),
            abs_diff_total=("amount_diff", lambda s: np.nansum(np.abs(s))),
        )
        .reset_index()
    )

    miss = missing.groupby("bucket").size().reset_index(name="missing_count")
    summary = summary.merge(miss, on="bucket", how="left")
    summary["missing_count"] = summary["missing_count"].fillna(0).astype(int)

    return ReconResult(matched, late, missing, summary)
