import re
def _norm_col(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().lower())

def _resolve_col(df: "pd.DataFrame", requested: str, fallbacks=None) -> str:
    """Return actual column name in df matching requested (case/space-insensitive).
    If not found, try fallbacks. Raise KeyError if none found.
    """
    cols = list(df.columns)
    norm_map = {_norm_col(c): c for c in cols}
    cand = []
    if requested:
        cand.append(requested)
    if fallbacks:
        cand.extend(fallbacks)
    for c in cand:
        key = _norm_col(c)
        if key in norm_map:
            return norm_map[key]
    raise KeyError(f"Column not found. Tried: {cand}. Available: {cols}")


from dataclasses import dataclass
import pandas as pd
import numpy as np


def _norm_col(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().lower())

def _resolve_col(df: pd.DataFrame, requested: str, fallbacks=None) -> str:
    """Return actual column name in df matching requested (case/space-insensitive).
    If not found, try fallbacks list. Raise KeyError if none found.
    """
    cols = list(df.columns)
    norm_map = {_norm_col(c): c for c in cols}
    candidates = []
    if requested:
        candidates.append(requested)
    if fallbacks:
        candidates.extend(list(fallbacks))
    for c in candidates:
        key = _norm_col(c)
        if key in norm_map:
            return norm_map[key]
    raise KeyError(f"Column not found. Tried: {candidates}. Available: {cols}")

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

    backend_id_col = _resolve_col(b, backend_id_col, fallbacks=["Transaction ID","Tracking ID","TrackingID","tracking id","tracking_id","Txn ID","TXN ID","Reference","Reference ID"])
    b["txn_id"] = _clean_id(b[backend_id_col])
    wallet_id_col = _resolve_col(w, wallet_id_col, fallbacks=["Tracking ID","TrackingID","tracking id","tracking_id","Txn ID","TXN ID","Reference","Reference ID"])
    w["txn_id"] = _clean_id(w[wallet_id_col])

    backend_ts_col = _resolve_col(b, backend_ts_col, fallbacks=["Created","Created At","CreatedAt","created","created at"])
    b["ts_utc"] = _to_utc(b[backend_ts_col], backend_tz)
    w["ts_utc"] = _to_utc(w[wallet_ts_col], wallet_tz)

    b["ts_report_backend"] = b["ts_utc"].dt.tz_convert(report_tz)
    w["ts_report_wallet"] = w["ts_utc"].dt.tz_convert(report_tz)

    backend_amount_col = _resolve_col(b, backend_amount_col, fallbacks=["Disbursement Amount","Disbursement amount","Amount","amount"])
    b["amount_backend"] = _safe_float(b[backend_amount_col])
    w["amount_wallet"] = _safe_float(w[wallet_amount_col]).abs()

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
    """Rise matching:
    - Backend Payment method ID appears inside Rise Description
    - If multiple Rise rows match the same id, pick the closest timestamp to backend.
    """
    b = backend_df.copy()
    r = rise_df.copy()

    backend_id_col = _resolve_col(b, backend_id_col, fallbacks=["Transaction ID","Tracking ID","TrackingID","tracking id","tracking_id","Txn ID","TXN ID","Reference","Reference ID"])
    b["txn_id"] = _clean_id(b[backend_id_col])
    r["_desc"] = r[rise_desc_col].astype(str).str.upper()

    backend_ts_col = _resolve_col(b, backend_ts_col, fallbacks=["Created","Created At","CreatedAt","created","created at"])
    b["ts_utc"] = _to_utc(b[backend_ts_col], backend_tz)
    rise_ts_col = _resolve_col(r, rise_ts_col, fallbacks=["Date","Timestamp","Created","created","date"])
    r["ts_utc"] = _to_utc(r[rise_ts_col], rise_tz)

    b["ts_report_backend"] = b["ts_utc"].dt.tz_convert(report_tz)
    r["ts_report_wallet"] = r["ts_utc"].dt.tz_convert(report_tz)

    backend_amount_col = _resolve_col(b, backend_amount_col, fallbacks=["Disbursement Amount","Disbursement amount","Amount","amount"])
    b["amount_backend"] = _safe_float(b[backend_amount_col])
    rise_amount_col = _resolve_col(r, rise_amount_col, fallbacks=["Amount","amount","Net Amount","net amount"])
    r["amount_wallet_raw"] = _safe_float(r[rise_amount_col]).abs()

    b_win = b[(b["ts_report_backend"] >= report_start) & (b["ts_report_backend"] < report_end)].copy()
    r_win = r[(r["ts_report_wallet"] >= report_start - pd.Timedelta(hours=6)) & (r["ts_report_wallet"] < report_end + pd.Timedelta(hours=6))].copy()

    def _pick_best(txn_id: str, backend_ts: pd.Timestamp):
        m = r_win[r_win["_desc"].str.contains(txn_id, na=False)]
        if len(m) == 0 or pd.isna(backend_ts):
            return (pd.NaT, float("nan"))
        deltas = (m["ts_report_wallet"] - backend_ts).abs()
        idx = deltas.idxmin()
        row = m.loc[idx]
        return (row["ts_report_wallet"], row["amount_wallet_raw"])

    picked = [
        _pick_best(tid, ts)
        for tid, ts in zip(b_win["txn_id"].tolist(), b_win["ts_report_backend"].tolist())
    ]
    ts_list = [x[0] for x in picked]
    amt_list = [x[1] for x in picked]

    dt = pd.to_datetime(ts_list, errors="coerce", utc=True)
    b_win["ts_report_wallet"] = pd.Series(dt, index=b_win.index).dt.tz_convert(report_tz)
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

import re  # ensure available for email regex

def _extract_email(text: pd.Series) -> pd.Series:
    s = text.astype(str)
    return s.str.extract(r'([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})', flags=re.IGNORECASE, expand=False).str.lower()

def reconcile_rise_email(
    backend_df: pd.DataFrame,
    rise_df: pd.DataFrame,
    backend_ts_col: str,
    backend_tz: str,
    backend_email_col: str,
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
    """Rise matching by email:
    - Backend payment method email matches Rise email extracted from Rise Description.
    - If multiple Rise rows match the same email, pick the closest timestamp to backend.
    """
    b = backend_df.copy()
    r = rise_df.copy()

    b["match_key"] = b[backend_email_col].astype(str).str.strip().str.lower()
    r["match_key"] = _extract_email(r[rise_desc_col])

    backend_ts_col = _resolve_col(b, backend_ts_col, fallbacks=["Created","Created At","CreatedAt","created","created at"])
    b["ts_utc"] = _to_utc(b[backend_ts_col], backend_tz)
    rise_ts_col = _resolve_col(r, rise_ts_col, fallbacks=["Date","Timestamp","Created","created","date"])
    r["ts_utc"] = _to_utc(r[rise_ts_col], rise_tz)

    b["ts_report_backend"] = b["ts_utc"].dt.tz_convert(report_tz)
    r["ts_report_wallet"] = r["ts_utc"].dt.tz_convert(report_tz)

    backend_amount_col = _resolve_col(b, backend_amount_col, fallbacks=["Disbursement Amount","Disbursement amount","Amount","amount"])
    b["amount_backend"] = _safe_float(b[backend_amount_col])
    rise_amount_col = _resolve_col(r, rise_amount_col, fallbacks=["Amount","amount","Net Amount","net amount"])
    r["amount_wallet_raw"] = _safe_float(r[rise_amount_col]).abs()

    b_win = b[(b["ts_report_backend"] >= report_start) & (b["ts_report_backend"] < report_end)].copy()
    r_win = r[(r["ts_report_wallet"] >= report_start - pd.Timedelta(hours=6)) & (r["ts_report_wallet"] < report_end + pd.Timedelta(hours=6))].copy()

    rise_groups = {k: g for k, g in r_win.dropna(subset=["match_key"]).groupby("match_key")}

    def _pick_best(key: str, backend_ts: pd.Timestamp):
        g = rise_groups.get(key)
        if g is None or len(g) == 0 or pd.isna(backend_ts):
            return (pd.NaT, float("nan"))
        deltas = (g["ts_report_wallet"] - backend_ts).abs()
        idx = deltas.idxmin()
        row = g.loc[idx]
        return (row["ts_report_wallet"], row["amount_wallet_raw"])

    picked = [
        _pick_best(k, ts)
        for k, ts in zip(b_win["match_key"].tolist(), b_win["ts_report_backend"].tolist())
    ]
    ts_list = [x[0] for x in picked]
    amt_list = [x[1] for x in picked]

    dt = pd.to_datetime(ts_list, errors="coerce", utc=True)
    b_win["ts_report_wallet"] = pd.Series(dt, index=b_win.index).dt.tz_convert(report_tz)
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
