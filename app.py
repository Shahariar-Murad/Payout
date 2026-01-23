
import io
from datetime import datetime, date, time, timedelta
import pandas as pd
import streamlit as st
import plotly.express as px

from recon import (
    reconcile_with_tolerance,
    extract_rise_id,
    plan_category,
    is_automation,
    export_excel,
)

st.set_page_config(page_title="Payout Recon Platform", layout="wide")
st.title("Payout Recon Platform")

# ---- Sidebar ----
with st.sidebar:
    st.header("Upload 3 files")
    backend_file = st.file_uploader("Backend (Payout Wallet CSV)", type=["csv"], key="backend")
    crypto_file = st.file_uploader("Crypto wallet report (API payout report CSV)", type=["csv"], key="crypto")
    rise_file = st.file_uploader("Rise report CSV", type=["csv"], key="rise")

    st.header("Report day (GMT+6)")
    report_tz = st.text_input("Report timezone", value="Asia/Dhaka")
    report_day = st.date_input("Select day", value=date.today())

    st.header("Source timezones")
    backend_tz = st.text_input("Backend timezone (naive)", value="Etc/GMT-2")   # UTC+2
    crypto_tz = st.text_input("Crypto report timezone (naive)", value="Etc/GMT-2")  # API payout report GMT+2
    rise_tz = st.text_input("Rise report timezone (naive)", value="Asia/Dhaka")  # GMT+6

    st.header("Tolerance")
    tol = st.number_input("Wallet→Backend max delay (minutes)", min_value=0, max_value=120, value=15, step=1)

def format_range(ts: pd.Timestamp) -> str:
    start = ts
    end = ts + pd.Timedelta(hours=3)
    return f"{start.strftime('%I:%M %p')} - {end.strftime('%I:%M %p')}"

def ensure_files():
    if backend_file is None or crypto_file is None or rise_file is None:
        st.info("Upload all 3 CSV files to run reconciliation.")
        st.stop()

ensure_files()

# Window for the selected day in report tz
start_dt = datetime.combine(report_day, time(0, 0))
end_dt = start_dt + timedelta(days=1)
report_start = pd.Timestamp(start_dt, tz=report_tz)
report_end = pd.Timestamp(end_dt, tz=report_tz)

backend_df = pd.read_csv(backend_file)
crypto_df = pd.read_csv(crypto_file)
rise_df = pd.read_csv(rise_file)

# Add classifications to backend (for tab 2)
backend_df["_payout_type"] = plan_category(backend_df.get("Plan"))
backend_df["_is_automation"] = is_automation(backend_df.get("Internal Status"))

# Split backend by payment method
pm = backend_df.get("Payment Method", pd.Series([""]*len(backend_df))).astype(str).str.lower()
backend_crypto = backend_df[pm.isin(["usdt", "usdc"])].copy()
backend_rise = backend_df[pm.eq("risework")].copy()

# --- Reconcile Crypto ---
crypto_res = reconcile_with_tolerance(
    backend_df=backend_crypto,
    wallet_df=crypto_df,
    backend_ts_col="Disbursed Time",
    backend_tz=backend_tz,
    backend_id_col="Transaction ID",
    backend_amount_col="Disbursement Amount",
    wallet_ts_col="Created",
    wallet_tz=crypto_tz,
    wallet_id_col="Tracking ID",
    wallet_amount_col="Amount",
    report_tz=report_tz,
    report_start=report_start,
    report_end=report_end,
    tolerance_minutes=int(tol),
    channel_name="Crypto",
)

# --- Reconcile Rise ---
rise_res = reconcile_with_tolerance(
    backend_df=backend_rise,
    wallet_df=rise_df,
    backend_ts_col="Disbursed Time",
    backend_tz=backend_tz,
    backend_id_col="Payment method ID",
    backend_amount_col="Disbursement Amount",
    wallet_ts_col="Date",
    wallet_tz=rise_tz,
    wallet_id_col="Description",
    wallet_id_extractor=lambda df: extract_rise_id(df["Description"]),
    wallet_amount_col="Amount",
    report_tz=report_tz,
    report_start=report_start,
    report_end=report_end,
    tolerance_minutes=int(tol),
    channel_name="Rise",
)

# Counts by 3h from backend (all, by channel)
def counts_by_bucket(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["_ts"] = pd.to_datetime(d["Disbursed Time"], errors="coerce")
    # localize backend tz then to report tz
    d["_ts"] = d["_ts"].dt.tz_localize(backend_tz).dt.tz_convert(report_tz)
    d = d[(d["_ts"] >= report_start) & (d["_ts"] < report_end)]
    d["bucket_3h"] = d["_ts"].dt.floor("3H")
    return d.groupby("bucket_3h").size().reset_index(name="count")

c_crypto = counts_by_bucket(backend_crypto).rename(columns={"count":"crypto_count"})
c_rise = counts_by_bucket(backend_rise).rename(columns={"count":"rise_count"})
buckets = pd.DataFrame({"bucket_3h": pd.date_range(start=report_start, end=report_end, freq="3H", inclusive="left").tz_convert(report_tz)})
counts_3h = buckets.merge(c_crypto, on="bucket_3h", how="left").merge(c_rise, on="bucket_3h", how="left")
counts_3h["crypto_count"] = counts_3h["crypto_count"].fillna(0).astype(int)
counts_3h["rise_count"] = counts_3h["rise_count"].fillna(0).astype(int)
counts_3h["Date"] = counts_3h["bucket_3h"].dt.strftime("%Y-%m-%d")
counts_3h["Time Range"] = counts_3h["bucket_3h"].apply(format_range)

# Segment summary (Tab 2) using reconciled only (matched + late_sync)
def tag_backend_rows(res: pd.DataFrame, channel: str) -> pd.DataFrame:
    # res has backend columns + ts_report_backend, etc.
    out = res.copy()
    out["channel"] = channel
    # Map payout type & automation from original backend columns present in res
    out["_payout_type"] = plan_category(out.get("Plan"))
    out["_is_automation"] = is_automation(out.get("Internal Status"))
    return out

reconciled_crypto = pd.concat([tag_backend_rows(crypto_res.matched, "Crypto"), tag_backend_rows(crypto_res.late_sync, "Crypto")], ignore_index=True)
reconciled_rise = pd.concat([tag_backend_rows(rise_res.matched, "Rise"), tag_backend_rows(rise_res.late_sync, "Rise")], ignore_index=True)
reconciled_all = pd.concat([reconciled_crypto, reconciled_rise], ignore_index=True)

def segment_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Payout Type","Channel","Count","Total Amount","Automation Count","Automation Total"])
    g = df.groupby(["_payout_type","channel"], dropna=False)
    base = g.agg(
        Count=("txn_id","count"),
        Total_Amount=("amount_backend","sum"),
        Automation_Count=("_is_automation","sum"),
        Automation_Total=("amount_backend", lambda s: float(s[df.loc[s.index, "_is_automation"]].sum()) if len(s) else 0.0),
    ).reset_index()
    base = base.rename(columns={"_payout_type":"Payout Type","channel":"Channel"})
    return base

segment_summary = segment_table(reconciled_all)

# ---- Tabs ----
tab1, tab2 = st.tabs(["Payout reconciliation", "Reconciled breakdown"])

with tab1:
    st.subheader("Overview")
    colA, colB = st.columns(2)
    with colA:
        st.markdown("### Rise")
        r1, r2, r3 = st.columns(3)
        r1.metric("Matched (≤ tolerance)", len(rise_res.matched))
        r2.metric("Late sync (> tolerance)", len(rise_res.late_sync))
        r3.metric("Missing (true)", len(rise_res.missing_true))
    with colB:
        st.markdown("### Crypto")
        c1, c2, c3 = st.columns(3)
        c1.metric("Matched (≤ tolerance)", len(crypto_res.matched))
        c2.metric("Late sync (> tolerance)", len(crypto_res.late_sync))
        c3.metric("Missing (true)", len(crypto_res.missing_true))

    st.subheader("3-hour payout counts (backend) — Rise vs Crypto")
    chart_df = counts_3h.melt(id_vars=["bucket_3h","Date","Time Range"], value_vars=["rise_count","crypto_count"], var_name="Channel", value_name="Count")
    chart_df["Channel"] = chart_df["Channel"].replace({"rise_count":"Rise","crypto_count":"Crypto"})
    fig = px.bar(chart_df, x="Time Range", y="Count", color="Channel", barmode="group")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Rise 3-hour summary")
    rs = rise_res.summary_3h.copy()
    rs["Date"] = rs["bucket_3h"].dt.strftime("%Y-%m-%d")
    rs["Time Range"] = rs["bucket_3h"].apply(format_range)
    rs = rs[["Date","Time Range","matched_count","late_sync_count","missing_count","backend_total","wallet_total","diff_total","abs_diff_total"]]
    st.dataframe(rs, use_container_width=True, height=260)

    st.subheader("Crypto 3-hour summary")
    cs = crypto_res.summary_3h.copy()
    cs["Date"] = cs["bucket_3h"].dt.strftime("%Y-%m-%d")
    cs["Time Range"] = cs["bucket_3h"].apply(format_range)
    cs = cs[["Date","Time Range","matched_count","late_sync_count","missing_count","backend_total","wallet_total","diff_total","abs_diff_total"]]
    st.dataframe(cs, use_container_width=True, height=260)

    with st.expander("Details: Rise"):
        st.markdown("**Late Sync (Rise)**")
        st.dataframe(rise_res.late_sync, use_container_width=True, height=220)
        st.markdown("**Missing True (Rise)**")
        st.dataframe(rise_res.missing_true, use_container_width=True, height=220)

    with st.expander("Details: Crypto"):
        st.markdown("**Late Sync (Crypto)**")
        st.dataframe(crypto_res.late_sync, use_container_width=True, height=220)
        st.markdown("**Missing True (Crypto)**")
        st.dataframe(crypto_res.missing_true, use_container_width=True, height=220)

    # Excel download
    st.subheader("Download Excel report")
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name
    export_excel(tmp_path, report_tz, rise_res, crypto_res, segment_summary, counts_3h)
    with open(tmp_path, "rb") as f:
        data = f.read()
    os.remove(tmp_path)

    st.download_button(
        "Download detailed Excel",
        data=data,
        file_name=f"recon_{report_day.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

with tab2:
    st.subheader("Reconciled payout breakdown (Matched + Late Sync only)")
    st.caption("This excludes true-missing payouts.")

    st.dataframe(segment_summary.sort_values(["Payout Type","Channel"]), use_container_width=True, height=280)

    # Additional insight cards
    total = reconciled_all["amount_backend"].sum() if not reconciled_all.empty else 0.0
    futures_total = reconciled_all.loc[reconciled_all["_payout_type"]=="Futures","amount_backend"].sum() if not reconciled_all.empty else 0.0
    cfd_total = reconciled_all.loc[reconciled_all["_payout_type"]=="CFD","amount_backend"].sum() if not reconciled_all.empty else 0.0

    a1, a2, a3 = st.columns(3)
    a1.metric("Total reconciled amount", f"{total:.2f}")
    a2.metric("Futures reconciled amount", f"{futures_total:.2f}")
    a3.metric("CFD reconciled amount", f"{cfd_total:.2f}")

    # Automation metrics
    auto = reconciled_all[reconciled_all["_is_automation"]] if not reconciled_all.empty else reconciled_all
    auto_total = auto["amount_backend"].sum() if not auto.empty else 0.0
    st.metric("Total amount via payout automation", f"{auto_total:.2f}")

    st.subheader("Reconciled rows (sample)")
    st.dataframe(reconciled_all.head(300), use_container_width=True, height=320)
