
import pandas as pd
import streamlit as st
from datetime import datetime, date, time, timedelta

from recon import reconcile, plan_category, is_automation

st.set_page_config(page_title="Payout Recon Platform v3", layout="wide")
st.title("Payout Reconciliation Platform (v3)")

with st.sidebar:
    backend_file = st.file_uploader("Backend (Payout Wallet CSV)", type=["csv"])
    crypto_file = st.file_uploader("Crypto Wallet CSV", type=["csv"])
    rise_file = st.file_uploader("Rise CSV", type=["csv"])

    date_range = st.date_input(
        "Report date range (GMT+6)",
        value=(date.today() - timedelta(days=1), date.today()),
    )

if not backend_file or not crypto_file or not rise_file:
    st.stop()

start_date, end_date = date_range
report_tz = "Asia/Dhaka"
start = pd.Timestamp(datetime.combine(start_date, time(0, 0)), tz=report_tz)
end = pd.Timestamp(datetime.combine(end_date + timedelta(days=1), time(0, 0)), tz=report_tz)

backend = pd.read_csv(backend_file)
crypto = pd.read_csv(crypto_file)
rise = pd.read_csv(rise_file)

backend["_ptype"] = plan_category(backend["Plan"])
backend["_auto"] = is_automation(backend["Internal Status"])

pm = backend["Payment Method"].str.lower()
backend_crypto = backend[pm.isin(["usdt", "usdc"])]
backend_rise = backend[pm.eq("risework")]

crypto_res = reconcile(
    backend_crypto,
    crypto,
    "Transaction ID",
    "Tracking ID",
    "Disbursed Time",
    "Created",
    "Etc/GMT-2",
    "UTC",
    report_tz,
    start,
    end,
)

rise_res = reconcile(
    backend_rise,
    rise,
    "Payment method ID",
    "Description",
    "Disbursed Time",
    "Date",
    "Etc/GMT-2",
    "Asia/Dhaka",
    report_tz,
    start,
    end,
)

tab1, tab2 = st.tabs(["Payout reconciliation", "Reconciled breakdown"])

with tab1:
    st.metric("Crypto matched", len(crypto_res.matched))
    st.metric("Rise matched", len(rise_res.matched))

    st.subheader("Rise summary")
    st.dataframe(rise_res.summary_3h, use_container_width=True)

    st.subheader("Crypto summary")
    st.dataframe(crypto_res.summary_3h, use_container_width=True)

with tab2:
    all_rec = pd.concat([crypto_res.matched, crypto_res.late_sync, rise_res.matched, rise_res.late_sync])
    all_rec["_ptype"] = plan_category(all_rec["Plan"])
    all_rec["_auto"] = is_automation(all_rec["Internal Status"])
    all_rec["Channel"] = all_rec["Payment Method"].str.lower().map(
        lambda x: "Crypto" if x in ["usdt","usdc"] else "Rise"
    )

    summary = (
        all_rec.groupby(["_ptype","Channel"])
        .agg(
            Count=("txn_id","count"),
            Total=("Disbursement Amount","sum"),
            Automation_Count=("_auto","sum"),
            Automation_Total=("Disbursement Amount", lambda s: s[all_rec.loc[s.index,"_auto"]].sum()),
        )
        .reset_index()
        .rename(columns={"_ptype":"Payout Type"})
    )

    st.dataframe(summary, use_container_width=True)
