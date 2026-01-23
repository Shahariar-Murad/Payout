
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
)

st.set_page_config(page_title="Payout Recon Platform", layout="wide")
st.title("Payout Recon Platform")

with st.sidebar:
    st.header("Upload files")
    backend_file = st.file_uploader("Backend (Payout Wallet CSV)", type=["csv"])
    crypto_file = st.file_uploader("Crypto wallet report (API payout CSV)", type=["csv"])
    rise_file = st.file_uploader("Rise report CSV", type=["csv"])

    st.header("Report date range (GMT+6)")
    date_range = st.date_input(
        "Select start and end date",
        value=(date.today() - timedelta(days=1), date.today()),
    )

    st.header("Source timezones")
    backend_tz = st.text_input("Backend timezone (naive)", value="Etc/GMT-2")  # UTC+2
    crypto_tz = st.text_input("Crypto report timezone (naive)", value="UTC")   # FIXED
    rise_tz = st.text_input("Rise report timezone (naive)", value="Asia/Dhaka")

    st.header("Tolerance")
    tol = st.number_input("Wallet → Backend delay (minutes)", min_value=0, max_value=120, value=15)

if not backend_file or not crypto_file or not rise_file:
    st.info("Upload all files to start.")
    st.stop()

start_date, end_date = date_range
report_tz = "Asia/Dhaka"

report_start = pd.Timestamp(datetime.combine(start_date, time(0, 0)), tz=report_tz)
report_end = pd.Timestamp(datetime.combine(end_date + timedelta(days=1), time(0, 0)), tz=report_tz)

backend_df = pd.read_csv(backend_file)
crypto_df = pd.read_csv(crypto_file)
rise_df = pd.read_csv(rise_file)

backend_df["_payout_type"] = plan_category(backend_df["Plan"])
backend_df["_is_automation"] = is_automation(backend_df["Internal Status"])

pm = backend_df["Payment Method"].astype(str).str.lower()
backend_crypto = backend_df[pm.isin(["usdt", "usdc"])].copy()
backend_rise = backend_df[pm.eq("risework")].copy()

crypto_res = reconcile_with_tolerance(
    backend_crypto, crypto_df,
    "Disbursed Time", backend_tz, "Transaction ID", "Disbursement Amount",
    "Created", crypto_tz, "Tracking ID", "Amount",
    report_tz, report_start, report_end, int(tol),
)

rise_res = reconcile_with_tolerance(
    backend_rise, rise_df,
    "Disbursed Time", backend_tz, "Payment method ID", "Disbursement Amount",
    "Date", rise_tz, "Description", "Amount",
    report_tz, report_start, report_end, int(tol),
    wallet_id_extractor=lambda df: extract_rise_id(df["Description"]),
)

st.subheader("Summary")
c1, c2, c3 = st.columns(3)
c1.metric("Crypto matched", len(crypto_res.matched))
c2.metric("Rise matched", len(rise_res.matched))
c3.metric("True missing (all)", len(crypto_res.missing_true) + len(rise_res.missing_true))

st.subheader("Crypto 3-hour summary")
cs = crypto_res.summary_3h.copy()
cs["Date"] = cs["bucket_3h"].dt.strftime("%Y-%m-%d")
cs["Time Range"] = cs["bucket_3h"].apply(lambda x: f"{x.strftime('%I:%M %p')} - {(x+pd.Timedelta(hours=3)).strftime('%I:%M %p')}")
st.dataframe(cs, use_container_width=True)

st.subheader("Rise 3-hour summary")
rs = rise_res.summary_3h.copy()
rs["Date"] = rs["bucket_3h"].dt.strftime("%Y-%m-%d")
rs["Time Range"] = rs["bucket_3h"].apply(lambda x: f"{x.strftime('%I:%M %p')} - {(x+pd.Timedelta(hours=3)).strftime('%I:%M %p')}")
st.dataframe(rs, use_container_width=True)
