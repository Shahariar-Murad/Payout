
import pandas as pd
import streamlit as st
from datetime import datetime, date, time, timedelta
import plotly.express as px

from recon import reconcile_exact, reconcile_rise_substring, plan_category, is_automation

st.set_page_config(page_title="Payout Recon Platform", layout="wide")
st.title("Payout Reconciliation Platform")

st.markdown("""
<style>
  .share-card {background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08);
               padding: 14px 16px; border-radius: 14px; margin: 10px 0 16px 0;}
  .share-title {font-size: 22px; font-weight: 700; margin-bottom: 6px;}
  .share-sub {color: rgba(255,255,255,0.70); font-size: 13px; margin-bottom: 10px;}
</style>
""", unsafe_allow_html=True)


def format_range(ts):
    return f"{ts.strftime('%I:%M %p')} - {(ts + pd.Timedelta(hours=3)).strftime('%I:%M %p')}"

with st.sidebar:
    st.header("Upload 3 files")
    backend_file = st.file_uploader("Backend (Payout Wallet CSV)", type=["csv"])
    crypto_file = st.file_uploader("Crypto wallet report CSV", type=["csv"])
    rise_file = st.file_uploader("Rise report CSV", type=["csv"])

    st.header("Report date range (GMT+6)")
    dr = st.date_input("Select start and end date", value=(date.today() - timedelta(days=1), date.today()))
    report_tz = st.text_input("Report timezone", value="Asia/Dhaka")

    st.header("Source timezones (naive)")
    backend_tz = st.text_input("Backend timezone", value="Etc/GMT-2")  # UTC+2
    crypto_tz = st.text_input("Crypto report timezone", value="UTC")   # data is GMT+00
    rise_tz = st.text_input("Rise report timezone", value="Asia/Dhaka") # GMT+6

    tol = st.number_input("Max wallet→backend delay (minutes)", min_value=0, max_value=120, value=15)

if not backend_file:
    st.info("Upload at least the Backend file to run reconciliation.")
    st.stop()

run_crypto = crypto_file is not None
run_rise = rise_file is not None
if (not run_crypto) and (not run_rise):
    st.info("Upload either Crypto wallet report or Rise report (or both).")
    st.stop()

start_date, end_date = dr
report_start = pd.Timestamp(datetime.combine(start_date, time(0,0)), tz=report_tz)
report_end = pd.Timestamp(datetime.combine(end_date + timedelta(days=1), time(0,0)), tz=report_tz)

backend = pd.read_csv(backend_file)
crypto = pd.read_csv(crypto_file) if run_crypto else None
rise = pd.read_csv(rise_file) if run_rise else None

backend["_ptype"] = plan_category(backend.get("Plan", pd.Series([""]*len(backend))))
backend["_auto"] = is_automation(backend.get("Internal Status", pd.Series([""]*len(backend))))

pm = backend.get("Payment Method", pd.Series([""]*len(backend))).astype(str).str.lower()
backend_crypto = backend[pm.isin(["usdt","usdc"])].copy() if run_crypto else None
backend_rise = backend[pm.isin(["riseworks","risework","rise"])].copy() if run_rise else None

crypto_res = None
if run_crypto:
    crypto_res = reconcile_exact(
        backend_df=backend_crypto,
        wallet_df=crypto,
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
    )

rise_res = None
if run_rise:
    rise_res = reconcile_rise_substring(
    backend_df=backend_rise,
    rise_df=rise,
    backend_ts_col="Disbursed Time",
    backend_tz=backend_tz,
    backend_id_col="Payment method ID",
    backend_amount_col="Disbursement Amount",
    rise_ts_col="Date",
    rise_tz=rise_tz,
    rise_desc_col="Description",
    rise_amount_col="Amount",
    report_tz=report_tz,
    report_start=report_start,
    report_end=report_end,
    tolerance_minutes=int(tol),
)

tab1, tab2 = st.tabs(["Payout reconciliation", "Breakdown"])

with tab1:
    st.subheader("Overview")
    a,b,c = st.columns(3)
    crypto_matched = len(crypto_res.matched) if crypto_res is not None else 0
    rise_matched = len(rise_res.matched) if rise_res is not None else 0
    true_missing = (len(crypto_res.missing_true) if crypto_res is not None else 0) + (len(rise_res.missing_true) if rise_res is not None else 0)
    a.metric("Crypto matched", crypto_matched)
    b.metric("Rise matched", rise_matched)
    c.metric("True missing (all)", true_missing)

    st.subheader("Missing transaction details")
    st.caption("These are Backend payouts that were not found in the selected wallet report (after applying the 15-minute tolerance).")
    m1, m2 = st.columns(2)
    with m1:
        st.markdown("**Crypto missing (Backend present, Wallet missing)**")
        cm = crypto_res.missing_true.copy() if crypto_res is not None else pd.DataFrame()
        if cm.empty:
            st.write("No missing rows ✅")
        else:
            show_cols = [c for c in ["Disbursed Time","Transaction ID","Disbursement Amount","Payment Method","Plan","Internal Status","Customer Email","Login","Id"] if c in cm.columns]
            st.dataframe(cm[show_cols + [c for c in ["txn_id","ts_report_backend","amount_backend"] if c in cm.columns]].head(200), use_container_width=True, height=220)
    with m2:
        st.markdown("**Rise missing (Backend present, Wallet missing)**")
        rm = rise_res.missing_true.copy() if rise_res is not None else pd.DataFrame()
        if rm.empty:
            st.write("No missing rows ✅")
        else:
            show_cols = [c for c in ["Disbursed Time","Payment method ID","Disbursement Amount","Payment Method","Plan","Internal Status","Customer Email","Login","Id"] if c in rm.columns]
            st.dataframe(rm[show_cols + [c for c in ["txn_id","ts_report_backend","amount_backend"] if c in rm.columns]].head(200), use_container_width=True, height=220)


    st.subheader("3-hour payout counts (backend) — Rise vs Crypto")

    def counts(df):
        if df is None or df.empty:
            return pd.DataFrame(columns=["bucket_3h","count"])
        ts = pd.to_datetime(df["Disbursed Time"], errors="coerce")
        ts = ts.dt.tz_localize(backend_tz).dt.tz_convert(report_tz)
        win = df.copy()
        win["_ts"] = ts
        win = win[(win["_ts"]>=report_start)&(win["_ts"]<report_end)]
        win["bucket_3h"] = win["_ts"].dt.floor("3H")
        return win.groupby("bucket_3h").size().reset_index(name="count")

    cc = counts(backend_crypto).rename(columns={"count":"crypto_count"})
    rc = counts(backend_rise).rename(columns={"count":"rise_count"})
    buckets = pd.DataFrame({"bucket_3h": pd.date_range(start=report_start, end=report_end, freq="3H", inclusive="left").tz_convert(report_tz)})
    counts_3h = buckets.merge(cc,on="bucket_3h",how="left").merge(rc,on="bucket_3h",how="left").fillna(0)
    counts_3h["Time Range"] = counts_3h["bucket_3h"].apply(format_range)

    value_cols = []
    if run_rise: value_cols.append("rise_count")
    if run_crypto: value_cols.append("crypto_count")

    if len(value_cols) == 0:
        st.write("Upload Crypto or Rise report to see chart.")
    else:
        chart_df = counts_3h.melt(id_vars=["bucket_3h","Time Range"], value_vars=value_cols, var_name="Channel", value_name="Count")
        chart_df["Channel"] = chart_df["Channel"].replace({"rise_count":"Rise","crypto_count":"Crypto"})
        chart_df["Date"] = chart_df["bucket_3h"].dt.strftime("%Y-%m-%d")

        dates = sorted(chart_df["Date"].unique().tolist())
        times = sorted(chart_df["Time Range"].unique().tolist())
        sel_dates = st.multiselect("Filter date (optional)", options=dates, default=dates)
        sel_times = st.multiselect("Filter 3-hour slot (optional)", options=times, default=times)

        fdf = chart_df[(chart_df["Date"].isin(sel_dates)) & (chart_df["Time Range"].isin(sel_times))].copy()
        fig = px.bar(fdf, x="Time Range", y="Count", color="Channel", barmode="group")
        st.plotly_chart(fig, use_container_width=True)

        st.session_state["__sel_dates"] = sel_dates
        st.session_state["__sel_times"] = sel_times
if run_rise:
    st.subheader("Rise 3-hour summary")

    rs = rise_res.summary_3h.copy()
    rs["Date"] = rs["bucket_3h"].dt.strftime("%Y-%m-%d")
    rs["Time Range"] = rs["bucket_3h"].apply(format_range)

    rs = rs[[
        "Date",
        "Time Range",
        "matched_count",
        "late_sync_count",
        "missing_count",
        "backend_total",
        "wallet_total",
        "diff_total",
        "abs_diff_total",
    ]]

    sel_dates = st.session_state.get("__sel_dates")
    sel_times = st.session_state.get("__sel_times")

    if sel_dates is not None and sel_times is not None:
        rs = rs[(rs["Date"].isin(sel_dates)) & (rs["Time Range"].isin(sel_times))]

    st.markdown(
        '<div class="share-card">'
        '<div class="share-title">Rise 3-hour summary</div>'
        '<div class="share-sub">Filtered view — ready for screenshot.</div>'
        '</div>',
        unsafe_allow_html=True
    )

    st.dataframe(rs, use_container_width=True, height=260)

    if run_crypto:
    st.subheader("Crypto 3-hour summary")

    cs = crypto_res.summary_3h.copy()
    cs["Date"] = cs["bucket_3h"].dt.strftime("%Y-%m-%d")
    cs["Time Range"] = cs["bucket_3h"].apply(format_range)

    cs = cs[[
        "Date",
        "Time Range",
        "matched_count",
        "late_sync_count",
        "missing_count",
        "backend_total",
        "wallet_total",
        "diff_total",
        "abs_diff_total",
    ]]

    sel_dates = st.session_state.get("__sel_dates")
    sel_times = st.session_state.get("__sel_times")

    if sel_dates is not None and sel_times is not None:
        cs = cs[(cs["Date"].isin(sel_dates)) & (cs["Time Range"].isin(sel_times))]

    st.markdown(
        '<div class="share-card">'
        '<div class="share-title">Crypto 3-hour summary</div>'
        '<div class="share-sub">Filtered view — ready for screenshot.</div>'
        '</div>',
        unsafe_allow_html=True
    )

    st.dataframe(cs, use_container_width=True, height=260)

with tab2:
    st.subheader("Breakdown (Matched + Late Sync only)")
    parts = []
    if crypto_res is not None:
        parts += [crypto_res.matched.assign(channel="Crypto"), crypto_res.late_sync.assign(channel="Crypto")]
    if rise_res is not None:
        parts += [rise_res.matched.assign(channel="Rise"), rise_res.late_sync.assign(channel="Rise")]
    rec = pd.concat(parts, ignore_index=True) if len(parts) else pd.DataFrame()

    if rec.empty:
        st.info("No reconciled rows in the selected range.")
        st.stop()

    rec["_ptype"] = plan_category(rec.get("Plan", pd.Series([""]*len(rec))))
    rec["_auto"] = is_automation(rec.get("Internal Status", pd.Series([""]*len(rec))))

    summary = (
        rec.groupby(["_ptype","channel"])
        .agg(
            Count=("txn_id","count"),
            Total_Sum=("amount_backend","sum"),
            Automation_Count=("_auto","sum"),
            Automation_Sum=("amount_backend", lambda s: float(s[rec.loc[s.index,"_auto"]].sum())),
        )
        .reset_index()
        .rename(columns={"_ptype":"Payout Type","channel":"Channel"})
    )

    st.dataframe(summary.sort_values(["Payout Type","Channel"]), use_container_width=True, height=260)

    # Totals by payout type (CFD vs Futures) - across Rise + Crypto (Matched + Late Sync)
    totals = (
        rec.groupby("_ptype")
        .agg(Count=("txn_id", "count"), Total_Sum=("amount_backend", "sum"))
        .reset_index()
        .rename(columns={"_ptype": "Payout Type"})
    )
    c1, c2, c3, c4 = st.columns(4)
    cfd_row = totals[totals["Payout Type"] == "CFD"]
    fut_row = totals[totals["Payout Type"] == "Futures"]

    c1.metric("CFD count", int(cfd_row["Count"].iloc[0]) if not cfd_row.empty else 0)
    c2.metric("CFD sum", f"{float(cfd_row['Total_Sum'].iloc[0]) if not cfd_row.empty else 0.0:,.2f}")
    c3.metric("Futures count", int(fut_row["Count"].iloc[0]) if not fut_row.empty else 0)
    c4.metric("Futures sum", f"{float(fut_row['Total_Sum'].iloc[0]) if not fut_row.empty else 0.0:,.2f}")

    st.markdown("")
    st.subheader("CFD vs Futures share (by amount)")
    pie_df = totals.copy()
    pie_df = pie_df[pie_df["Total_Sum"] > 0].copy()
    if pie_df.empty:
        st.write("No data to chart.")
    else:
        fig_pie = px.pie(pie_df, names="Payout Type", values="Total_Sum", hole=0.35)
        st.plotly_chart(fig_pie, use_container_width=True)

