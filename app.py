
import streamlit as st
import pandas as pd
from recon import reconcile_exact, reconcile_rise_substring, plan_category, is_automation

st.set_page_config(layout="wide")
st.title("Payout Reconciliation Platform")

# Sidebar date filter
sel_date = st.sidebar.date_input("Select date")

# ---- assume data already loaded & reconciled above ----

# --- CRYPTO SUMMARY ---
if True:
    st.subheader("Crypto 3-hour summary")

    cs = crypto_res.summary_3h.copy()
    cs["Date"] = cs["bucket_3h"].dt.strftime("%Y-%m-%d")
    cs["Time Range"] = cs["bucket_3h"].apply(format_range)

    # Time filter (no default selection)
    times = sorted(cs["Time Range"].unique().tolist())
    sel_times = st.multiselect("Filter time slot (optional)", options=times)

    if sel_times:
        cs = cs[cs["Time Range"].isin(sel_times)]

    st.dataframe(cs, use_container_width=True)

