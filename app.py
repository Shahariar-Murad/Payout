
import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("Payout Reconciliation Platform")

st.markdown("Stable build – filters apply only when selected.")

# ---------- Helpers ----------
def format_range(ts):
    h = ts.hour
    if h == 0: return "12:00 AM - 03:00 AM"
    if h == 3: return "03:00 AM - 06:00 AM"
    if h == 6: return "06:00 AM - 09:00 AM"
    if h == 9: return "09:00 AM - 12:00 PM"
    if h == 12: return "12:00 PM - 03:00 PM"
    if h == 15: return "03:00 PM - 06:00 PM"
    if h == 18: return "06:00 PM - 09:00 PM"
    return "09:00 PM - 12:00 AM"

# ---------- Upload ----------
backend = st.file_uploader("Backend CSV", type="csv")
crypto = st.file_uploader("Crypto CSV", type="csv")

if backend and crypto:
    b = pd.read_csv(backend)
    c = pd.read_csv(crypto)

    b["ts"] = pd.to_datetime(b["Created"], errors="coerce")
    b["bucket_3h"] = b["ts"].dt.floor("3H")
    b["Time Range"] = b["bucket_3h"].apply(format_range)
    b["Date"] = b["bucket_3h"].dt.date.astype(str)

    summary = (
        b.groupby(["Date","Time Range"])
        .agg(
            matched_count=("Created","count"),
            backend_total=("Amount","sum")
        )
        .reset_index()
    )

    st.subheader("3-hour summary")
    times = sorted(summary["Time Range"].unique().tolist())
    sel_times = st.multiselect("Filter time (optional)", options=times, default=[])

    view = summary.copy()
    if sel_times:
        view = view[view["Time Range"].isin(sel_times)]

    st.dataframe(view, use_container_width=True)
else:
    st.info("Upload backend and crypto files to start")
