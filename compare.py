import streamlit as st
import datetime
from common import create_db, get_all_run_timestamps, compare_runs

create_db()  # Ensure database exists

st.header("Compare Scrape Runs")
raw_timestamps = get_all_run_timestamps()
if not raw_timestamps:
    st.warning("No scrape runs available for comparison.")
else:
    ts_options = [(ts, datetime.datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")) for ts in raw_timestamps]
    format_map = dict(ts_options)
    default_ts1 = ts_options[-2][0] if len(ts_options) >= 2 else ts_options[0][0]
    default_ts2 = ts_options[-1][0] if len(ts_options) >= 2 else ts_options[0][0]
    
    ts1 = st.selectbox("Select First Run Timestamp", options=[x[0] for x in ts_options],
                         format_func=lambda x: format_map[x],
                         index=[x[0] for x in ts_options].index(default_ts1))
    ts2 = st.selectbox("Select Second Run Timestamp", options=[x[0] for x in ts_options],
                         format_func=lambda x: format_map[x],
                         index=[x[0] for x in ts_options].index(default_ts2))
    
    if st.button("Compare Runs"):
        if ts1 == ts2:
            st.error("Please select two different timestamps for comparison.")
        else:
            new_products, removed_products, updated_products = compare_runs(ts1, ts2)
            st.subheader("Comparison Results")
            st.markdown("### New Products (present only in the second run)")
            if new_products:
                for name, details in new_products.items():
                    st.write(f"- **{name}** – Price: {details['price']}, Availability: {details['availability']}")
            else:
                st.write("None")
            
            st.markdown("### Removed Products (present only in the first run)")
            if removed_products:
                for name, details in removed_products.items():
                    st.write(f"- **{name}** – Price: {details['price']}, Availability: {details['availability']}")
            else:
                st.write("None")
            
            st.markdown("### Updated Products (changed availability or price)")
            if updated_products:
                for name, changes in updated_products.items():
                    st.write(f"- **{name}**:")
                    for field, vals in changes.items():
                        st.write(f"    - {field}: {vals['old']} → {vals['new']}")
            else:
                st.write("None")
