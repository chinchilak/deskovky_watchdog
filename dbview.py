import streamlit as st
import sqlite3
import pandas as pd
import datetime
from common import DB_PATH, get_all_run_timestamps, compare_runs


st.set_page_config(layout="wide")

st.header("View & Manage Scraped Data")

def load_data_by_timestamps(selected_timestamps):
    """Load products with run_timestamp in the selected list."""
    if not selected_timestamps:
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)

    placeholders = ','.join('?' * len(selected_timestamps))
    query = f"""
        SELECT * FROM product 
        WHERE run_timestamp IN ({placeholders})
        ORDER BY run_timestamp DESC
    """
    df = pd.read_sql_query(query, conn, params=selected_timestamps)
    conn.close()
    return df

def delete_data_by_timestamps(selected_timestamps):
    """Delete rows from product table where run_timestamp is in the selected list."""
    if not selected_timestamps:
        return 0
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    placeholders = ','.join('?' * len(selected_timestamps))
    delete_query = f"DELETE FROM product WHERE run_timestamp IN ({placeholders})"
    c.execute(delete_query, selected_timestamps)
    conn.commit()
    count = c.rowcount
    conn.close()
    return count

def get_all_run_timestamps():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT run_timestamp FROM product ORDER BY run_timestamp")
    rows = c.fetchall()
    conn.close()
    return [row[0] for row in rows]


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
                    st.write(f"- **{name}** – Price: {details['price']}, Availability: {details['availability']}, Link: {details['link']}")
            else:
                st.write("None")
            
            st.markdown("### Removed Products (present only in the first run)")
            if removed_products:
                for name, details in removed_products.items():
                    st.write(f"- **{name}** – Price: {details['price']}, Availability: {details['availability']}, Link: {details['link']}")
            else:
                st.write("None")
            
            st.markdown("### Updated Products (changed availability or price)")
            if updated_products:
                for name, details in updated_products.items():
                    st.write(f"- **{name}** – Price: {details['price']}, Availability: {details['availability']}, Link: {details['link']}")
            else:
                st.write("None")

st.subheader("Filter Data by Scrape Run Timestamps")

# Get available timestamps and create a mapping for human‑readable formatting.
raw_timestamps = get_all_run_timestamps()
if not raw_timestamps:
    st.warning("No scrape runs available in the database.")
else:
    # Prepare a list of tuples: (raw, formatted)
    ts_options = [(ts, datetime.datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")) for ts in raw_timestamps]
    format_map = dict(ts_options)
    
    # Multiselect widget to pick one or more timestamps.
    selected_ts = st.multiselect(
        "Select scrape run timestamps to view and/or delete:",
        options=[x[0] for x in ts_options],
        format_func=lambda x: format_map[x]
    )
    
    if selected_ts:
        df = load_data_by_timestamps(selected_ts)
        st.write("### Data for Selected Timestamps")
        st.dataframe(df)
    else:
        st.info("Select one or more timestamps to view data below.")

    st.subheader("Delete Data for Selected Timestamps")
    st.warning("**Warning:** This action is irreversible. The data corresponding to the selected timestamps will be permanently deleted.")
    if st.button("Delete Selected Data"):
        if not selected_ts:
            st.error("Please select at least one timestamp to delete.")
        else:
            count = delete_data_by_timestamps(selected_ts)
            st.success(f"Deleted {count} rows from the database.")
            raw_timestamps = get_all_run_timestamps()
            if raw_timestamps:
                ts_options = [(ts, datetime.datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")) for ts in raw_timestamps]
                format_map = dict(ts_options)
                st.write("### Updated Available Timestamps")
                st.write([format_map[ts] for ts in raw_timestamps])
            else:
                st.info("No data remaining in the database.")
        st.rerun()