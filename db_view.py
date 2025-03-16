import streamlit as st
import sqlite3
import pandas as pd
import datetime

# Set wide layout for this page
st.set_page_config(layout="wide", page_title="View & Manage Database Data")

st.header("View & Manage Scraped Data")

# --- Utility Functions ---
def load_data_by_timestamps(selected_timestamps):
    """Load products with run_timestamp in the selected list."""
    if not selected_timestamps:
        return pd.DataFrame()
    conn = sqlite3.connect("scrape_data.db")
    # Build a placeholder string for parameterized query
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
    conn = sqlite3.connect("scrape_data.db")
    c = conn.cursor()
    placeholders = ','.join('?' * len(selected_timestamps))
    delete_query = f"DELETE FROM product WHERE run_timestamp IN ({placeholders})"
    c.execute(delete_query, selected_timestamps)
    conn.commit()
    count = c.rowcount
    conn.close()
    return count

def get_all_run_timestamps():
    conn = sqlite3.connect("scrape_data.db")
    c = conn.cursor()
    c.execute("SELECT DISTINCT run_timestamp FROM product ORDER BY run_timestamp")
    rows = c.fetchall()
    conn.close()
    return [row[0] for row in rows]

# --- Main UI ---
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

    st.markdown("---")
    st.subheader("Delete Data for Selected Timestamps")
    st.warning("**Warning:** This action is irreversible. The data corresponding to the selected timestamps will be permanently deleted.")
    if st.button("Delete Selected Data"):
        if not selected_ts:
            st.error("Please select at least one timestamp to delete.")
        else:
            count = delete_data_by_timestamps(selected_ts)
            st.success(f"Deleted {count} rows from the database.")
            # Refresh the available timestamps and display updated data
            raw_timestamps = get_all_run_timestamps()
            if raw_timestamps:
                ts_options = [(ts, datetime.datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")) for ts in raw_timestamps]
                format_map = dict(ts_options)
                st.write("### Updated Available Timestamps")
                st.write([format_map[ts] for ts in raw_timestamps])
            else:
                st.info("No data remaining in the database.")
