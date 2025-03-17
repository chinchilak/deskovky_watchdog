import streamlit as st
import sqlite3
import json
import pandas as pd

from common import DB_PATH


st.set_page_config(layout="wide")


def fetch_comparison_log():
    """Retrieve all comparison logs from the database as a DataFrame with readable timestamps."""
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("""
            SELECT ts1, ts2, new_count, removed_count, updated_count, new_items, removed_items, updated_items, log_time
            FROM comparison_log
            ORDER BY log_time DESC
        """, conn)

    if df.empty:
        return df

    # Convert timestamps to human-readable format
    df["log_time"] = pd.to_datetime(df["log_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["ts1"] = pd.to_datetime(df["ts1"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["ts2"] = pd.to_datetime(df["ts2"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Decode JSON fields into dictionary objects
    df["new_items"] = df["new_items"].apply(lambda x: json.loads(x) if x else {})
    df["removed_items"] = df["removed_items"].apply(lambda x: json.loads(x) if x else {})
    df["updated_items"] = df["updated_items"].apply(lambda x: json.loads(x) if x else {})

    # Set log_time as index for better readability
    df.set_index("log_time", inplace=True)

    return df

df_logs = fetch_comparison_log()
st.dataframe(df_logs)
