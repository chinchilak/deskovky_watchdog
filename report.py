import streamlit as st
import sqlite3
import json
import pandas as pd

from common import DB_PATH

st.set_page_config(layout="wide")


def fetch_comparison_log():
    """Retrieve and expand comparison logs into a structured DataFrame."""
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

    # Decode JSON fields into dictionary objects
    df["new_items"] = df["new_items"].apply(lambda x: json.loads(x) if x else {})
    df["removed_items"] = df["removed_items"].apply(lambda x: json.loads(x) if x else {})
    df["updated_items"] = df["updated_items"].apply(lambda x: json.loads(x) if x else {})

    # **Filter out rows where all three fields are empty dictionaries**
    df = df[~(df["new_items"].apply(lambda x: x == {}) &
              df["removed_items"].apply(lambda x: x == {}) &
              df["updated_items"].apply(lambda x: x == {}))]

    # Function to transform dictionary into DataFrame format
    def expand_items(log_time, item_dict, change_type):
        rows = []
        for name, details in item_dict.items():
            row = {
                "log_time": log_time,
                "change_type": change_type,
                "item_name": name,
                "availability": details.get("availability", ""),  # Handle missing keys safely
                "link": details.get("link", ""),
                "price": details.get("price", "")
            }
            rows.append(row)
        return rows

    # Flatten dictionaries
    expanded_data = []
    for _, row in df.iterrows():
        expanded_data.extend(expand_items(row["log_time"], row["new_items"], "new"))
        expanded_data.extend(expand_items(row["log_time"], row["removed_items"], "removed"))
        expanded_data.extend(expand_items(row["log_time"], row["updated_items"], "updated"))

    # Convert to DataFrame
    expanded_df = pd.DataFrame(expanded_data)

    # Ensure all expected columns exist
    expected_columns = ["log_time", "change_type", "item_name", "availability", "link", "price"]
    for col in expected_columns:
        if col not in expanded_df.columns:
            expanded_df[col] = ""  # Add missing columns with empty values

    # Reorder columns
    expanded_df = expanded_df[expected_columns]

    return expanded_df


df_logs = fetch_comparison_log()
st.dataframe(df_logs)
