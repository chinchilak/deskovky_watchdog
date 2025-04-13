import streamlit as st
import sqlite3
import json
import pandas as pd

from common import DB_PATH

st.set_page_config(layout="wide")


def st_num_of_rows(pdframe:pd.DataFrame, limit:bool=False) -> int:
    nrows = len(pdframe)
    res = (nrows + 1) * 35 + 3
    if limit and res > 1000:
        res = 1000
    return res
    

def fetch_comparison_log():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("""
            SELECT ts1, ts2, new_count, removed_count, updated_count, new_items, removed_items, updated_items, log_time
            FROM comparison_log
            ORDER BY log_time DESC
        """, conn)

    if df.empty:
        return df

    df["log_time"] = pd.to_datetime(df["log_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    df["new_items"] = df["new_items"].apply(lambda x: json.loads(x) if x else {})
    df["removed_items"] = df["removed_items"].apply(lambda x: json.loads(x) if x else {})
    df["updated_items"] = df["updated_items"].apply(lambda x: json.loads(x) if x else {})

    df = df[~(df["new_items"].apply(lambda x: x == {}) &
              df["removed_items"].apply(lambda x: x == {}) &
              df["updated_items"].apply(lambda x: x == {}))]

    def expand_items(log_time, item_dict, change_type):
        rows = []
        for name, details in item_dict.items():
            row = {
                "log_time": log_time,
                "change_type": change_type,
                "item_name": name,
                "availability": details.get("availability", ""),
                "link": details.get("link", ""),
                "price": details.get("price", "")
            }
            rows.append(row)
        return rows

    expanded_data = []
    for _, row in df.iterrows():
        expanded_data.extend(expand_items(row["log_time"], row["new_items"], "new"))
        expanded_data.extend(expand_items(row["log_time"], row["removed_items"], "removed"))
        expanded_data.extend(expand_items(row["log_time"], row["updated_items"], "updated"))

    expanded_df = pd.DataFrame(expanded_data)

    expected_columns = ["log_time", "change_type", "item_name", "availability", "link", "price"]
    for col in expected_columns:
        if col not in expanded_df.columns:
            expanded_df[col] = ""

    expanded_df = expanded_df[expected_columns]
    return expanded_df



df_logs = fetch_comparison_log()
filtered_df = df_logs.copy()

with st.expander("🔍 Filter options", expanded=False):
    if st.button("❌ Clear Filters"):
        for column in df_logs.columns:
            if f"filter_{column}" in st.session_state:
                del st.session_state[f"filter_{column}"]

    col_filters = st.columns(len(filtered_df.columns))
    for i, column in enumerate(filtered_df.columns):
        unique_vals = sorted(filtered_df[column].dropna().unique())
        key = f"filter_{column}"

        selected_vals = col_filters[i].multiselect(
            f"{column}",
            options=unique_vals,
            default=st.session_state.get(key, []),
            key=key
        )

        if selected_vals:
            filtered_df = filtered_df[filtered_df[column].isin(selected_vals)]

nrows = len(filtered_df)

st.data_editor(
    filtered_df,
    use_container_width=True,
    hide_index=True,
    height=st_num_of_rows(filtered_df),
    column_config={"link": st.column_config.LinkColumn("link")}
)
