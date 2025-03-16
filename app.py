import streamlit as st

pages = {
    "Schedule": [
        st.Page("schedule.py", title="Schedule & Run Scraper")
    ],
    "Compare": [
        st.Page("compare.py", title="Compare Scrape Runs")
    ],
    "View Data": [
        st.Page("db_view.py", title="View Database Data")
    ],
}

pg = st.navigation(pages)
pg.run()
