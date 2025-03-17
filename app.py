import streamlit as st
from common import create_db

create_db()

pages = {
    "Menu": [
        st.Page("report.py", title="Report"),
        st.Page("schedule.py", title="Schedule & Run Scraper"),
        st.Page("dbview.py", title="View Database Data")
    ]
}

pg = st.navigation(pages)
pg.run()
