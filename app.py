import streamlit as st
from common import create_db

create_db()

pages = {
    "Menu": [
        st.Page("report.py", title="Report"),
        st.Page("schedule.py", title="Scheduler")
    ]
}

pg = st.navigation(pages)
pg.run()
