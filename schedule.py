import streamlit as st
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from common import DB_PATH, run_scrape_job


st.set_page_config(layout="wide", page_title="Schedule & Run Scraper")

st.write("## Schedule & Run Scraper Page Loaded")

# Initialize persistent scheduler
if "scheduler" not in st.session_state:
    jobstores = {
        'default': SQLAlchemyJobStore(url=f'sqlite:///{DB_PATH}')
    }
    scheduler = BackgroundScheduler(jobstores=jobstores)
    scheduler.start()
    st.session_state.scheduler = scheduler
    st.success("Scheduler created and started.")
else:
    st.info("Scheduler loaded from session state.")

# Run job on demand
if st.button("Run Scraper"):
    run_scrape_job()

# Job setup UI
job_type = st.selectbox("Job Type", options=["Cron", "Interval"])
job_name = st.text_input("Job Name", value="ScrapeJob")

def schedule_cron_jobs(job_name, frequency, days, times):
    job_list = []
    if frequency == "daily":
        for t in times:
            job_id = f"{job_name}_daily_{t.strftime('%H%M')}_{int(datetime.datetime.now().timestamp())}"
            trigger = CronTrigger(hour=t.hour, minute=t.minute)
            st.session_state.scheduler.add_job("tasks:run_scrape_job", trigger=trigger, args=[job_name], id=job_id, replace_existing=False)
            job = st.session_state.scheduler.get_job(job_id)
            job_list.append((job_id, job.next_run_time))
    elif frequency == "weekly":
        day_of_week = days.lower().replace(" ", "")
        t = times[0]
        job_id = f"{job_name}_weekly_{day_of_week}_{t.strftime('%H%M')}_{int(datetime.datetime.now().timestamp())}"
        trigger = CronTrigger(day_of_week=day_of_week, hour=t.hour, minute=t.minute)
        st.session_state.scheduler.add_job("tasks:run_scrape_job", trigger=trigger, args=[job_name], id=job_id, replace_existing=False)
        job = st.session_state.scheduler.get_job(job_id)
        job_list.append((job_id, job.next_run_time))
    elif frequency == "monthly":
        t = times[0]
        job_id = f"{job_name}_monthly_{t.strftime('%H%M')}_{int(datetime.datetime.now().timestamp())}"
        trigger = CronTrigger(day=1, hour=t.hour, minute=t.minute)
        st.session_state.scheduler.add_job("tasks:run_scrape_job", trigger=trigger, args=[job_name], id=job_id, replace_existing=False)
        job = st.session_state.scheduler.get_job(job_id)
        job_list.append((job_id, job.next_run_time))
    return job_list

def schedule_interval_job(job_name, interval_value, interval_unit):
    job_id = f"{job_name}_interval_{interval_value}{interval_unit}_{int(datetime.datetime.now().timestamp())}"
    if interval_unit == "minutes":
        trigger = IntervalTrigger(minutes=interval_value)
    elif interval_unit == "hours":
        trigger = IntervalTrigger(hours=interval_value)
    else:
        return None, None

    st.session_state.scheduler.add_job("tasks:run_scrape_job", trigger=trigger, args=[job_name], id=job_id, replace_existing=False)
    job = st.session_state.scheduler.get_job(job_id)
    return job_id, job.next_run_time

# Job scheduling UI
if job_type == "Cron":
    frequency = st.selectbox("Frequency", options=["daily", "weekly", "monthly"])
    if frequency == "daily":
        times_input = st.text_input("Enter times (comma separated, e.g., 08:00,12:00,18:00)", value="12:00")
        try:
            time_strings = [t.strip() for t in times_input.split(",") if t.strip()]
            times = [datetime.datetime.strptime(t, "%H:%M").time() for t in time_strings]
        except Exception as e:
            st.error("Error parsing times. Please ensure they are in HH:MM format.", e)
            times = []
    elif frequency == "weekly":
        days = st.text_input("Enter days (e.g., Mon,Wed,Fri)", value="Mon,Wed,Fri")
        time_input = st.time_input("Time for weekly job", value=datetime.time(12, 0))
        times = [time_input]
    elif frequency == "monthly":
        time_input = st.time_input("Time for monthly job", value=datetime.time(12, 0))
        times = [time_input]

    if st.button("Add Cron Job"):
        if frequency == "daily" and not times:
            st.error("Please enter valid times for daily job.")
        else:
            job_list = schedule_cron_jobs(job_name, frequency, days if frequency=="weekly" else "", times)
            if job_list:
                for jid, nxt in job_list:
                    st.success(f"Job '{jid}' scheduled. Next run at {nxt}")
            else:
                st.error("Error scheduling cron job.")
elif job_type == "Interval":
    interval_value = st.number_input("Interval Value", min_value=1, value=60, step=1)
    interval_unit = st.selectbox("Interval Unit", options=["minutes", "hours"])
    if st.button("Add Interval Job"):
        jid, nxt = schedule_interval_job(job_name, interval_value, interval_unit)
        if jid:
            st.success(f"Job '{jid}' scheduled. Next run at {nxt}")
        else:
            st.error("Error scheduling interval job.")

# Show existing jobs
st.subheader("Current Scheduled Jobs")
jobs = st.session_state.scheduler.get_jobs()
if jobs:
    for job in jobs:
        col1, col2, col3 = st.columns([3, 3, 1])
        with col1:
            st.write(f"**Job ID:** {job.id}")
        with col2:
            st.write(f"**Next Run:** {job.next_run_time}")
        with col3:
            if st.button("Remove", key=f"remove_{job.id}"):
                try:
                    st.session_state.scheduler.remove_job(job.id)
                    st.success(f"Deleted job: {job.id}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error deleting job {job.id}: {e}")
else:
    st.info("No scheduled jobs.")
