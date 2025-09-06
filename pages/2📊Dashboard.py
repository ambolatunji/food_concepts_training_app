import streamlit as st
import pandas as pd
import plotly.express as px
from core.db import get_conn, list_trainings, trainings_summary, distinct_employee_values, distinct_training_values
from core.db import (
    get_conn, list_trainings, trainings_summary, distinct_employee_values, distinct_training_values
)

conn = get_conn()
st.title("📊 Dashboard")
st.caption("Filter by dimensions and date range to analyze training coverage and trends.")

regions = ["All"] + distinct_employee_values(conn, "region")
positions = ["All"] + distinct_employee_values(conn, "position")
stores = ["All"] + distinct_employee_values(conn, "store")
departments = ["All"] + distinct_employee_values(conn, "department")
titles = ["All"] + distinct_training_values(conn, "training_title")
venues = ["All"] + distinct_training_values(conn, "training_venue")

c1, c2, c3, c4 = st.columns(4)
with c1:
    region = st.selectbox("Region", regions)
with c2:
    department = st.selectbox("Department", departments)
with c3:
    store = st.selectbox("Store", stores)
with c4:
    position = st.selectbox("Position", positions)

c5, c6 = st.columns(2)
with c5:
    f_title = st.selectbox("Training Title", titles)
with c6:
    f_venue = st.selectbox("Training Venue", venues)

c7, c8 = st.columns(2)
with c7:
    date_from = st.date_input("From", value=None, format="YYYY-MM-DD")
with c8:
    date_to = st.date_input("To", value=None, format="YYYY-MM-DD")

filters={}
if region!="All": filters["region"]=region
if department!="All": filters["department"]=department
if store!="All": filters["store"]=store
if position!="All": filters["position"]=position
if date_from: filters["date_from"]=date_from.isoformat()
if date_to: filters["date_to"]=date_to.isoformat()

rows = list_trainings(conn, filters=filters)
df = pd.DataFrame(rows, columns=[
    "Training ID","Employee Name","Department","Store","Position","Region",
    "Training Date","Next Due Date","Evidence Path","Training Title","Training Venue"
])

# Client-side filter for title/venue (simpler than altering SQL)
if f_title!="All":
    df = df[df["Training Title"]==f_title]
if f_venue!="All":
    df = df[df["Training Venue"]==f_venue]

left, right = st.columns([2,3])

with left:
    st.metric("Total Trainings", len(df))
    if len(df)>0:
        overdue = (pd.to_datetime(df["Next Due Date"]) < pd.Timestamp.today().normalize()).sum()
        st.metric("Overdue", int(overdue))
        due_30 = (pd.to_datetime(df["Next Due Date"]) <= (pd.Timestamp.today().normalize()+pd.Timedelta(days=30))).sum()
        st.metric("Due in 30 days", int(due_30))

with right:
    if len(df)>0:
        df["Training Date"]=pd.to_datetime(df["Training Date"])
        trend = df.groupby(df["Training Date"].dt.to_period("M")).size().reset_index(name="Count")
        trend["Training Date"]=trend["Training Date"].astype(str)
        fig = px.line(trend, x="Training Date", y="Count", title="Training Trend (monthly)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trainings found for current filters.")

st.subheader("Breakdown")
by = st.selectbox("Group by", ["department","store","region","position","training_title","training_venue"], index=0)
summary = trainings_summary(conn, filters.get("date_from"), filters.get("date_to"), by=by)
df_sum = pd.DataFrame(summary, columns=[by.capitalize(),"Trainings"])
if len(df_sum)>0:
    fig2 = px.bar(df_sum, x=by.capitalize(), y="Trainings", title=f"Trainings by {by.capitalize()}")
    st.plotly_chart(fig2, use_container_width=True)
st.dataframe(df, use_container_width=True)

st.download_button(
    "Download table (CSV)",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="trainings_filtered.csv",
    mime="text/csv",
    use_container_width=True
)
