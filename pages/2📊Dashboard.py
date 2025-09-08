import streamlit as st
import pandas as pd
import plotly.express as px
import io
from core.db import get_conn, list_trainings, trainings_summary, distinct_employee_values, distinct_training_values, count_employees


def to_excel_bytes(df_dict: dict, filename="export.xlsx"):
    # df_dict = {"SheetName": dataframe, ...}
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for sheet, df in df_dict.items():
            # openpyxl sheet names max 31 chars
            safe = (sheet or "Sheet")[:31]
            df.to_excel(writer, index=False, sheet_name=safe)
    bio.seek(0)
    return bio.getvalue()

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
    total_emps = count_employees(conn)
    st.metric("Total Employees", total_emps)

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
st.download_button(
    "Download table (Excel)",
    data=to_excel_bytes({"Trainings": df}),
    file_name="trainings_filtered.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)
# Per-group multi-sheet workbook (each group's rows)
if not df.empty:
    group_frames = {}
    for g in df_sum[by.capitalize()].dropna().unique():
        group_frames[str(g)] = df[df[by.capitalize()] == g]
    if group_frames:
        st.download_button(
            f"Download per-{by} detail (multi-sheet Excel)",
            data=to_excel_bytes(group_frames),
            file_name=f"trainings_by_{by}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
st.caption(f"Total Employees (all time): {total_emps} | Trainings shown: {len(df)}")
from datetime import date

# canonical buckets for titles
from datetime import date

TITLE_BUCKETS = {
    "food safety": "Food Safety Training",
    "fire": "Fire Safety Training",
    "first aid": "First Aid Training",
    "pest": "Pest Control Training",
    "occupational health": "Occupational Health and Safety Training",
    "ohs": "Occupational Health and Safety Training",
    "6s": "6S Training",
    "water treatment": "Water Treatment Plant Training",
}

def bucket_title(title: str) -> str:
    if not title: return ""
    t = str(title).lower()
    for key, canon in TITLE_BUCKETS.items():
        if key in t:
            return canon
    return title

def build_matrix(conn):
    # employees (must include 'id')
    emps = pd.read_sql_query("""
        SELECT id, employee_code, name, store AS location, department AS division
        FROM employees
        WHERE deleted_at IS NULL
        ORDER BY name COLLATE NOCASE
    """, conn)

    if emps.empty:
        # still return empty matrix with columns
        out = emps.copy()
        out.insert(0, "S/N", range(1, len(out)+1))
        out.rename(columns={
            "employee_code": "EMPLOYEE CODE",
            "name": "EMPLOYEE NAME",
            "location": "LOCATION",
            "division": "DIVISION",
        }, inplace=True)
        for canon in TITLE_BUCKETS.values():
            for sub in ("DATE TRAINED","DUE DATE","NO OF DAYS PRIOR DUE DATE","STATUS"):
                out[f"{canon} - {sub}"] = ""
        return out

    # trainings
    trs = pd.read_sql_query("""
        SELECT t.employee_id, t.training_date, t.next_due_date, t.training_title
        FROM trainings t
        JOIN employees e ON e.id=t.employee_id
        WHERE t.deleted_at IS NULL AND e.deleted_at IS NULL
    """, conn)

    if trs.empty:
        out = emps.copy()
        out.insert(0, "S/N", range(1, len(out)+1))
        out.rename(columns={
            "employee_code": "EMPLOYEE CODE",
            "name": "EMPLOYEE NAME",
            "location": "LOCATION",
            "division": "DIVISION",
        }, inplace=True)
        for canon in TITLE_BUCKETS.values():
            for sub in ("DATE TRAINED","DUE DATE","NO OF DAYS PRIOR DUE DATE","STATUS"):
                out[f"{canon} - {sub}"] = ""
        return out

    trs["bucket"] = trs["training_title"].apply(bucket_title)
    trs["training_date"] = pd.to_datetime(trs["training_date"])
    trs.sort_values(["employee_id","bucket","training_date"], inplace=True)
    last = trs.groupby(["employee_id","bucket"], as_index=False).last()

    today = pd.Timestamp(date.today())
    last["next_due_date"] = pd.to_datetime(last["next_due_date"])
    last["days_prior_due"] = (last["next_due_date"].dt.normalize() - today.normalize()).dt.days
    last["status"] = last["days_prior_due"].apply(lambda d: "NOT DUE" if pd.notna(d) and d > 0 else "DUE")

    # build per-bucket frames
    wide = {}
    for canon in TITLE_BUCKETS.values():
        sub = last[last["bucket"] == canon][["employee_id","training_date","next_due_date","days_prior_due","status"]].copy()
        sub.columns = ["employee_id",
                       f"{canon} - DATE TRAINED",
                       f"{canon} - DUE DATE",
                       f"{canon} - NO OF DAYS PRIOR DUE DATE",
                       f"{canon} - STATUS"]
        wide[canon] = sub

    out = emps.copy()
    for canon, sub in wide.items():
        # FIX: merge on id (left) and employee_id (right)
        out = out.merge(sub, left_on="id", right_on="employee_id", how="left")
        out.drop(columns=["employee_id"], inplace=True)

    out.insert(0, "S/N", range(1, len(out)+1))
    out.rename(columns={
        "employee_code": "EMPLOYEE CODE",
        "name": "EMPLOYEE NAME",
        "location": "LOCATION",
        "division": "DIVISION",
    }, inplace=True)
    return out
matrix_df = build_matrix(conn)
# --- Matrix preview (first 50 by default) ---
st.subheader("Matrix preview")

# Choose how many rows to view
view_opt = st.radio("Rows to show", ["50", "100", "All"], horizontal=True, index=0)

if view_opt == "All":
    view_df = matrix_df
else:
    view_df = matrix_df.head(int(view_opt))

st.dataframe(view_df, use_container_width=True, height=520)

# Download exactly what's shown (Excel)
st.download_button(
    "Download shown (Excel)",
    data=to_excel_bytes({"Matrix": view_df}),   # uses the helper you already added
    file_name=f"training_matrix_{view_opt.lower()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)

# view the first 20 matrix
st.caption(f"Total Employees (all time): {count_employees(conn)}")
st.caption(f"Matrix generated on {date.today().isoformat()}")
st.caption(f"Total Employees (all time): {total_emps} | Trainings shown: {len(df)}")
