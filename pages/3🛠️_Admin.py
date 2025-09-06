import streamlit as st
import pandas as pd
from pathlib import Path
from core.db import get_conn, migrate, upsert_employee, find_employee_by_fields, insert_training
from core.logic import unique_key, normalize_str, to_date_str, canonicalize, REGION_SYNONYMS
from core.auth import build_authenticator
from core.templates import write_employee_template, write_training_template
from core.emailer import send_confirmation
from core.db import (
    get_conn, migrate, upsert_employee, find_employee_by_fields, insert_training
)


migrate()
conn = get_conn()

st.title("🛠️ Admin")

authenticator = build_authenticator()
#name, auth_status, username = authenticator.login("Login", "sidebar")
authenticator.login(location="sidebar", key="Login")
auth_status = st.session_state.get("authentication_status", None)
username = st.session_state.get("username", None)
#name = st.session_state.get("name", None)

if not auth_status:
    if auth_status is False:
        st.error("Invalid credentials")
    st.stop()

st.success(f"Logged in as {username}")
authenticator.logout(location="sidebar", key="logout")
if st.session_state.get("logout"):
    st.rerun()

st.subheader("Download Excel Templates")
c1, c2 = st.columns(2)
with c1:
    p = write_employee_template()
    st.download_button("Employee_Upload_Template.xlsx", data=open(p,"rb").read(),
                       file_name=p.name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with c2:
    p2 = write_training_template()
    st.download_button("Training_Upload_Template.xlsx", data=open(p2,"rb").read(),
                       file_name=p2.name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.subheader("Upload/Refresh Employee Master")
st.caption("De-duplicates within upload and against existing DB (normalized by Name + Email + Store + Department).")
up = st.file_uploader("Upload Employee Master (xlsx)", type=["xlsx"])
if up and st.button("Process Employee Upload", type="primary"):
    df = pd.read_excel(up)
    # Expected columns (loose): Employee Name, Email, Employee Code, Department, Store, Position, Region, Start Date
    # Normalize headers
    rename_map = {c: c.strip().lower() for c in df.columns}
    df.columns = [rename_map[c] for c in df.columns]
    def pick(df, *options):
        for o in options:
            if o in df.columns: return o
        return None

    c_name = pick(df, "employee name","name")
    c_email= pick(df, "email","work email")
    c_code = pick(df, "employee code","code")
    c_dept = pick(df, "department","dept")
    c_store= pick(df, "store","location")
    c_pos  = pick(df, "position","postion","postion ","postion  ")
    c_reg  = pick(df, "region")
    c_start= pick(df, "start date","start_date","start date (yyyy-mm-dd)")
    if not all([c_name, c_dept, c_store]):
        st.error("Employee Name, Department, and Store columns are required.")
        st.stop()

    df["_name"]=df[c_name].astype(str).apply(normalize_str)
    df["_email"]=df[c_email].astype(str).apply(normalize_str) if c_email else ""
    df["_dept"]=df[c_dept].astype(str).apply(normalize_str)
    df["_store"]=df[c_store].astype(str).apply(normalize_str)
    df["_pos"]=df[c_pos].astype(str).apply(normalize_str) if c_pos else ""
    df["_reg"]=df[c_reg].astype(str).apply(normalize_str) if c_reg else ""
    # canonicalize region via synonyms
    df["_reg"] = df["_reg"].apply(lambda v: canonicalize(v, REGION_SYNONYMS))

    # dedupe inside upload
    df["_uk"]=df.apply(lambda r: unique_key(r["_name"], r["_email"], r["_store"], r["_dept"]), axis=1)
    df = df.drop_duplicates(subset=["_uk"], keep="first")

    cnt=0
    for _,r in df.iterrows():
        row = {
            "employee_code": df[c_code][_] if c_code else "",
            "name": r["_name"],
            "email": r["_email"],
            "department": r["_dept"],
            "store": r["_store"],
            "position": r["_pos"],
            "region": r["_reg"],
            "start_date": to_date_str(str(df[c_start][_])) if c_start else ""
        }
        upsert_employee(conn, r["_uk"], row)
        cnt+=1
    st.success(f"Processed {cnt} employee records (deduped).")

# -------- Training History Upload (bulk) --------
st.subheader("Upload Training History (Bulk)")
st.caption("Use Training_Upload_Template.xlsx. Only Employee Name and Training Date are strictly required. Email/Store/Department help disambiguate.")

tup = st.file_uploader("Upload Training History (xlsx)", type=["xlsx"], key="training_upload")
if tup and st.button("Process Training Upload", type="primary", width='stretch'):
    import pandas as pd
    from core.logic import to_date_str, compute_next_due, normalize_str

    tf = pd.read_excel(tup)
    rmap = {c: c.strip().lower() for c in tf.columns}
    tf.columns = [rmap[c] for c in tf.columns]

    def pick(df, *options):
        for o in options:
            if o in df.columns: return o
        return None

    c_name = pick(tf, "employee name","name")
    c_email= pick(tf, "email","work email")
    c_dept = pick(tf, "department","dept")
    c_store= pick(tf, "store","location")
    c_titl = pick(tf, "training title","title")
    c_venu = pick(tf, "training venue","venue")
    c_date = pick(tf, "training date (yyyy-mm-dd)","training date","date")
    c_evid = pick(tf, "evidence file name (optional)","evidence file","evidence")

    # Only these are strictly required
    if c_name is None or c_date is None:
        st.error("Missing required columns. Need at least Employee Name and Training Date.")
        st.stop()

    # Helpers
    def find_unique_by_email(email):
        if not email: return None, 0
        rows = conn.execute("""
            SELECT id,name,email,department,store,position,region
            FROM employees WHERE lower(email)=?
        """, (email.lower().strip(),)).fetchall()
        return (rows[0], len(rows)) if rows else (None, 0)

    def find_candidates_by_name(name):
        return conn.execute("""
            SELECT id,name,email,department,store,position,region
            FROM employees WHERE lower(name)=?
        """, (name.lower().strip(),)).fetchall()

    inserted = 0
    invalid_date = 0
    not_found = 0
    ambiguous = 0
    other_errors = 0
    problems = []  # collect first few issues for display

    for i, row in tf.iterrows():
        name  = normalize_str(row[c_name]) if pd.notna(row[c_name]) else ""
        email = normalize_str(row[c_email]) if (c_email and pd.notna(row[c_email])) else ""
        dept  = normalize_str(row[c_dept]) if (c_dept and pd.notna(row[c_dept])) else ""
        store = normalize_str(row[c_store]) if (c_store and pd.notna(row[c_store])) else ""
        title = normalize_str(row[c_titl]) if (c_titl and pd.notna(row[c_titl])) else ""
        venue = normalize_str(row[c_venu]) if (c_venu and pd.notna(row[c_venu])) else ""
        dstr  = row[c_date] if pd.notna(row[c_date]) else None

        # Date parse
        tdate = to_date_str(dstr)
        if not tdate:
            invalid_date += 1
            if len(problems) < 20:
                problems.append({"row": int(i)+2, "issue": "Invalid date", "value": str(dstr)})
            continue

        # Find employee (flexible)
        emp_row = None
        candidates= []

        # 1) Email unique
        if email:
            candidates = conn.execute("""
                SELECT id,name,email,department,store,position,region,start_date
                FROM employees WHERE lower(email)=?
            """, (email.lower(),)).fetchall()
            # Narrow with provided name/store/department
            def _eq(a,b): return a and b and a.lower()==b.lower()
            if candidates:
                if name:  candidates = [r for r in candidates if _eq(r[1], name)]
                if store: candidates = [r for r in candidates if _eq(r[4], store)]
                if dept:  candidates = [r for r in candidates if _eq(r[3], dept)]
                if len(candidates)==1:
                    emp_row = candidates[0]
                elif len(candidates)>1:
                    # tie-breaker by most recent start_date
                    def _d(r):
                        from core.logic import to_date_str
                        return to_date_str(r[7]) or ""
                    candidates = sorted(candidates, key=_d, reverse=True)
                    emp_row = candidates[0]  # or comment this line to force "ambiguous"

        # 2) Full combo
        if emp_row is None and name and dept and store:
            emp_row = conn.execute("""
                SELECT id,name,email,department,store,position,region
                FROM employees
                WHERE lower(name)=? AND lower(department)=? AND lower(store)=?
            """, (name.lower(), dept.lower(), store.lower())).fetchone()

        # 3) Name + Store
        if emp_row is None and name and store:
            rows = conn.execute("""
                SELECT id,name,email,department,store,position,region
                FROM employees WHERE lower(name)=? AND lower(store)=?
            """, (name.lower(), store.lower())).fetchall()
            if len(rows) == 1:
                emp_row = rows[0]
            elif len(rows) > 1:
                ambiguous += 1
                if len(problems) < 20:
                    problems.append({"row": int(i)+2, "issue": "Ambiguous (name+store)", "value": f"{name} @ {store}"})
                continue

        # 4) Name + Department
        if emp_row is None and name and dept:
            rows = conn.execute("""
                SELECT id,name,email,department,store,position,region
                FROM employees WHERE lower(name)=? AND lower(department)=?
            """, (name.lower(), dept.lower())).fetchall()
            if len(rows) == 1:
                emp_row = rows[0]
            elif len(rows) > 1:
                ambiguous += 1
                if len(problems) < 20:
                    problems.append({"row": int(i)+2, "issue": "Ambiguous (name+dept)", "value": f"{name} / {dept}"})
                continue

        # 5) Name-only if unique
        if emp_row is None and name:
            rows = find_candidates_by_name(name)
            if len(rows) == 1:
                emp_row = rows[0]
            elif len(rows) > 1:
                ambiguous += 1
                if len(problems) < 20:
                    problems.append({"row": int(i)+2, "issue": "Ambiguous (name-only)", "value": name})
                continue

        if emp_row is None:
            not_found += 1
            if len(problems) < 20:
                problems.append({"row": int(i)+2, "issue": "Employee not found", "value": name})
            continue

        emp_id = emp_row[0]
        next_due = compute_next_due(tdate)

        try:
            insert_training(
                conn,
                employee_id=emp_id,
                training_date=tdate,
                next_due_date=next_due,
                evidence_path=None,
                evidence_mime=None,
                evidence_size=None,
                training_title=title or None,
                training_venue=venue or None
            )
            inserted += 1
        except Exception as e:
            other_errors += 1
            if len(problems) < 20:
                problems.append({"row": int(i)+2, "issue": f"DB insert error: {e}", "value": f"{name} {tdate}"})
            continue

    st.success(f"Training upload done. Inserted: {inserted}, Invalid date: {invalid_date}, Not found: {not_found}, Ambiguous: {ambiguous}, Other errors: {other_errors}.")

    if problems:
        st.caption("First few issues (row numbers are Excel rows, incl. header):")
        st.dataframe(pd.DataFrame(problems))

# -------- Bulk Reminder --------

st.subheader("Bulk Reminder (Optional)")
st.caption("Send manual reminders for people due within X days.")
days = st.number_input("Due within days", 1, 365, 30)
if st.button("Send Reminders Now"):
    q = """
    SELECT e.name, e.email, e.department, e.store, MIN(date(t.next_due_date))
    FROM trainings t JOIN employees e ON e.id=t.employee_id
    WHERE date(t.next_due_date) <= date('now','+%d days')
    GROUP BY e.id
    """ % days
    rows = conn.execute(q).fetchall()
    sent=0; failed=0
    for r in rows:
        name,email,dept,store,next_due = r
        if not email: 
            failed+=1
            continue
        subject = "Training Due Reminder – Food Concepts"
        body = f"""Dear {name},

Your next training is due on {next_due}.
Department: {dept}
Store: {store}

Please schedule accordingly.
"""
        ok, msg = send_confirmation(email, subject, body)
        if ok: sent+=1
        else: failed+=1
    st.success(f"Reminders attempted: {len(rows)}. Sent: {sent}. Failed: {failed}.")
