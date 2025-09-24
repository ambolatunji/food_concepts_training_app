import streamlit as st
import pandas as pd
from pathlib import Path
from core.logic import unique_key, normalize_str, to_date_str, canonicalize, REGION_SYNONYMS
from core.auth import build_authenticator
from core.templates import write_employee_template, write_training_template
from core.emailer import send_confirmation
from pathlib import Path
import pandas as pd
from core.db import (
    get_conn, migrate, upsert_employee, find_employee_by_fields, insert_training, process_hires_df, process_leavers_df,
    soft_delete_training, restore_training, hard_delete_training, list_deleted_trainings,
    soft_delete_employee, restore_employee, hard_delete_employee, list_deleted_employees,
    list_change_requests, approve_change_request, reject_change_request,
    soft_delete_all_trainings, restore_all_trainings, hard_delete_all_trainings,
    soft_delete_all_employees, restore_all_employees, hard_delete_all_employees,
    count_employees
)
import json
import io

def _pick(df, *options):
    for o in options:
        if o in df.columns: return o
    return None

import io
def to_excel_bytes(sheets: dict) -> bytes:
    # sheets = {"SheetName": df, ...}
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        for name, df in sheets.items():
            safe = (name or "Sheet")[:31]
            df.to_excel(xw, index=False, sheet_name=safe)
    bio.seek(0)
    return bio.getvalue()

def _process_employee_df(conn, df: pd.DataFrame):
    rename = {c: c.strip().lower() for c in df.columns}
    df.columns = [rename[c] for c in df.columns]
    c_name = _pick(df, "employee name","name")
    c_email= _pick(df, "email","work email")
    c_code = _pick(df, "employee code","code")
    c_dept = _pick(df, "department","dept")
    c_store= _pick(df, "store","location")
    c_pos  = _pick(df, "position","postion","postion ","postion  ")
    c_reg  = _pick(df, "region")
    c_start= _pick(df, "start date (yyyy-mm-dd)","start date","start_date")
    if not all([c_name, c_dept, c_store]):
        return 0, "Employee Name, Department, and Store columns are required."

    from core.logic import normalize_str, unique_key, to_date_str
    df["_name"]=df[c_name].astype(object).apply(normalize_str)
    df["_email"]=df[c_email].astype(object).apply(normalize_str) if c_email else ""
    df["_dept"]=df[c_dept].astype(object).apply(normalize_str)
    df["_store"]=df[c_store].astype(object).apply(normalize_str)
    df["_pos"]=df[c_pos].astype(object).apply(normalize_str) if c_pos else ""
    df["_reg"]=df[c_reg].astype(object).apply(normalize_str) if c_reg else ""
    df["_uk"]=df.apply(lambda r: unique_key(r["_name"], r["_email"], r["_store"], r["_dept"]), axis=1)
    df = df.drop_duplicates(subset=["_uk"], keep="first")

    cnt=0
    for i,r in df.iterrows():
        row = {
            "employee_code": (df[c_code][i] if c_code in df.columns else ""),
            "name": r["_name"],
            "email": r["_email"] if isinstance(r["_email"], str) else "",
            "department": r["_dept"],
            "store": r["_store"],
            "position": r["_pos"],
            "region": r["_reg"],
            "start_date": to_date_str(df[c_start][i]) if c_start in df.columns else ""
        }
        upsert_employee(conn, r["_uk"], row)
        cnt+=1
    return cnt, "OK"

def _process_training_df(conn, df: pd.DataFrame):
    rmap = {c: c.strip().lower() for c in df.columns}
    df.columns = [rmap[c] for c in df.columns]
    c_name = _pick(df, "employee name","name")
    c_email= _pick(df, "email","work email")
    c_dept = _pick(df, "department","dept")
    c_store= _pick(df, "store","location")
    c_titl = _pick(df, "training title","title")
    c_venu = _pick(df, "training venue","venue")
    c_date = _pick(df, "training date (yyyy-mm-dd)","training date","date")
    if c_name is None or c_date is None:
        return 0, "Need at least Employee Name and Training Date."

    from core.logic import normalize_str, to_date_str, compute_next_due
    inserted = 0
    for _, row in df.iterrows():
        name  = normalize_str(row[c_name]) if c_name in df.columns else ""
        email = normalize_str(row[c_email]) if c_email in df.columns else ""
        dept  = normalize_str(row[c_dept]) if c_dept in df.columns else ""
        store = normalize_str(row[c_store]) if c_store in df.columns else ""
        title = normalize_str(row[c_titl]) if c_titl in df.columns else ""
        venue = normalize_str(row[c_venu]) if c_venu in df.columns else ""
        tdate = to_date_str(row[c_date])

        if not name or not tdate:
            continue

        # Flexible matching (email -> name+store+dept -> name+store -> name+dept -> unique name)
        emp_row = None
        if email:
            cand = conn.execute("""
                SELECT id,name,email,department,store,position,region,start_date
                FROM employees
                WHERE deleted_at IS NULL AND lower(email)=?
            """, (email.lower(),)).fetchall()
            def _eq(a,b): return a and b and a.lower()==b.lower()
            if cand:
                if name:  cand = [r for r in cand if _eq(r[1], name)]
                if store: cand = [r for r in cand if _eq(r[4], store)]
                if dept:  cand = [r for r in cand if _eq(r[3], dept)]
                if len(cand)==1:
                    emp_row = cand[0]
                elif len(cand)>1:
                    # tie-break by most recent start_date, else first
                    from core.logic import to_date_str as _d
                    cand = sorted(cand, key=lambda r: _d(r[7]) or "", reverse=True)
                    emp_row = cand[0]
        if emp_row is None and name and store and dept:
            emp_row = conn.execute("""
                SELECT id,name,email,department,store,position,region
                FROM employees
                WHERE deleted_at IS NULL AND lower(name)=? AND lower(store)=? AND lower(department)=?
            """, (name.lower(), store.lower(), dept.lower())).fetchone()
        if emp_row is None and name and store:
            rows = conn.execute("""
                SELECT id,name,email,department,store,position,region
                FROM employees WHERE deleted_at IS NULL AND lower(name)=? AND lower(store)=?
            """, (name.lower(), store.lower())).fetchall()
            if len(rows)==1: emp_row = rows[0]
        if emp_row is None and name and dept:
            rows = conn.execute("""
                SELECT id,name,email,department,store,position,region
                FROM employees WHERE deleted_at IS NULL AND lower(name)=? AND lower(department)=?
            """, (name.lower(), dept.lower())).fetchall()
            if len(rows)==1: emp_row = rows[0]
        if emp_row is None and name:
            rows = conn.execute("""
                SELECT id,name,email,department,store,position,region
                FROM employees WHERE deleted_at IS NULL AND lower(name)=?
            """, (name.lower(),)).fetchall()
            if len(rows)==1: emp_row = rows[0]

        if not emp_row: 
            continue

        next_due = compute_next_due(tdate)
        insert_training(conn,
            employee_id=emp_row[0],
            training_date=tdate,
            next_due_date=next_due,
            evidence_path=None, evidence_mime=None, evidence_size=None,
            training_title=title or None, training_venue=venue or None
        )
        inserted += 1

    return inserted, "OK"

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

st.subheader("Auto-import from /data (runs if DB is empty)")

auto_log = st.empty()
did_anything = False

try:
    # 1) If no employees yet, try to auto-import employees
    if count_employees(conn) == 0:
        candidates_emp = [
            Path("data/Employee_Upload_Template.xlsx"),
        ] + list(Path("data").glob("Employee*.xlsx")) + list(Path("data").glob("Employees*.xlsx"))
        for p in candidates_emp:
            if p.exists():
                df = pd.read_excel(p)
                n, msg = _process_employee_df(conn, df)
                auto_log.info(f"Imported employees from {p.name}: {n} rows ({msg})")
                did_anything = True
                break

    # 2) If trainings table empty, try to auto-import trainings
    cnt_trainings = conn.execute("SELECT COUNT(*) FROM trainings").fetchone()[0]
    if cnt_trainings == 0 and count_employees(conn) > 0:
        candidates_trn = [
            Path("data/Training_Upload_Template.xlsx"),
        ] + list(Path("data").glob("Training*.xlsx")) + list(Path("data").glob("Trainings*.xlsx"))
        for p in candidates_trn:
            if p.exists():
                df = pd.read_excel(p)
                n, msg = _process_training_df(conn, df)
                auto_log.info(f"Imported trainings from {p.name}: {n} rows ({msg})")
                did_anything = True
                break
except Exception as e:
    st.warning(f"Auto-import skipped due to: {e}")

if not did_anything:
    st.caption("No matching files found in /data, or DB already contains data. You can still upload manually below.")
else:
    st.success("Auto-import attempt completed.")

st.caption("Scans /data for Excel files. If filename contains **hire** or **leave/leaver/resign/exit**, "
           "it will import accordingly (fallback: header detection). Safe to run multiple times — events are deduped.")

if st.button("Scan /data now", type="primary", use_container_width=True):
    data_dir = Path("data")
    if not data_dir.exists():
        st.warning("No /data folder found.")
    else:
        total_hires = 0
        total_leavers = 0
        logs = []
        files = list(data_dir.glob("*.xlsx")) + list(data_dir.glob("*.xls"))
        for p in files:
            try:
                df = pd.read_excel(p)
                cols = [c.strip().lower() for c in df.columns]
                name = p.name.lower()

                # filename clues
                is_hire_by_name = "hire" in name
                is_leave_by_name = any(x in name for x in ["leave","leaver","resign","exit"])

                # header clues
                has_start = any(("startdate" == c) or ("start date" in c) or ("start_date" == c) for c in cols)
                has_end   = any(("end date" in c) or ("leaving date" in c) or ("resign" in c) or ("exit date" in c) for c in cols)
                decided = None
                if is_hire_by_name or (has_start and not has_end):
                    n, msg = process_hires_df(conn, df, source_name=p.name)
                    total_hires += n
                    logs.append(f"✅ {p.name}: Hires imported = {n} ({msg})")
                    decided = "hire"
                if is_leave_by_name or has_end:
                    n, msg = process_leavers_df(conn, df, source_name=p.name)
                    total_leavers += n
                    logs.append(f"✅ {p.name}: Leavers imported = {n} ({msg})")
                    decided = "leave"
                if decided is None:
                    logs.append(f"⚠️ {p.name}: could not classify (no clear Start/End headers or hint in filename)")
            except Exception as e:
                logs.append(f"❌ {p.name}: error {e}")

        st.success(f"Done. Hires: {total_hires}, Leavers: {total_leavers}")
        with st.expander("Details"):
            for m in logs:
                st.write(m)


st.subheader("Download Excel Templates")
def build_hire_template():
    cols = ["Employee Code","Employee Name","Department","Region","Store","Start Date (YYYY-MM-DD)","Position"]
    df = pd.DataFrame(columns=cols)
    return to_excel_bytes({"Hires_Template": df})

def build_leaver_template():
    cols = ["Employee Code","Employee Name","Department","Region","Store","End Date (YYYY-MM-DD)","Position","Start Date (optional)"]
    df = pd.DataFrame(columns=cols)
    return to_excel_bytes({"Leavers_Template": df})
c1, c2, c3, c4 = st.columns(4)
with c1:
    p = write_employee_template()
    st.download_button("Employee_Upload_Template.xlsx", data=open(p,"rb").read(),
                       file_name=p.name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with c2:
    p2 = write_training_template()
    st.download_button("Training_Upload_Template.xlsx", data=open(p2,"rb").read(),
                       file_name=p2.name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with c3:
    st.download_button("Hires_Upload_Template.xlsx", data=build_hire_template(),
                       file_name="Hires_Upload_Template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with c4:
    st.download_button("Leavers_Upload_Template.xlsx", data=build_leaver_template(),
                       file_name="Leavers_Upload_Template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

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
if tup and st.button("Process Training Upload", type="primary", use_container_width=True):
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

st.subheader("Upload New Hires & Leavers (two files at once)")
st.caption("Drop both Excel files here; I’ll auto-detect which is Hires vs Leavers by headers (presence of End Date).")

files = st.file_uploader("Upload files", type=["xlsx","xls"], accept_multiple_files=True, key="hires_leavers")
if files and st.button("Process Hires/Leavers", type="primary", use_container_width=True):
    import pandas as pd
    total_hires=0; total_leavers=0; msgs=[]
    for f in files:
        try:
            df = pd.read_excel(f)
            cols = [c.strip().lower() for c in df.columns]
            has_end = any(("end date" in c) or ("leaving date" in c) or ("resign" in c) or ("exit date" in c) for c in cols)
            has_start = any(("start" in c and "date" in c) for c in cols)

            if has_end:
                n, msg = process_leavers_df(conn, df, source_name=f.name)
                total_leavers += n; msgs.append(f"{f.name}: leavers {n} ({msg})")
            elif has_start:
                n, msg = process_hires_df(conn, df, source_name=f.name)
                total_hires += n; msgs.append(f"{f.name}: hires {n} ({msg})")
            else:
                msgs.append(f"{f.name}: could not classify (no Start/End headers)")
        except Exception as e:
            msgs.append(f"{f.name}: error {e}")
    st.success(f"Processed. Hires: {total_hires}, Leavers: {total_leavers}")
    with st.expander("Details"):
        for m in msgs:
            st.write("- ", m)


st.subheader("Manage Trainings")
# show a small recent list to act on
recent = conn.execute("""
    SELECT t.id, e.name, e.department, e.store, t.training_title, t.training_venue, t.training_date, t.next_due_date
    FROM trainings t JOIN employees e ON e.id=t.employee_id
    WHERE t.deleted_at IS NULL
    ORDER BY date(t.training_date) DESC
    LIMIT 200
""").fetchall()
df_recent = pd.DataFrame(recent, columns=["ID","Name","Department","Store","Title","Venue","Date","Next Due"])
st.dataframe(df_recent, use_container_width=True, height=300)

tid = st.number_input("Training ID to delete (soft-delete)", min_value=0, step=1)
if st.button("Soft-delete training", use_container_width=True):
    if tid > 0:
        soft_delete_training(conn, int(tid))
        st.success(f"Training {int(tid)} moved to Recycle Bin.")
        st.rerun()
st.subheader("Recycle Bin – Trainings")
trash = list_deleted_trainings(conn)
df_trash = pd.DataFrame(trash, columns=["ID","Name","Department","Store","Date","Title","Venue","Deleted At"])
st.dataframe(df_trash, use_container_width=True, height=240)

colR1, colR2 = st.columns(2)
with colR1:
    rid = st.number_input("Training ID to restore", min_value=0, step=1, key="restore_tid")
    if st.button("Restore training", use_container_width=True):
        if rid>0:
            restore_training(conn, int(rid))
            st.success(f"Training {int(rid)} restored.")
            st.rerun()
with colR2:
    hid = st.number_input("Training ID to permanently delete", min_value=0, step=1, key="hard_tid")
    if st.button("Permanently delete training", use_container_width=True, type="secondary"):
        if hid>0:
            hard_delete_training(conn, int(hid))
            st.success(f"Training {int(hid)} permanently deleted.")
            st.rerun()

st.subheader("Danger Zone – Bulk Actions")

entity = st.radio("Target", ["Trainings","Employees"], horizontal=True)
action = st.radio("Action", ["Soft delete ALL (to Recycle Bin)","Restore ALL from Recycle Bin","Permanently delete ALL"], horizontal=False)
confirm = st.text_input("Type YES to confirm")

if st.button("Execute bulk action", type="secondary", use_container_width=True):
    if confirm.strip().upper() != "YES":
        st.error("Type YES to confirm.")
    else:
        if entity == "Trainings":
            if action.startswith("Soft delete"): 
                soft_delete_all_trainings(conn); st.success("All trainings soft-deleted.")
            elif action.startswith("Restore"):
                restore_all_trainings(conn); st.success("All trainings restored.")
            else:
                hard_delete_all_trainings(conn); st.success("All trainings permanently deleted.")
        else:
            if action.startswith("Soft delete"):
                soft_delete_all_employees(conn); st.success("All employees soft-deleted.")
            elif action.startswith("Restore"):
                restore_all_employees(conn); st.success("All employees restored.")
            else:
                hard_delete_all_employees(conn); st.success("All employees permanently deleted (and their trainings).")
        st.rerun()

st.subheader("Correction Requests (Pending)")
pending = list_change_requests(conn, "pending")
dfp = pd.DataFrame(pending, columns=["ID","Employee ID","Payload JSON","Status","Created At","Decided At","Decided By"])
st.dataframe(dfp, use_container_width=True, height=240)

cc1, cc2 = st.columns(2)
with cc1:
    app_id = st.number_input("Request ID to approve", min_value=0, step=1)
    if st.button("Approve request", use_container_width=True):
        if app_id>0:
            ok, msg = approve_change_request(conn, int(app_id), decided_by=username or "admin")
            if ok: st.success(msg); st.rerun()
            else: st.error(msg)
with cc2:
    rej_id = st.number_input("Request ID to reject", min_value=0, step=1, key="rej_id")
    if st.button("Reject request", use_container_width=True, type="secondary"):
        if rej_id>0:
            reject_change_request(conn, int(rej_id), decided_by=username or "admin")
            st.success("Rejected."); st.rerun()

st.subheader("Approved Corrections (Download)")
approved = list_change_requests(conn, "approved")
dfa = pd.DataFrame(approved, columns=["ID","Employee ID","Payload JSON","Status","Created At","Decided At","Decided By"])
def _explode_payload(df):
    # turn JSON payload into columns
    rows=[]
    for _,r in df.iterrows():
        payload = json.loads(r["Payload JSON"])
        base = r.to_dict()
        base.update(payload)
        rows.append(base)
    return pd.DataFrame(rows)

if len(dfa)>0:
    df_exp = _explode_payload(dfa)
    from io import BytesIO
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        df_exp.to_excel(xw, index=False, sheet_name="approved_updates")
    bio.seek(0)
    st.download_button(
        "Download approved updates (Excel)",
        data=bio.getvalue(),
        file_name="approved_corrections.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
else:
    st.caption("No approved corrections yet.")

