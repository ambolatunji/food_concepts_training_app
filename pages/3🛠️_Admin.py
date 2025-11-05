import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date
from core.logic import unique_key, normalize_str, normalize_name, normalize_field, to_date_str, canonicalize, normalize_training_title, REGION_SYNONYMS
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
    count_employees, find_duplicate_employees, merge_duplicate_employees, now
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

    from core.logic import normalize_str, normalize_name, normalize_field, unique_key, to_date_str
    df["_name"]=df[c_name].astype(object).apply(normalize_name)
    df["_email"]=df[c_email].astype(object).apply(lambda x: normalize_str(x).lower()) if c_email else ""
    df["_dept"]=df[c_dept].astype(object).apply(normalize_field)
    df["_store"]=df[c_store].astype(object).apply(normalize_field)
    df["_pos"]=df[c_pos].astype(object).apply(normalize_field) if c_pos else ""
    df["_reg"]=df[c_reg].astype(object).apply(normalize_field) if c_reg else ""
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
    c_reg  = _pick(df, "region")
    c_titl = _pick(df, "training title","title")
    c_venu = _pick(df, "training venue","venue")
    c_date = _pick(df, "training date (yyyy-mm-dd)","training date","date")
    if c_name is None or c_date is None:
        return 0, "Need at least Employee Name and Training Date."

    from core.logic import normalize_str, normalize_name, normalize_field, to_date_str, compute_next_due
    inserted = 0
    for _, row in df.iterrows():
        name  = normalize_name(row[c_name]) if c_name in df.columns else ""
        email = normalize_str(row[c_email]).lower() if c_email in df.columns else ""
        dept  = normalize_field(row[c_dept]) if c_dept in df.columns else ""
        store = normalize_field(row[c_store]) if c_store in df.columns else ""
        region = normalize_field(row[c_reg]) if c_reg in df.columns else ""
        title = normalize_training_title(row[c_titl]) if c_titl in df.columns else ""
        venue = normalize_field(row[c_venu]) if c_venu in df.columns else ""
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

        # Auto-create employee if not found
        if not emp_row:
            if not name:
                continue

            # Create new employee record
            try:
                conn.execute("""
                    INSERT INTO employees (name, email, employee_code, department, store, position, region, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, email or "", "", dept or "", store or "", "", region or "", now(), now()))
                conn.commit()

                # Fetch the newly created employee
                emp_row = conn.execute("""
                    SELECT id, name, email, department, store, position, region
                    FROM employees WHERE name=? AND email=? AND department=? AND store=?
                    ORDER BY id DESC LIMIT 1
                """, (name, email or "", dept or "", store or "")).fetchone()

                if not emp_row:
                    continue
            except Exception:
                continue

        # Sync logic: Update employee with training data
        emp_id = emp_row[0]
        db_dept = emp_row[3] if len(emp_row) > 3 else ""
        db_store = emp_row[4] if len(emp_row) > 4 else ""
        db_region = emp_row[6] if len(emp_row) > 6 else ""
        training_region = region if region else db_region

        # Update employee fields if training data differs
        needs_update = False
        updates = {}

        if dept and (not db_dept or dept.lower() != db_dept.lower()):
            updates["department"] = dept
            needs_update = True

        if store and (not db_store or store.lower() != db_store.lower()):
            updates["store"] = store
            needs_update = True

        if region and (not db_region or region.lower() != db_region.lower()):
            updates["region"] = region
            needs_update = True

        if needs_update:
            set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
            update_vals = list(updates.values()) + [now(), emp_id]
            conn.execute(f"UPDATE employees SET {set_clause}, updated_at=? WHERE id=?", update_vals)
            conn.commit()

        next_due = compute_next_due(tdate)
        insert_training(conn,
            employee_id=emp_id,
            training_date=tdate,
            next_due_date=next_due,
            evidence_path=None, evidence_mime=None, evidence_size=None,
            training_title=title or None, training_venue=venue or None,
            region=training_region or None
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

st.subheader("📥 Download All Data (Full Database Export)")
st.caption("Export complete datasets for backup, analysis, or external reporting")

dl_col1, dl_col2, dl_col3, dl_col4 = st.columns(4)

with dl_col1:
    # All Employees
    all_emps = conn.execute("""
        SELECT employee_code, name, email, department, store, position, region, start_date, end_date, created_at
        FROM employees
        WHERE deleted_at IS NULL
        ORDER BY name COLLATE NOCASE
    """).fetchall()

    if all_emps:
        df_all_emps = pd.DataFrame(all_emps, columns=[
            "Employee Code", "Name", "Email", "Department", "Store", "Position", "Region", "Start Date", "End Date", "Created At"
        ])
        st.download_button(
            "📊 All Employees",
            data=to_excel_bytes({"All Employees": df_all_emps}),
            file_name=f"all_employees_{date.today().isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(f"{len(all_emps)} employees")
    else:
        st.info("No employees")

with dl_col2:
    # All Trainings
    all_trains = conn.execute("""
        SELECT e.name, e.email, e.department, e.store, e.position, e.region,
               t.training_title, t.training_venue, t.training_date, t.next_due_date, t.created_at
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE t.deleted_at IS NULL AND e.deleted_at IS NULL
        ORDER BY t.training_date DESC
    """).fetchall()

    if all_trains:
        df_all_trains = pd.DataFrame(all_trains, columns=[
            "Employee Name", "Email", "Department", "Store", "Position", "Region",
            "Training Title", "Training Venue", "Training Date", "Next Due Date", "Created At"
        ])
        st.download_button(
            "📚 All Trainings",
            data=to_excel_bytes({"All Trainings": df_all_trains}),
            file_name=f"all_trainings_{date.today().isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(f"{len(all_trains)} trainings")
    else:
        st.info("No trainings")

with dl_col3:
    # All New Hires
    all_hires = conn.execute("""
        SELECT e.employee_code, e.name, e.email, e.department, e.store, e.position, e.region, e.start_date,
               ee.event_date as hire_date, ee.source
        FROM employees e
        INNER JOIN employment_events ee ON e.id = ee.employee_id
        WHERE e.deleted_at IS NULL
          AND ee.event_type = 'hire'
        ORDER BY ee.event_date DESC
    """).fetchall()

    if all_hires:
        df_all_hires = pd.DataFrame(all_hires, columns=[
            "Employee Code", "Name", "Email", "Department", "Store", "Position", "Region", "Start Date", "Hire Date", "Source"
        ])
        st.download_button(
            "👤 All New Hires",
            data=to_excel_bytes({"All New Hires": df_all_hires}),
            file_name=f"all_new_hires_{date.today().isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(f"{len(all_hires)} hires")
    else:
        st.info("No hire events")

with dl_col4:
    # All Leavers
    all_leavers = conn.execute("""
        SELECT e.employee_code, e.name, e.email, e.department, e.store, e.position, e.region,
               e.start_date, e.end_date,
               ee.event_date as leave_date, ee.source
        FROM employees e
        LEFT JOIN employment_events ee ON e.id = ee.employee_id AND ee.event_type = 'leave'
        WHERE e.end_date IS NOT NULL OR e.deleted_at IS NOT NULL
        ORDER BY COALESCE(ee.event_date, e.end_date) DESC
    """).fetchall()

    if all_leavers:
        df_all_leavers = pd.DataFrame(all_leavers, columns=[
            "Employee Code", "Name", "Email", "Department", "Store", "Position", "Region", "Start Date", "End Date", "Leave Date", "Source"
        ])
        st.download_button(
            "👋 All Leavers",
            data=to_excel_bytes({"All Leavers": df_all_leavers}),
            file_name=f"all_leavers_{date.today().isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(f"{len(all_leavers)} leavers")
    else:
        st.info("No leavers")

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
    c_reg  = pick(tf, "region")
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
            FROM employees WHERE deleted_at IS NULL AND lower(name)=?
        """, (name.lower().strip(),)).fetchall()

    inserted = 0
    invalid_date = 0
    not_found = 0
    ambiguous = 0
    other_errors = 0
    tie_broken = 0  # count of automatic tie-breaking by start_date
    overridden = 0  # count of employee records updated with new store/dept from training data
    problems = []  # collect first few issues for display
    override_details = []  # track what was overridden

    for i, row in tf.iterrows():
        name  = normalize_name(row[c_name]) if pd.notna(row[c_name]) else ""
        email = normalize_str(row[c_email]).lower() if (c_email and pd.notna(row[c_email])) else ""
        dept  = normalize_field(row[c_dept]) if (c_dept and pd.notna(row[c_dept])) else ""
        store = normalize_field(row[c_store]) if (c_store and pd.notna(row[c_store])) else ""
        region = normalize_field(row[c_reg]) if (c_reg and pd.notna(row[c_reg])) else ""
        title = normalize_training_title(row[c_titl]) if (c_titl and pd.notna(row[c_titl])) else ""
        venue = normalize_field(row[c_venu]) if (c_venu and pd.notna(row[c_venu])) else ""
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
                    emp_row = candidates[0]
                    tie_broken += 1
                    if len(problems) < 20:
                        problems.append({"row": int(i)+2, "issue": "WARNING: Auto tie-break by email",
                                       "value": f"{email} - Selected ID:{emp_row[0]} (most recent hire)",
                                       "matches": f"{len(candidates)} employees share this email"})

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
                FROM employees WHERE deleted_at IS NULL AND lower(name)=? AND lower(store)=?
            """, (name.lower(), store.lower())).fetchall()
            if len(rows) == 1:
                emp_row = rows[0]
            elif len(rows) > 1:
                ambiguous += 1
                if len(problems) < 20:
                    # Show ALL matching employees for better debugging
                    matches_detail = " | ".join([f"ID:{r[0]} Dept:{r[3]} Store:{r[4]}" for r in rows[:5]])
                    problems.append({"row": int(i)+2, "issue": "Ambiguous (name+store)", "value": f"{name} @ {store}", "matches": matches_detail})
                continue

        # 4) Name + Department
        if emp_row is None and name and dept:
            rows = conn.execute("""
                SELECT id,name,email,department,store,position,region
                FROM employees WHERE deleted_at IS NULL AND lower(name)=? AND lower(department)=?
            """, (name.lower(), dept.lower())).fetchall()
            if len(rows) == 1:
                emp_row = rows[0]
            elif len(rows) > 1:
                ambiguous += 1
                if len(problems) < 20:
                    # Show ALL matching employees for better debugging
                    matches_detail = " | ".join([f"ID:{r[0]} Store:{r[4]} Dept:{r[3]}" for r in rows[:5]])
                    problems.append({"row": int(i)+2, "issue": "Ambiguous (name+dept)", "value": f"{name} / {dept}", "matches": matches_detail})
                continue

        # 5) Name-only if unique
        if emp_row is None and name:
            rows = find_candidates_by_name(name)
            if len(rows) == 1:
                emp_row = rows[0]
            elif len(rows) > 1:
                ambiguous += 1
                if len(problems) < 20:
                    # Show ALL matching employees for better debugging
                    matches_detail = " | ".join([f"ID:{r[0]} Store:{r[4]} Dept:{r[3]}" for r in rows[:5]])
                    problems.append({"row": int(i)+2, "issue": "Ambiguous (name-only)", "value": name, "matches": matches_detail})
                continue

        # Auto-create employee if not found
        if emp_row is None:
            if not name:
                not_found += 1
                if len(problems) < 20:
                    problems.append({"row": int(i)+2, "issue": "Employee not found (no name)", "value": ""})
                continue

            # Create new employee record
            try:
                conn.execute("""
                    INSERT INTO employees (name, email, employee_code, department, store, position, region, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, email or "", "", dept or "", store or "", "", region or "", now(), now()))
                conn.commit()

                # Fetch the newly created employee
                emp_row = conn.execute("""
                    SELECT id, name, email, department, store, position, region
                    FROM employees WHERE name=? AND email=? AND department=? AND store=?
                    ORDER BY id DESC LIMIT 1
                """, (name, email or "", dept or "", store or "")).fetchone()

                if emp_row:
                    overridden += 1
                    if len(override_details) < 50:
                        override_details.append({
                            "row": int(i)+2,
                            "employee_id": emp_row[0],
                            "name": name,
                            "changes": f"NEW EMPLOYEE CREATED from training upload - Store: {store}, Dept: {dept}, Region: {region}"
                        })
                else:
                    not_found += 1
                    if len(problems) < 20:
                        problems.append({"row": int(i)+2, "issue": "Failed to create employee", "value": name})
                    continue
            except Exception as e:
                not_found += 1
                if len(problems) < 20:
                    problems.append({"row": int(i)+2, "issue": f"Error creating employee: {e}", "value": name})
                continue

        emp_id = emp_row[0]
        db_region = emp_row[6] if len(emp_row) > 6 else ""

        # Sync logic: Always update employee with training data if provided
        training_region = region if region else db_region

        # Check if store/department/region in upload differs from employee master - if so, UPDATE employee record
        db_dept = emp_row[3] if len(emp_row) > 3 else ""
        db_store = emp_row[4] if len(emp_row) > 4 else ""

        needs_update = False
        updates = {}

        # Always update if training data has these fields and they differ
        if dept and (not db_dept or dept.lower() != db_dept.lower()):
            updates["department"] = dept
            needs_update = True

        if store and (not db_store or store.lower() != db_store.lower()):
            updates["store"] = store
            needs_update = True

        if region and (not db_region or region.lower() != db_region.lower()):
            updates["region"] = region
            needs_update = True

        if needs_update:
            # Update employee record with new store/department/region from training upload
            try:
                set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
                update_vals = list(updates.values()) + [now(), emp_id]
                conn.execute(f"UPDATE employees SET {set_clause}, updated_at=? WHERE id=?", update_vals)
                conn.commit()
                overridden += 1

                override_info = f"ID:{emp_id} {name} - "
                changes = []
                if "department" in updates:
                    changes.append(f"Dept: {db_dept} → {dept}")
                if "store" in updates:
                    changes.append(f"Store: {db_store} → {store}")
                if "region" in updates:
                    changes.append(f"Region: {db_region} → {region}")
                override_info += " | ".join(changes)

                if len(override_details) < 50:  # Track first 50 overrides
                    override_details.append({
                        "row": int(i)+2,
                        "employee_id": emp_id,
                        "name": name,
                        "changes": override_info
                    })
            except Exception as e:
                if len(problems) < 20:
                    problems.append({"row": int(i)+2, "issue": f"Employee update error: {e}", "value": f"{name}"})

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
                training_venue=venue or None,
                region=training_region or None
            )
            inserted += 1
        except Exception as e:
            other_errors += 1
            if len(problems) < 20:
                problems.append({"row": int(i)+2, "issue": f"DB insert error: {e}", "value": f"{name} {tdate}"})
            continue

    st.success(f"Training upload done. Inserted: {inserted}, Invalid date: {invalid_date}, Not found: {not_found}, Ambiguous: {ambiguous}, Auto tie-broken: {tie_broken}, Employee records overridden: {overridden}, Other errors: {other_errors}.")

    if override_details:
        st.info(f"ℹ️ {overridden} employee record(s) were updated with store/department from training data:")
        with st.expander(f"View {len(override_details)} override details", expanded=False):
            st.dataframe(pd.DataFrame(override_details), use_container_width=True)
            st.download_button(
                "Download Override Details (Excel)",
                data=to_excel_bytes({"Overridden Records": pd.DataFrame(override_details)}),
                file_name="employee_overrides.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    if problems:
        st.caption("First few issues (row numbers are Excel rows, incl. header):")
        st.dataframe(pd.DataFrame(problems), use_container_width=True)

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

st.subheader("View & Search Employees")
search_emp = st.text_input("Search employees by Name, Email, Store, or Department")
if search_emp:
    search_term = search_emp.lower()
    emps = conn.execute("""
        SELECT id, employee_code, name, email, department, store, position, region, start_date, end_date
        FROM employees
        WHERE deleted_at IS NULL
          AND (lower(name) LIKE ? OR lower(email) LIKE ? OR lower(store) LIKE ? OR lower(department) LIKE ?)
        ORDER BY name COLLATE NOCASE
        LIMIT 100
    """, (f"%{search_term}%", f"%{search_term}%", f"%{search_term}%", f"%{search_term}%")).fetchall()
else:
    emps = conn.execute("""
        SELECT id, employee_code, name, email, department, store, position, region, start_date, end_date
        FROM employees
        WHERE deleted_at IS NULL
        ORDER BY name COLLATE NOCASE
        LIMIT 200
    """).fetchall()

df_emps = pd.DataFrame(emps, columns=["ID","Employee Code","Name","Email","Department","Store","Position","Region","Start Date","End Date"])
st.dataframe(df_emps, use_container_width=True, height=350)
st.caption(f"Showing {len(df_emps)} employee(s)")

st.subheader("Edit Employee")
emp_id_edit = st.number_input("Employee ID to Edit", min_value=0, step=1, key="emp_edit_id")
if emp_id_edit > 0:
    emp_data = conn.execute("""
        SELECT id, employee_code, name, email, department, store, position, region
        FROM employees WHERE id=? AND deleted_at IS NULL
    """, (emp_id_edit,)).fetchone()

    if emp_data:
        with st.form("edit_employee_form"):
            st.write(f"**Editing Employee ID: {emp_data[0]}**")
            new_code = st.text_input("Employee Code", value=emp_data[1] or "")
            new_name = st.text_input("Name", value=emp_data[2] or "")
            new_email = st.text_input("Email", value=emp_data[3] or "")
            new_dept = st.text_input("Department", value=emp_data[4] or "")
            new_store = st.text_input("Store", value=emp_data[5] or "")
            new_pos = st.text_input("Position", value=emp_data[6] or "")
            new_region = st.text_input("Region", value=emp_data[7] or "")

            update_trainings_too = st.checkbox("Also update all training records for this employee", value=True)

            submitted = st.form_submit_button("Save Changes", use_container_width=True, type="primary")

            if submitted:
                # Normalize case before saving
                new_name = normalize_name(new_name)
                new_email = new_email.lower()
                new_dept = normalize_field(new_dept)
                new_store = normalize_field(new_store)
                new_pos = normalize_field(new_pos)
                new_region = normalize_field(new_region)

                # Update employee
                conn.execute("""
                    UPDATE employees
                    SET employee_code=?, name=?, email=?, department=?, store=?, position=?, region=?, updated_at=?
                    WHERE id=?
                """, (new_code, new_name, new_email, new_dept, new_store, new_pos, new_region, now(), emp_id_edit))
                conn.commit()

                # Update trainings if requested (bidirectional sync)
                if update_trainings_too:
                    conn.execute("""
                        UPDATE trainings
                        SET region=?, created_at=created_at
                        WHERE employee_id=?
                    """, (new_region, emp_id_edit))
                    conn.commit()

                st.success(f"✅ Employee {emp_id_edit} updated successfully!")
                if update_trainings_too:
                    st.info(f"✅ All training records for this employee also updated with new region.")
                st.rerun()
    else:
        st.warning("Employee not found or has been deleted.")

st.subheader("Search & Edit Trainings")
search_train = st.text_input("Search trainings by Employee Name, Title, Venue, or Store")
if search_train:
    search_term_t = search_train.lower()
    trains = conn.execute("""
        SELECT t.id, e.name, e.department, e.store, t.training_title, t.training_venue, t.training_date, t.next_due_date
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE t.deleted_at IS NULL AND e.deleted_at IS NULL
          AND (lower(e.name) LIKE ? OR lower(t.training_title) LIKE ? OR lower(t.training_venue) LIKE ? OR lower(e.store) LIKE ?)
        ORDER BY date(t.training_date) DESC
        LIMIT 100
    """, (f"%{search_term_t}%", f"%{search_term_t}%", f"%{search_term_t}%", f"%{search_term_t}%")).fetchall()
else:
    trains = conn.execute("""
        SELECT t.id, e.name, e.department, e.store, t.training_title, t.training_venue, t.training_date, t.next_due_date
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE t.deleted_at IS NULL AND e.deleted_at IS NULL
        ORDER BY date(t.training_date) DESC
        LIMIT 200
    """).fetchall()

df_trains = pd.DataFrame(trains, columns=["ID","Employee Name","Department","Store","Title","Venue","Training Date","Next Due"])
st.dataframe(df_trains, use_container_width=True, height=350)
st.caption(f"Showing {len(df_trains)} training(s)")

st.subheader("Edit Training")
train_id_edit = st.number_input("Training ID to Edit", min_value=0, step=1, key="train_edit_id")
if train_id_edit > 0:
    train_data = conn.execute("""
        SELECT t.id, t.employee_id, e.name, t.training_title, t.training_venue, t.training_date, t.next_due_date, t.region
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE t.id=? AND t.deleted_at IS NULL
    """, (train_id_edit,)).fetchone()

    if train_data:
        with st.form("edit_training_form"):
            st.write(f"**Editing Training ID: {train_data[0]}** (Employee: {train_data[2]})")
            new_title = st.text_input("Training Title", value=train_data[3] or "")
            new_venue = st.text_input("Training Venue", value=train_data[4] or "")
            new_date = st.date_input("Training Date", value=pd.to_datetime(train_data[5]).date() if train_data[5] else None)
            new_due = st.date_input("Next Due Date", value=pd.to_datetime(train_data[6]).date() if train_data[6] else None)
            new_train_region = st.text_input("Region", value=train_data[7] or "")

            st.info("💡 Tip: Training title will be auto-normalized to standard format")

            submitted_train = st.form_submit_button("Save Changes", use_container_width=True, type="primary")

            if submitted_train:
                # Normalize all fields
                normalized_title = normalize_training_title(new_title)
                normalized_venue = normalize_field(new_venue)
                normalized_region = normalize_field(new_train_region)

                # Update training
                conn.execute("""
                    UPDATE trainings
                    SET training_title=?, training_venue=?, training_date=?, next_due_date=?, region=?
                    WHERE id=?
                """, (normalized_title, normalized_venue, new_date.isoformat(), new_due.isoformat(), normalized_region, train_id_edit))
                conn.commit()

                st.success(f"✅ Training {train_id_edit} updated successfully!")
                if normalized_title != new_title:
                    st.info(f"📝 Title normalized: '{new_title}' → '{normalized_title}'")
                st.rerun()
    else:
        st.warning("Training not found or has been deleted.")

st.subheader("🔍 Find & Replace - Bulk Data Cleanup")
st.caption("Search for values in any column and replace ALL occurrences at once")

entity_type = st.radio("Select Data Type", ["Employees", "Trainings"], horizontal=True, key="find_replace_entity")

if entity_type == "Employees":
    column_options = ["name", "email", "employee_code", "department", "store", "position", "region"]
    search_column = st.selectbox("Select Column to Search", column_options, key="emp_search_col")

    search_value = st.text_input("Search for value (will search ALL records)", key="emp_search_val")

    if search_value:
        # Search ALL matching records (no limit)
        matches = conn.execute(f"""
            SELECT id, employee_code, name, email, department, store, position, region
            FROM employees
            WHERE deleted_at IS NULL AND lower({search_column}) LIKE ?
            ORDER BY {search_column} COLLATE NOCASE
        """, (f"%{search_value.lower()}%",)).fetchall()

        if matches:
            st.success(f"✅ Found {len(matches)} matching employee(s)")
            df_matches = pd.DataFrame(matches, columns=["ID","Employee Code","Name","Email","Department","Store","Position","Region"])
            st.dataframe(df_matches, use_container_width=True, height=350)

            st.warning(f"⚠️ You are about to modify {len(matches)} employee record(s)")

            replace_value = st.text_input(f"Replace '{search_column}' with:", key="emp_replace_val")

            if replace_value:
                # Normalize the replacement value
                if search_column == "name":
                    replace_value_normalized = normalize_name(replace_value)
                elif search_column == "email":
                    replace_value_normalized = replace_value.lower()
                else:
                    replace_value_normalized = normalize_field(replace_value)

                st.info(f"💡 Will replace with: '{replace_value_normalized}'")

                if st.button(f"🔄 Replace in ALL {len(matches)} record(s)", type="primary", use_container_width=True):
                    # Apply bulk update
                    conn.execute(f"""
                        UPDATE employees
                        SET {search_column}=?, updated_at=?
                        WHERE id IN ({','.join(['?']*len(matches))})
                    """, (replace_value_normalized, now(), *[m[0] for m in matches]))
                    conn.commit()

                    st.success(f"✅ Successfully updated {len(matches)} employee(s)!")
                    st.balloons()
                    st.rerun()
        else:
            st.info(f"No employees found with '{search_value}' in {search_column}")

else:  # Trainings
    column_options = ["training_title", "training_venue", "region"]
    search_column = st.selectbox("Select Column to Search", column_options, key="train_search_col")

    search_value = st.text_input("Search for value (will search ALL records)", key="train_search_val")

    if search_value:
        # Search ALL matching records (no limit)
        matches = conn.execute(f"""
            SELECT t.id, e.name, e.department, e.store, t.training_title, t.training_venue, t.region, t.training_date
            FROM trainings t
            JOIN employees e ON e.id = t.employee_id
            WHERE t.deleted_at IS NULL AND lower(t.{search_column}) LIKE ?
            ORDER BY t.{search_column} COLLATE NOCASE
        """, (f"%{search_value.lower()}%",)).fetchall()

        if matches:
            st.success(f"✅ Found {len(matches)} matching training(s)")
            df_matches = pd.DataFrame(matches, columns=["ID","Employee","Department","Store","Title","Venue","Region","Date"])
            st.dataframe(df_matches, use_container_width=True, height=350)

            st.warning(f"⚠️ You are about to modify {len(matches)} training record(s)")

            replace_value = st.text_input(f"Replace '{search_column}' with:", key="train_replace_val")

            if replace_value:
                # Normalize the replacement value
                if search_column == "training_title":
                    replace_value_normalized = normalize_training_title(replace_value)
                else:
                    replace_value_normalized = normalize_field(replace_value)

                st.info(f"💡 Will replace with: '{replace_value_normalized}'")

                if st.button(f"🔄 Replace in ALL {len(matches)} record(s)", type="primary", use_container_width=True):
                    # Apply bulk update
                    conn.execute(f"""
                        UPDATE trainings
                        SET {search_column}=?
                        WHERE id IN ({','.join(['?']*len(matches))})
                    """, (replace_value_normalized, *[m[0] for m in matches]))
                    conn.commit()

                    st.success(f"✅ Successfully updated {len(matches)} training(s)!")
                    st.balloons()
                    st.rerun()
        else:
            st.info(f"No trainings found with '{search_value}' in {search_column}")

st.subheader("🔎 Duplicate Detection & Cleanup")
st.caption("Find and merge duplicate employee records with same Name + Department + Store")

if st.button("🔍 Scan for Duplicates", use_container_width=True):
    duplicates = find_duplicate_employees(conn)

    if duplicates:
        st.warning(f"⚠️ Found {len(duplicates)} duplicate group(s) ({sum(len(g) for g in duplicates)} total records)")

        for idx, dup_group in enumerate(duplicates):
            with st.expander(f"Duplicate Group {idx+1}: {dup_group[0][1]} - {dup_group[0][2]} - {dup_group[0][3]}", expanded=True):
                st.write(f"**{len(dup_group)} records with same Name + Department + Store:**")

                # Show all duplicates in group
                dup_df = pd.DataFrame([
                    {"ID": d[0], "Name": d[1], "Department": d[2], "Store": d[3], "Email": d[4] or "(none)"}
                    for d in dup_group
                ])
                st.dataframe(dup_df, use_container_width=True)

                # Get training counts for each
                training_counts = []
                for d in dup_group:
                    count = conn.execute("""
                        SELECT COUNT(*) FROM trainings
                        WHERE employee_id=? AND deleted_at IS NULL
                    """, (d[0],)).fetchone()[0]
                    training_counts.append(count)

                st.write("**Training counts per record:**")
                for i, (d, count) in enumerate(zip(dup_group, training_counts)):
                    st.write(f"- ID {d[0]}: {count} training(s)")

                # Suggest keeping the one with most trainings
                max_trainings_idx = training_counts.index(max(training_counts))
                suggested_keep_id = dup_group[max_trainings_idx][0]

                st.info(f"💡 Suggested: Keep ID **{suggested_keep_id}** (has {max(training_counts)} training(s))")

                # Let user choose which to keep
                keep_id = st.selectbox(
                    "Select record to KEEP:",
                    options=[d[0] for d in dup_group],
                    index=max_trainings_idx,
                    key=f"keep_dup_{idx}"
                )

                merge_ids = [d[0] for d in dup_group if d[0] != keep_id]

                if st.button(f"🔗 Merge: Keep ID {keep_id}, Delete {len(merge_ids)} duplicate(s)",
                           type="primary", key=f"merge_btn_{idx}"):
                    merge_duplicate_employees(conn, keep_id, merge_ids)
                    st.success(f"✅ Merged {len(merge_ids)} duplicate(s) into ID {keep_id}")
                    st.balloons()
                    st.rerun()
    else:
        st.success("✅ No duplicates found! All employee records are unique.")

st.subheader("Employee Counts Summary")
total_emps = count_employees(conn, active_only=False)
active_emps = count_employees(conn, active_only=True)
leavers = total_emps - active_emps

col_c1, col_c2, col_c3 = st.columns(3)
with col_c1:
    st.metric("Total Employees", total_emps, help="Master + Hires + Leavers")
with col_c2:
    st.metric("Active Employees", active_emps, help="Master + Hires - Leavers")
with col_c3:
    st.metric("Leavers", leavers)

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

