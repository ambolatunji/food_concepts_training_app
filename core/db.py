import sqlite3
from pathlib import Path
from typing import Iterable, List, Dict
from datetime import datetime
import json

DB_PATH = Path("data") / "training.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# ---- schema helpers ----
def has_column(conn, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(r[1].lower() == col.lower() for r in cur.fetchall())

def ensure_column(conn, table: str, col: str, col_type: str):
    if not has_column(conn, table, col):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type};")
        conn.commit()

def migrate():
    conn = get_conn()
    cur = conn.cursor()
    ensure_column(conn, "employees", "end_date", "TEXT")

    # employees
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unique_key TEXT UNIQUE, -- normalized: name|email|store|dept
        employee_code TEXT,
        name TEXT NOT NULL,
        email TEXT,
        department TEXT,
        store TEXT,
        position TEXT,
        region TEXT,
        start_date TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """)

    # Employment events (join/leave audit)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employment_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,            -- 'hire' | 'leave'
        event_date TEXT NOT NULL,
        source TEXT,                         -- filename or 'admin'
        created_at TEXT NOT NULL,
        FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
    );
    """)
    
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_emp_events_unique
    ON employment_events(employee_id, event_type, event_date)
    """)


    # trainings
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trainings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL,
        training_date TEXT NOT NULL,
        next_due_date TEXT NOT NULL,
        evidence_path TEXT,
        evidence_mime TEXT,
        evidence_size INTEGER,
        created_at TEXT NOT NULL,
        FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
    );
    """)

    # add new cols safely
    ensure_column(conn, "trainings", "training_title", "TEXT")
    ensure_column(conn, "trainings", "training_venue", "TEXT")
    ensure_column(conn, "trainings", "region", "TEXT")

    # (optional) lookups table – safe to keep even if unused now
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lookups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,    -- region / position / store / department / training_title / training_venue
        value TEXT NOT NULL,
        canonical_value TEXT NOT NULL
    );
    """)

    # admin users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'admin',
        created_at TEXT NOT NULL
    );
    """)

    # soft-delete columns
    ensure_column(conn, "employees", "deleted_at", "TEXT")
    ensure_column(conn, "trainings", "deleted_at", "TEXT")

    # change requests (pending user corrections)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS change_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL,
        payload_json TEXT NOT NULL,   -- {"name": "...", "store": "...", ...}
        status TEXT NOT NULL DEFAULT 'pending',  -- pending | approved | rejected
        created_at TEXT NOT NULL,
        decided_at TEXT,
        decided_by TEXT,
        FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
    );
    """)

    conn.commit()
    conn.close()


def now():
    return datetime.now().isoformat(timespec="seconds")

# ---- upserts / inserts / queries ----
def upsert_employee(conn, unique_key, row: Dict):
    #data = {k: v for k, v in data.items() if v is not None}
    cur = conn.cursor()
    ts = now()
    cur.execute("""
        INSERT INTO employees (unique_key, employee_code, name, email, department, store, position, region, start_date, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(unique_key) DO UPDATE SET
          employee_code=CASE WHEN excluded.employee_code IS NOT NULL AND excluded.employee_code!='' THEN excluded.employee_code ELSE employees.employee_code END,
          name=CASE WHEN excluded.name IS NOT NULL AND excluded.name!='' THEN excluded.name ELSE employees.name END,
          email=CASE WHEN excluded.email IS NOT NULL AND excluded.email!='' THEN excluded.email ELSE employees.email END,
          department=CASE WHEN excluded.department IS NOT NULL AND excluded.department!='' THEN excluded.department ELSE employees.department END,
          store=CASE WHEN excluded.store IS NOT NULL AND excluded.store!='' THEN excluded.store ELSE employees.store END,
          position=CASE WHEN excluded.position IS NOT NULL AND excluded.position!='' THEN excluded.position ELSE employees.position END,
          region=CASE WHEN excluded.region IS NOT NULL AND excluded.region!='' THEN excluded.region ELSE employees.region END,
          start_date=CASE WHEN excluded.start_date IS NOT NULL AND excluded.start_date!='' THEN excluded.start_date ELSE employees.start_date END,
          updated_at=excluded.updated_at
    """, (
        unique_key,
        row.get("employee_code"),
        row.get("name"),
        row.get("email"),
        row.get("department"),
        row.get("store"),
        row.get("position"),
        row.get("region"),
        row.get("start_date") or "",
        ts, ts
    ))
    conn.commit()

def record_employment_event(conn, employee_id:int, event_type:str, event_date:str, source:str=None):
    # avoid duplicates if this event already exists
    exists = conn.execute("""
        SELECT 1 FROM employment_events
        WHERE employee_id=? AND event_type=? AND event_date=?
        LIMIT 1
    """, (employee_id, event_type, event_date)).fetchone()
    if exists:
        return
    conn.execute("""
        INSERT INTO employment_events (employee_id, event_type, event_date, source, created_at)
        VALUES (?,?,?,?,?)
    """, (employee_id, event_type, event_date, source or "", now()))
    conn.commit()


def set_employee_end_date(conn, employee_id:int, end_date:str):
    conn.execute("UPDATE employees SET end_date=?, updated_at=? WHERE id=?",
                 (end_date, now(), employee_id))
    conn.commit()


def find_employee_by_fields(conn, name:str, department:str, store:str, email:str=None):
    cur = conn.cursor()
    base = """SELECT id,name,email,department,store,position,region
              FROM employees
              WHERE deleted_at IS NULL AND lower(name)=? AND lower(department)=? AND lower(store)=?"""
    if email:
        cur.execute(base + " AND lower(email)=?", (name.lower().strip(), department.lower().strip(), store.lower().strip(), email.lower().strip()))
    else:
        cur.execute(base, (name.lower().strip(), department.lower().strip(), store.lower().strip()))
    return cur.fetchone()

def insert_training(conn, employee_id:int, training_date:str, next_due_date:str,
                    evidence_path:str=None, evidence_mime:str=None, evidence_size:int=None,
                    training_title:str=None, training_venue:str=None, region:str=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trainings (employee_id, training_date, next_due_date, evidence_path, evidence_mime, evidence_size, training_title, training_venue, region, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (employee_id, training_date, next_due_date, evidence_path, evidence_mime, evidence_size, training_title or "", training_venue or "", region or "", now()))
    conn.commit()

def list_trainings(conn, filters:Dict=None):
    filters = filters or {}
    sql = """SELECT t.id, e.name, e.department, e.store, e.position,
                    COALESCE(t.region, e.region) as region,
                    t.training_date, t.next_due_date, t.evidence_path,
                    t.training_title, t.training_venue
             FROM trainings t
             JOIN employees e ON e.id = t.employee_id
             WHERE t.deleted_at IS NULL AND e.deleted_at IS NULL"""
    params=[]
    # employee-dimension filters (check both training and employee region)
    for k in ("department","store","position"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())
    if filters.get("region"):
        sql += " AND (lower(COALESCE(t.region, e.region))=?)"
        params.append(filters["region"].lower())
    # date range
    if filters.get("date_from"):
        sql += " AND date(t.training_date)>=date(?)"
        params.append(filters["date_from"])
    if filters.get("date_to"):
        sql += " AND date(t.training_date)<=date(?)"
        params.append(filters["date_to"])
    # NEW: training title / venue filters
    if filters.get("training_title"):
        sql += " AND lower(t.training_title)=?"
        params.append(filters["training_title"].lower())
    if filters.get("training_venue"):
        sql += " AND lower(t.training_venue)=?"
        params.append(filters["training_venue"].lower())

    sql += " ORDER BY date(t.training_date) DESC"
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()

def trainings_summary(conn, date_from: str = None, date_to: str = None, by: str = "department"):
    # decide which column to group on
    if by in ("department", "store", "region", "position"):
        group_expr = f"e.{by}"
    elif by in ("training_title", "training_venue"):
        group_expr = f"t.{by}"
    else:
        group_expr = "e.department"

    sql = f"""
        SELECT {group_expr} AS grp, COUNT(t.id) AS trainings
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE 1=1
    """
    params = []
    if date_from:
        sql += " AND date(t.training_date) >= date(?)"
        params.append(date_from)
    if date_to:
        sql += " AND date(t.training_date) <= date(?)"
        params.append(date_to)

    sql += " GROUP BY grp ORDER BY trainings DESC"

    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()

# ---- distinct helpers for building dropdowns from real data ----
def _safe_col(col: str, allowed: tuple) -> str:
    if col not in allowed:
        raise ValueError(f"Invalid column: {col}")
    return col

def distinct_employee_values(conn, col: str):
    col = _safe_col(col, ("name","email","department","store","position","region"))
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DISTINCT {col}
        FROM employees
        WHERE deleted_at IS NULL AND {col} IS NOT NULL AND TRIM({col})!=''
        ORDER BY {col} COLLATE NOCASE
    """)
    return [r[0] for r in cur.fetchall()]

def distinct_training_values(conn, col: str):
    col = _safe_col(col, ("training_title","training_venue"))
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DISTINCT {col}
        FROM trainings
        WHERE {col} IS NOT NULL AND TRIM({col})!=''
        ORDER BY {col} COLLATE NOCASE
    """)
    return [r[0] for r in cur.fetchall()]

def employees_by_name(conn, name: str):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, email, department, store, position, region
        FROM employees
        WHERE deleted_at IS NULL AND lower(name)=?
        ORDER BY store COLLATE NOCASE, department COLLATE NOCASE
    """, (name.lower().strip(),))
    return cur.fetchall()

def get_employee_training_history(conn, employee_id: int):
    """
    Get all training history for an employee with eligibility status.
    Returns list of tuples: (training_title, training_date, next_due_date, days_until_due, training_venue, status)
    """
    from datetime import date
    today = date.today().isoformat()

    cur = conn.cursor()
    cur.execute("""
        SELECT
            training_title,
            training_date,
            next_due_date,
            CAST(julianday(next_due_date) - julianday(?) AS INTEGER) as days_until_due,
            training_venue,
            CASE
                WHEN julianday(next_due_date) - julianday(?) > 30 THEN 'Not Due'
                WHEN julianday(next_due_date) - julianday(?) BETWEEN 1 AND 30 THEN 'Due Soon (≤30 days)'
                WHEN julianday(next_due_date) - julianday(?) < 0 THEN 'Overdue'
                ELSE 'Due Today'
            END as status
        FROM trainings
        WHERE employee_id = ? AND deleted_at IS NULL
        ORDER BY training_date DESC
    """, (today, today, today, today, employee_id))
    return cur.fetchall()

def find_duplicate_employees(conn):
    """
    Find potential duplicate employees based on:
    - Same Name (normalized)
    - Same Department
    - Same Store

    Returns list of duplicate groups [(id1, id2, ...), ...]
    """
    from core.logic import normalize_name, normalize_field

    # Get all employees
    employees = conn.execute("""
        SELECT id, name, department, store, email
        FROM employees
        WHERE deleted_at IS NULL
        ORDER BY id
    """).fetchall()

    # Group by normalized name+dept+store
    groups = {}
    for emp_id, name, dept, store, email in employees:
        # Create key from normalized values
        key = f"{normalize_name(name).lower()}|{normalize_field(dept).lower()}|{normalize_field(store).lower()}"
        if key not in groups:
            groups[key] = []
        groups[key].append((emp_id, name, dept, store, email))

    # Find groups with more than one employee (duplicates)
    duplicates = []
    for key, group in groups.items():
        if len(group) > 1:
            duplicates.append(group)

    return duplicates


def merge_duplicate_employees(conn, keep_id:int, merge_ids:list):
    """
    Merge duplicate employee records.
    - Keeps the record with keep_id
    - Transfers all trainings from merge_ids to keep_id
    - Soft-deletes the duplicate records
    """
    for dup_id in merge_ids:
        if dup_id == keep_id:
            continue

        # Transfer trainings to the kept employee
        conn.execute("""
            UPDATE trainings
            SET employee_id = ?
            WHERE employee_id = ? AND deleted_at IS NULL
        """, (keep_id, dup_id))

        # Transfer employment events
        conn.execute("""
            UPDATE employment_events
            SET employee_id = ?
            WHERE employee_id = ?
        """, (keep_id, dup_id))

        # Soft delete the duplicate
        conn.execute("UPDATE employees SET deleted_at=? WHERE id=?", (now(), dup_id))

    conn.commit()


def count_employees(conn, active_only:bool=False)->int:
    """
    Count employees in database.

    Args:
        active_only: If True, excludes employees with end_date (leavers)
                    If False, counts all employees (master + hires + leavers)

    Returns:
        Total = Employee Master + Hires + Leavers (if active_only=False)
        Active = Employee Master + Hires - Leavers (if active_only=True)
    """
    if active_only:
        return conn.execute("""
            SELECT COUNT(*) FROM employees
            WHERE deleted_at IS NULL
              AND (end_date IS NULL OR date(end_date) > date('now'))
        """).fetchone()[0]
    else:
        return conn.execute("SELECT COUNT(*) FROM employees WHERE deleted_at IS NULL").fetchone()[0]


def count_trained_employees(conn, filters:dict=None, active_only:bool=False)->int:
    """
    Count unique employees who have received training.

    Args:
        filters: Optional filters (department, store, date_from, date_to, training_title, etc.)
        active_only: If True, only count active employees (not leavers)
    """
    filters = filters or {}
    sql = """
        SELECT COUNT(DISTINCT t.employee_id)
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE t.deleted_at IS NULL
          AND e.deleted_at IS NULL
    """
    params = []

    if active_only:
        sql += " AND (e.end_date IS NULL OR date(e.end_date) > date('now'))"

    for k in ("department", "store", "position", "region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())

    if filters.get("date_from"):
        sql += " AND date(t.training_date)>=date(?)"
        params.append(filters["date_from"])
    if filters.get("date_to"):
        sql += " AND date(t.training_date)<=date(?)"
        params.append(filters["date_to"])
    if filters.get("training_title"):
        sql += " AND lower(t.training_title)=?"
        params.append(filters["training_title"].lower())

    return conn.execute(sql, params).fetchone()[0]

def soft_delete_training(conn, training_id:int):
    conn.execute("UPDATE trainings SET deleted_at=? WHERE id=?", (now(), training_id))
    conn.commit()

def restore_training(conn, training_id:int):
    conn.execute("UPDATE trainings SET deleted_at=NULL WHERE id=?", (training_id,))
    conn.commit()

def hard_delete_training(conn, training_id:int):
    conn.execute("DELETE FROM trainings WHERE id=?", (training_id,))
    conn.commit()

def list_deleted_trainings(conn):
    return conn.execute("""
        SELECT t.id, e.name, e.department, e.store, t.training_date, t.training_title, t.training_venue, t.deleted_at
        FROM trainings t JOIN employees e ON e.id=t.employee_id
        WHERE t.deleted_at IS NOT NULL
        ORDER BY t.deleted_at DESC
    """).fetchall()

def soft_delete_employee(conn, employee_id:int):
    conn.execute("UPDATE employees SET deleted_at=? WHERE id=?", (now(), employee_id))
    conn.commit()

def restore_employee(conn, employee_id:int):
    conn.execute("UPDATE employees SET deleted_at=NULL WHERE id=?", (employee_id,))
    conn.commit()

def hard_delete_employee(conn, employee_id:int):
    # this will also DELETE trainings via ON DELETE CASCADE if you truly delete the row
    conn.execute("DELETE FROM employees WHERE id=?", (employee_id,))
    conn.commit()

def list_deleted_employees(conn):
    return conn.execute("""
        SELECT id, name, email, department, store, position, region, deleted_at
        FROM employees
        WHERE deleted_at IS NOT NULL
        ORDER BY deleted_at DESC
    """).fetchall()
def soft_delete_all_trainings(conn):
    conn.execute("UPDATE trainings SET deleted_at=? WHERE deleted_at IS NULL", (now(),))
    conn.commit()

def restore_all_trainings(conn):
    conn.execute("UPDATE trainings SET deleted_at=NULL WHERE deleted_at IS NOT NULL")
    conn.commit()

def hard_delete_all_trainings(conn):
    conn.execute("DELETE FROM trainings")
    conn.commit()

def soft_delete_all_employees(conn):
    conn.execute("UPDATE employees SET deleted_at=? WHERE deleted_at IS NULL", (now(),))
    conn.commit()

def restore_all_employees(conn):
    conn.execute("UPDATE employees SET deleted_at=NULL WHERE deleted_at IS NOT NULL")
    conn.commit()

def hard_delete_all_employees(conn):
    # WARNING: this will cascade delete trainings for those employees
    conn.execute("DELETE FROM employees")
    conn.commit()

def create_change_request(conn, employee_id:int, payload:dict):
    conn.execute("""
        INSERT INTO change_requests (employee_id, payload_json, status, created_at)
        VALUES (?,?, 'pending', ?)
    """, (employee_id, json.dumps(payload, ensure_ascii=False), now()))
    conn.commit()

def list_change_requests(conn, status:str="pending"):
    return conn.execute("""
        SELECT id, employee_id, payload_json, status, created_at, decided_at, decided_by
        FROM change_requests
        WHERE status=?
        ORDER BY created_at DESC
    """, (status,)).fetchall()

def approve_change_request(conn, request_id:int, decided_by:str):
    # fetch request
    r = conn.execute("SELECT employee_id, payload_json FROM change_requests WHERE id=?", (request_id,)).fetchone()
    if not r: return False, "Request not found"
    employee_id, payload_json = r
    payload = json.loads(payload_json)

    # allowlist of editable fields
    allowed = {"name","email","department","store","position","region","employee_code","start_date"}
    updates = {k:v for k,v in payload.items() if k in allowed}

    # build dynamic UPDATE
    if updates:
        sets = ", ".join(f"{k}=?" for k in updates.keys())
        params = list(updates.values()) + [now(), employee_id]
        conn.execute(f"UPDATE employees SET {sets}, updated_at=? WHERE id=?", params)

    conn.execute("""
        UPDATE change_requests SET status='approved', decided_at=?, decided_by=? WHERE id=?
    """, (now(), decided_by, request_id))
    conn.commit()
    return True, "Approved"

def reject_change_request(conn, request_id:int, decided_by:str):
    conn.execute("""
        UPDATE change_requests SET status='rejected', decided_at=?, decided_by=? WHERE id=?
    """, (now(), decided_by, request_id))
    conn.commit()

# ---- optional (kept for backwards compatibility) ----
def get_lookup_values(conn, kind:str)->List[str]:
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT canonical_value FROM lookups WHERE type=? ORDER BY canonical_value ASC", (kind,))
    return [r[0] for r in cur.fetchall()]

def seed_lookup(conn, kind:str, values:Iterable[str]):
    cur = conn.cursor()
    for v in values:
        cur.execute("""
            INSERT INTO lookups (type, value, canonical_value)
            VALUES (?,?,?)
        """, (kind, v, v))
    conn.commit()

def count_employees_matching(conn, filters:Dict=None)->int:
    filters = filters or {}
    sql = "SELECT COUNT(DISTINCT e.id) FROM employees e WHERE e.deleted_at IS NULL"
    params=[]
    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())
    return conn.execute(sql, params).fetchone()[0]

def process_hires_df(conn, df, source_name="upload"):
    import pandas as pd
    from core.logic import normalize_str, normalize_name, normalize_field, to_date_str, unique_key
    # normalize headers
    m = {c: c.strip().lower() for c in df.columns}; df.columns = [m[c] for c in df.columns]
    def pick(*opts):
        for o in opts:
            if o in df.columns: return o
        return None
    c_code = pick("employee code","code")
    c_name = pick("employee name","name")
    c_email= pick("email","work email")
    c_dept = pick("department","dept")
    c_reg  = pick("region")
    c_store= pick("store","location")
    c_pos  = pick("position","job title")
    c_start= pick("startdate","start date","start_date","start date (yyyy-mm-dd)")

    if not all([c_name, c_dept, c_store, c_start]):
        return 0, "Missing required columns for Hires"

    inserted=0
    for _, r in df.iterrows():
        # Normalize all fields with proper casing
        name  = normalize_name(r[c_name])
        email = normalize_str(r[c_email]).lower() if (c_email and pd.notna(r[c_email])) else ""
        dept  = normalize_field(r[c_dept])
        store = normalize_field(r[c_store])
        region= normalize_field(r[c_reg]) if (c_reg and pd.notna(r[c_reg])) else ""
        pos   = normalize_field(r[c_pos]) if (c_pos and pd.notna(r[c_pos])) else ""
        code  = normalize_str(r[c_code]) if (c_code and pd.notna(r[c_code])) else ""
        start = to_date_str(r[c_start])
        if not (name and dept and store and start):
            continue

        # Create unique key for upsert (handles duplicates)
        uk = unique_key(name, email, store, dept)
        upsert_employee(conn, uk, {
            "employee_code": code, "name": name, "email": email,
            "department": dept, "store": store, "position": pos, "region": region,
            "start_date": start
        })

        # find id and record hire event
        emp = conn.execute("SELECT id FROM employees WHERE unique_key=?", (uk,)).fetchone()
        if emp:
            record_employment_event(conn, emp[0], "hire", start, source_name)
            inserted += 1
    return inserted, "OK"

def process_leavers_df(conn, df, source_name="upload"):
    import pandas as pd
    from core.logic import normalize_str, normalize_name, normalize_field, to_date_str, unique_key
    m = {c: c.strip().lower() for c in df.columns}; df.columns = [m[c] for c in df.columns]
    def pick(*opts):
        for o in opts:
            if o in df.columns: return o
        return None
    c_code = pick("employee code","code")
    c_name = pick("employee name","name")
    c_email= pick("email","work email")
    c_dept = pick("department","dept")
    c_reg  = pick("region")
    c_store= pick("store","location")
    c_start= pick("startdate","start date","start_date","start date (yyyy-mm-dd)", "start date (optional)")
    c_end  = pick("end date","leaving date","resign date","exit date","end date (yyyy-mm-dd)" )
    c_pos = pick("position","job title","position ")
    if not all([c_name, c_dept, c_store, c_end]):
        return 0, "Missing required columns for Leavers"

    updated=0
    for _, r in df.iterrows():
        # Normalize all fields with proper casing
        name  = normalize_name(r[c_name])
        email = normalize_str(r[c_email]).lower() if (c_email and pd.notna(r[c_email])) else ""
        dept  = normalize_field(r[c_dept])
        store = normalize_field(r[c_store])
        region= normalize_field(r[c_reg]) if (c_reg and pd.notna(r[c_reg])) else ""
        code  = normalize_str(r[c_code]) if (c_code and pd.notna(r[c_code])) else ""
        end   = to_date_str(r[c_end])
        start = to_date_str(r[c_start]) if (c_start and pd.notna(r[c_start])) else None
        pos   = normalize_field(r[c_pos]) if (c_pos and pd.notna(r[c_pos])) else None
        if not (name and dept and store and end):
            continue

        # Create unique key (will match existing employee regardless of case)
        uk = unique_key(name, email, store, dept)

        # upsert first (in case not in DB yet, or to update fields)
        payload = {
            "employee_code": code or None,
            "name": name,
            "email": email or None,
            "department": dept or None,
            "store": store or None,
            "region": region or None,
            "start_date": start or None
        }
        if pos:
            payload["position"] = pos

        upsert_employee(conn, uk, payload)
        emp = conn.execute("SELECT id FROM employees WHERE unique_key=?", (uk,)).fetchone()
        if emp:
            set_employee_end_date(conn, emp[0], end)
            record_employment_event(conn, emp[0], "leave", end, source_name)
            updated += 1
    return updated, "OK"

def count_joins(conn, date_from:str=None, date_to:str=None, filters:dict=None)->int:
    filters = filters or {}
    sql = "SELECT COUNT(*) FROM employees WHERE deleted_at IS NULL AND start_date IS NOT NULL"
    args=[]
    if date_from:
        sql += " AND date(start_date)>=date(?)"; args.append(date_from)
    if date_to:
        sql += " AND date(start_date)<=date(?)"; args.append(date_to)
    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower({k})=?"; args.append(filters[k].lower())
    return get_conn().execute(sql, args).fetchone()[0]

def count_leaves(conn, date_from:str=None, date_to:str=None, filters:dict=None)->int:
    filters = filters or {}
    sql = "SELECT COUNT(*) FROM employees WHERE deleted_at IS NULL AND end_date IS NOT NULL"
    args=[]
    if date_from:
        sql += " AND date(end_date)>=date(?)"; args.append(date_from)
    if date_to:
        sql += " AND date(end_date)<=date(?)"; args.append(date_to)
    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower({k})=?"; args.append(filters[k].lower())
    return get_conn().execute(sql, args).fetchone()[0]

def headcount_as_of(conn, date_cut:str=None, filters:dict=None)->int:
    filters = filters or {}
    sql = """SELECT COUNT(*) FROM employees
             WHERE deleted_at IS NULL
               AND (start_date IS NULL OR date(start_date)<=date(?))
               AND (end_date IS NULL OR date(end_date)>date(?))"""
    args=[date_cut or "9999-12-31", date_cut or "9999-12-31"]
    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower({k})=?"; args.append(filters[k].lower())
    return get_conn().execute(sql, args).fetchone()[0]

def turnover_rate(conn, date_from:str, date_to:str, filters:dict=None)->float:
    """ crude monthly/period turnover: leavers / average headcount """
    leaves = count_leaves(conn, date_from, date_to, filters)
    # average headcount (start & end)
    hc_start = headcount_as_of(conn, date_from, filters)
    hc_end   = headcount_as_of(conn, date_to, filters)
    denom = (hc_start + hc_end)/2 if (hc_start or hc_end) else 0
    return (leaves / denom * 100.0) if denom else 0.0

from datetime import datetime, timedelta

REQUIRED_TRAININGS = (
    "Food Safety Training",
    "Fire Safety Training",
    "First Aid Training",
    "Pest Control Training",
    "Occupational Health and Safety Training",
    "6S Training",
    "Water Treatment Plant Training",
)

def employees_due_flags(conn, filters:dict=None)->dict:
    """
    Returns {emp_id: {has_any_training, any_due_30, any_overdue, any_missing}}
    Probation (6 months) applies ONLY to employees that have a recorded 'hire' event.
    Missing trainings count as due ONLY if not on probation.
    """
    filters = filters or {}
    cur = conn.cursor()

    # Candidate employees (active)
    sql_emp = """SELECT id
                 FROM employees
                 WHERE deleted_at IS NULL
                   AND (end_date IS NULL OR date(end_date) > date('now'))"""
    args=[]
    for k in ("department","store","position","region"):
        if filters.get(k):
            sql_emp += f" AND lower({k})=?"; args.append(filters[k].lower())
    emps = cur.execute(sql_emp, args).fetchall()
    if not emps:
        return {}
    emp_ids = [e[0] for e in emps]

    # Hire events (only these determine probation)
    qmarks = ",".join("?"*len(emp_ids))
    hires = cur.execute(f"""
        SELECT employee_id, MAX(event_date) AS hire_date
        FROM employment_events
        WHERE event_type='hire' AND employee_id IN ({qmarks})
        GROUP BY employee_id
    """, emp_ids).fetchall()
    hire_map = {eid: (hd or "") for eid, hd in hires}

    # All trainings for these employees
    rows = cur.execute(f"""
        SELECT employee_id, training_title, training_date, next_due_date
        FROM trainings
        WHERE deleted_at IS NULL AND employee_id IN ({qmarks})
    """, emp_ids).fetchall()

    from collections import defaultdict
    def bucket(title:str)->str:
        t=str(title or "").lower()
        if "food safety" in t: return "Food Safety Training"
        if "first aid" in t:   return "First Aid Training"
        if "pest" in t:        return "Pest Control Training"
        if "fire" in t:        return "Fire Safety Training"
        if "occupational health" in t or "ohs" in t: return "Occupational Health and Safety Training"
        if "6s" in t:          return "6S Training"
        if "water treatment" in t: return "Water Treatment Plant Training"
        return title or ""

    last = defaultdict(dict)  # last[emp_id][bucket] = (date, next_due)
    for eid, title, tdate, ndue in rows:
        b = bucket(title)
        prev = last[eid].get(b)
        if prev is None or (tdate and prev[0] and tdate > prev[0]) or prev is None:
            last[eid][b] = (tdate, ndue)

    from datetime import datetime, timedelta
    today = datetime.now().date()
    REQUIRED_TRAININGS = (
        "Food Safety Training","Fire Safety Training","First Aid Training",
        "Pest Control Training","Occupational Health and Safety Training",
        "6S Training","Water Treatment Plant Training",
    )

    flags={}
    for eid in emp_ids:
        # probation ONLY if we have a hire event within 183 days
        h = hire_map.get(eid, "")
        on_probation = False
        if h:
            try:
                on_probation = (today - datetime.strptime(h, "%Y-%m-%d").date()) < timedelta(days=183)
            except Exception:
                on_probation = False

        has_any = len(last.get(eid, {})) > 0
        any_missing=False; any_due_30=False; any_overdue=False

        for req in REQUIRED_TRAININGS:
            rec = last.get(eid, {}).get(req)
            if rec is None:
                # missing counts as due ONLY if not on probation
                if not on_probation:
                    any_missing = True
                    any_due_30 = True
                continue

            _, ndue = rec
            try:
                nd = datetime.strptime(ndue, "%Y-%m-%d").date() if ndue else None
            except Exception:
                nd = None
            if nd:
                if nd < today:
                    any_overdue = True
                elif (nd - today).days <= 30:
                    any_due_30 = True

        flags[eid] = {
            "has_any_training": bool(has_any),
            "any_due_30": bool(any_due_30),
            "any_overdue": bool(any_overdue),
            "any_missing": bool(any_missing),
        }
    return flags


# ---- NEW REPORTS FUNCTIONS ----

def training_frequency_report(conn, year:int=None, filters:dict=None):
    """
    Returns detailed report of how many times each training has been held,
    with dates and count per training title.
    """
    from datetime import date
    if year is None:
        year = date.today().year

    filters = filters or {}
    sql = """
        SELECT t.training_title,
               COUNT(*) as times_held,
               GROUP_CONCAT(date(t.training_date), ', ') as dates,
               MIN(date(t.training_date)) as first_date,
               MAX(date(t.training_date)) as last_date
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE t.deleted_at IS NULL
          AND e.deleted_at IS NULL
          AND strftime('%Y', t.training_date) = ?
    """
    params = [str(year)]

    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())

    sql += " GROUP BY t.training_title ORDER BY times_held DESC"

    rows = conn.execute(sql, params).fetchall()
    return rows


def stores_trained_count(conn, year:int=None, filters:dict=None):
    """
    Returns count of unique stores that have had at least one training.
    """
    from datetime import date
    if year is None:
        year = date.today().year

    filters = filters or {}
    sql = """
        SELECT COUNT(DISTINCT e.store) as stores_trained
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE t.deleted_at IS NULL
          AND e.deleted_at IS NULL
          AND strftime('%Y', t.training_date) = ?
    """
    params = [str(year)]

    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())

    return conn.execute(sql, params).fetchone()[0]


def stores_pending_training(conn, year:int=None, filters:dict=None):
    """
    Returns count of stores that have NOT had any training this year.
    """
    from datetime import date
    if year is None:
        year = date.today().year

    filters = filters or {}

    # All stores
    sql_all = "SELECT COUNT(DISTINCT store) FROM employees WHERE deleted_at IS NULL"
    params_all = []
    for k in ("region",):  # only filter by region for fairness
        if filters.get(k):
            sql_all += f" AND lower({k})=?"
            params_all.append(filters[k].lower())

    total_stores = conn.execute(sql_all, params_all).fetchone()[0]

    # Stores trained
    trained = stores_trained_count(conn, year, filters)

    return total_stores - trained


def staff_eligible_for_training(conn, filters:dict=None):
    """
    Returns count of all active employees (not on probation, not terminated).
    """
    filters = filters or {}
    sql = """
        SELECT COUNT(DISTINCT e.id)
        FROM employees e
        WHERE e.deleted_at IS NULL
          AND (e.end_date IS NULL OR date(e.end_date) > date('now'))
    """
    params = []

    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())

    return conn.execute(sql, params).fetchone()[0]


def staff_participated_in_training(conn, year:int=None, filters:dict=None):
    """
    Returns count of unique employees who have had at least one training.
    """
    from datetime import date
    if year is None:
        year = date.today().year

    filters = filters or {}
    sql = """
        SELECT COUNT(DISTINCT t.employee_id)
        FROM trainings t
        JOIN employees e ON e.id = t.employee_id
        WHERE t.deleted_at IS NULL
          AND e.deleted_at IS NULL
          AND strftime('%Y', t.training_date) = ?
    """
    params = [str(year)]

    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())

    return conn.execute(sql, params).fetchone()[0]


def staff_yet_to_be_trained(conn, filters:dict=None):
    """
    Returns list of employees who have missing required trainings (not on probation).
    Includes when their next training is due.
    """
    flags = employees_due_flags(conn, filters)

    # Filter to only those with missing trainings
    result = []
    for emp_id, status in flags.items():
        if status.get("any_missing", False):
            # Get employee details
            emp = conn.execute("""
                SELECT id, name, department, store, position, region
                FROM employees
                WHERE id=?
            """, (emp_id,)).fetchone()

            if emp:
                result.append({
                    "id": emp[0],
                    "name": emp[1],
                    "department": emp[2],
                    "store": emp[3],
                    "position": emp[4],
                    "region": emp[5],
                    "status": "Missing trainings - DUE NOW"
                })

    return result


def trainings_pending_count(conn, filters:dict=None):
    """
    Returns count of trainings that are due or overdue.
    """
    flags = employees_due_flags(conn, filters)

    pending = sum(1 for v in flags.values() if v.get("any_due_30", False) or v.get("any_overdue", False))

    return pending


def food_handlers_report(conn, filters:dict=None):
    """
    Returns detailed Food Safety Training report:
    - Required: All active employees
    - Tested: Employees with Food Safety training
    - Due for testing: Employees with overdue or upcoming Food Safety training
    """
    filters = filters or {}

    # All active employees (required to be tested)
    required = staff_eligible_for_training(conn, filters)

    # Get all employees and their food safety training status
    sql = """
        SELECT e.id, e.name, e.department, e.store, e.position, e.region,
               MAX(t.training_date) as last_food_safety_date,
               MAX(t.next_due_date) as next_due_date
        FROM employees e
        LEFT JOIN trainings t ON e.id = t.employee_id
            AND t.deleted_at IS NULL
            AND (lower(t.training_title) LIKE '%food safety%' OR lower(t.training_title) LIKE '%food handler%')
        WHERE e.deleted_at IS NULL
          AND (e.end_date IS NULL OR date(e.end_date) > date('now'))
    """
    params = []

    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())

    sql += " GROUP BY e.id"

    rows = conn.execute(sql, params).fetchall()

    tested = 0
    due_for_testing = []

    from datetime import datetime, timedelta
    today = datetime.now().date()

    for row in rows:
        emp_id, name, dept, store, position, region, last_date, next_due = row

        if last_date:  # Has been tested
            tested += 1

            # Check if due for re-testing
            if next_due:
                try:
                    due_date = datetime.strptime(next_due, "%Y-%m-%d").date()
                    if due_date <= today + timedelta(days=30):  # Due within 30 days or overdue
                        due_for_testing.append({
                            "id": emp_id,
                            "name": name,
                            "department": dept,
                            "store": store,
                            "position": position,
                            "region": region,
                            "last_tested": last_date,
                            "due_date": next_due,
                            "status": "OVERDUE" if due_date < today else f"DUE IN {(due_date - today).days} DAYS"
                        })
                except Exception:
                    pass
        else:
            # Never tested
            due_for_testing.append({
                "id": emp_id,
                "name": name,
                "department": dept,
                "store": store,
                "position": position,
                "region": region,
                "last_tested": "NEVER",
                "due_date": "NOW",
                "status": "NEVER TESTED"
            })

    return {
        "required": required,
        "tested": tested,
        "due_count": len(due_for_testing),
        "due_details": due_for_testing
    }


def trained_employees_who_left(conn, filters:dict=None):
    """
    Returns employees who received training but have since left the company.
    Useful for understanding training ROI and retention patterns.
    """
    filters = filters or {}

    sql = """
        SELECT
            e.id,
            e.name,
            e.email,
            e.department,
            e.store,
            e.position,
            e.region,
            e.start_date,
            e.end_date,
            COUNT(t.id) as total_trainings,
            GROUP_CONCAT(t.training_title, ' | ') as training_titles,
            MIN(date(t.training_date)) as first_training_date,
            MAX(date(t.training_date)) as last_training_date
        FROM employees e
        INNER JOIN trainings t ON e.id = t.employee_id
        WHERE t.deleted_at IS NULL
          AND (e.end_date IS NOT NULL OR e.deleted_at IS NOT NULL)
    """

    params = []

    # Apply filters
    for k in ("department", "store", "position", "region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())

    sql += """
        GROUP BY e.id, e.name, e.email, e.department, e.store, e.position, e.region, e.start_date, e.end_date
        ORDER BY e.end_date DESC, e.name COLLATE NOCASE
    """

    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()

