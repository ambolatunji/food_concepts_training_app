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
    cur = conn.cursor()
    ts = now()
    cur.execute("""
        INSERT INTO employees (unique_key, employee_code, name, email, department, store, position, region, start_date, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(unique_key) DO UPDATE SET
          employee_code=excluded.employee_code,
          name=excluded.name,
          email=excluded.email,
          department=excluded.department,
          store=excluded.store,
          position=excluded.position,
          region=excluded.region,
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
                    training_title:str=None, training_venue:str=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trainings (employee_id, training_date, next_due_date, evidence_path, evidence_mime, evidence_size, training_title, training_venue, created_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (employee_id, training_date, next_due_date, evidence_path, evidence_mime, evidence_size, training_title or "", training_venue or "", now()))
    conn.commit()

def list_trainings(conn, filters:Dict=None):
    filters = filters or {}
    sql = """SELECT t.id, e.name, e.department, e.store, e.position, e.region,
                    t.training_date, t.next_due_date, t.evidence_path,
                    t.training_title, t.training_venue
             FROM trainings t
             JOIN employees e ON e.id = t.employee_id
             WHERE 1=1"""
    params=[]
    for k in ("department","store","position","region"):
        if filters.get(k):
            sql += f" AND lower(e.{k})=?"
            params.append(filters[k].lower())
    if filters.get("date_from"):
        sql += " AND date(t.training_date)>=date(?)"
        params.append(filters["date_from"])
    if filters.get("date_to"):
        sql += " AND date(t.training_date)<=date(?)"
        params.append(filters["date_to"])
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

def list_trainings(conn, filters:Dict=None):
    filters = filters or {}
    sql = """SELECT t.id, e.name, e.department, e.store, e.position, e.region,
                    t.training_date, t.next_due_date, t.evidence_path,
                    t.training_title, t.training_venue
             FROM trainings t
             JOIN employees e ON e.id = t.employee_id
             WHERE t.deleted_at IS NULL AND e.deleted_at IS NULL"""
    
def count_employees(conn)->int:
    return conn.execute("SELECT COUNT(*) FROM employees WHERE deleted_at IS NULL").fetchone()[0]

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
