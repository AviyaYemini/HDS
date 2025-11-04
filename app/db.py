import sqlite3

DB_PATH = "database.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT,
        phone TEXT,
        active INTEGER DEFAULT 1,
        password_hash TEXT,
        is_admin INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS EmployeeAuthTokens (
        employee_id INTEGER,
        token TEXT PRIMARY KEY,
        expires_at TEXT,
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );

    CREATE TABLE IF NOT EXISTS EmployeeConstraints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL,
        kind TEXT NOT NULL,
        scope TEXT NOT NULL,
        value_json TEXT NOT NULL,
        valid_from TEXT,
        valid_to TEXT,
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );

    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        hourly_rate REAL DEFAULT 0,
        active INTEGER DEFAULT 1,
        morning_required INTEGER DEFAULT 0,
        afternoon_required INTEGER DEFAULT 0,
        night_required INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER,
        date TEXT,
        start_time TEXT,
        end_time TEXT,
        location TEXT,
        FOREIGN KEY(project_id) REFERENCES projects(id)
    );

    CREATE TABLE IF NOT EXISTS ShiftAssignments (
        shift_id INTEGER,
        employee_id INTEGER,
        PRIMARY KEY (shift_id, employee_id),
        FOREIGN KEY(shift_id) REFERENCES shifts(id),
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );
    """)

    # אם השדה employee_id עדיין קיים בטבלת shifts הישנה, נמפה נתונים לטבלת השיוכים החדשה
    cur.execute("PRAGMA table_info(employees)")
    employee_columns = [row[1] for row in cur.fetchall()]
    if "password_hash" not in employee_columns:
        cur.execute("ALTER TABLE employees ADD COLUMN password_hash TEXT")
    if "email" not in employee_columns:
        cur.execute("ALTER TABLE employees ADD COLUMN email TEXT")
    if "phone" not in employee_columns:
        cur.execute("ALTER TABLE employees ADD COLUMN phone TEXT")
    if "active" not in employee_columns:
        cur.execute("ALTER TABLE employees ADD COLUMN active INTEGER DEFAULT 1")
    if "is_admin" not in employee_columns:
        cur.execute("ALTER TABLE employees ADD COLUMN is_admin INTEGER DEFAULT 0")

    cur.execute("PRAGMA table_info(projects)")
    project_columns = [row[1] for row in cur.fetchall()]
    if "hourly_rate" not in project_columns:
        cur.execute("ALTER TABLE projects ADD COLUMN hourly_rate REAL DEFAULT 0")
    if "active" not in project_columns:
        cur.execute("ALTER TABLE projects ADD COLUMN active INTEGER DEFAULT 1")
    if "morning_required" not in project_columns:
        cur.execute("ALTER TABLE projects ADD COLUMN morning_required INTEGER DEFAULT 0")
    if "afternoon_required" not in project_columns:
        cur.execute("ALTER TABLE projects ADD COLUMN afternoon_required INTEGER DEFAULT 0")
    if "night_required" not in project_columns:
        cur.execute("ALTER TABLE projects ADD COLUMN night_required INTEGER DEFAULT 0")

    cur.execute("PRAGMA table_info(shifts)")
    shift_columns = [row[1] for row in cur.fetchall()]
    if "location" not in shift_columns:
        cur.execute("ALTER TABLE shifts ADD COLUMN location TEXT")

    if "employee_id" in shift_columns:
        cur.execute("""
            INSERT OR IGNORE INTO ShiftAssignments (shift_id, employee_id)
            SELECT id, employee_id
            FROM shifts
            WHERE employee_id IS NOT NULL
        """)

    conn.commit()
    conn.close()
