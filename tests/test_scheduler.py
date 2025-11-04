import sqlite3
import unittest
from datetime import datetime

from app.routes import admin


def setup_in_memory_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            hourly_rate REAL,
            active INTEGER,
            morning_required INTEGER,
            afternoon_required INTEGER,
            night_required INTEGER
        );
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            active INTEGER
        );
        CREATE TABLE EmployeeConstraints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER,
            kind TEXT,
            scope TEXT,
            value_json TEXT,
            valid_from TEXT,
            valid_to TEXT
        );
        CREATE TABLE shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            date TEXT,
            start_time TEXT,
            end_time TEXT,
            location TEXT
        );
        CREATE TABLE ShiftAssignments (
            shift_id INTEGER,
            employee_id INTEGER,
            PRIMARY KEY (shift_id, employee_id)
        );
        """
    )
    conn.commit()
    return conn


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.conn = setup_in_memory_db()
        self.cur = self.conn.cursor()

        self.cur.execute(
            "INSERT INTO projects (name, hourly_rate, active, morning_required, afternoon_required, night_required) VALUES (?, ?, 1, 1, 0, 0)",
            ("אתר מבחן", 50.0),
        )
        self.project = self.cur.execute("SELECT * FROM projects").fetchone()

        employees = [("אלי", 1), ("נועה", 1)]
        self.cur.executemany("INSERT INTO employees (name, active) VALUES (?, ?)", employees)
        self.conn.commit()
        self.employees = self.cur.execute("SELECT id, name FROM employees ORDER BY id").fetchall()

    def tearDown(self):
        self.conn.close()

    def test_preferred_shift_is_selected(self):
        pref_constraint = {
            "kind": "shift",
            "scope": "shift",
            "value_json": '{"type":"shift","values":["morning"],"priority":"preferred"}'
        }
        block_constraint = {
            "kind": "shift",
            "scope": "shift",
            "value_json": '{"type":"shift","values":["morning"],"priority":"avoid"}'
        }
        constraints_map = {
            self.employees[0]["id"]: [pref_constraint],
            self.employees[1]["id"]: [block_constraint],
        }

        start_dt = datetime.strptime("2025-11-04", "%Y-%m-%d")
        end_dt = datetime.strptime("2025-11-04", "%Y-%m-%d")
        result = admin._generate_schedule_for_project(
            self.cur,
            self.project,
            self.employees,
            constraints_map,
            start_dt,
            end_dt,
            {"morning": 1, "afternoon": 0, "night": 0},
            "אתר מבחן",
        )
        self.conn.commit()

        self.assertEqual(result["total_assignments"], 1)
        assigned_employee = result["assignments_created"][0]["employee"]
        self.assertEqual(assigned_employee, self.employees[0]["name"])

    def test_blocked_shift_creates_warning(self):
        block_constraint = {
            "kind": "shift",
            "scope": "shift",
            "value_json": '{"type":"shift","values":["morning"],"priority":"avoid"}'
        }
        constraints_map = {
            self.employees[0]["id"]: [block_constraint],
            self.employees[1]["id"]: [block_constraint],
        }

        start_dt = datetime.strptime("2025-11-05", "%Y-%m-%d")
        end_dt = datetime.strptime("2025-11-05", "%Y-%m-%d")
        result = admin._generate_schedule_for_project(
            self.cur,
            self.project,
            self.employees,
            constraints_map,
            start_dt,
            end_dt,
            {"morning": 1, "afternoon": 0, "night": 0},
            "אתר מבחן",
        )

        self.assertEqual(result["total_assignments"], 0)
        self.assertTrue(result["warnings"])


if __name__ == "__main__":
    unittest.main()
