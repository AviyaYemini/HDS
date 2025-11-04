import hashlib
import json
from typing import Dict, List, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette import status

from app.db import get_connection

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _fetch_employee(cur, employee_id: int) -> Dict:
    cur.execute(
        """
        SELECT id, name, email, phone, active
        FROM employees
        WHERE id = ?
        """,
        (employee_id,),
    )
    employee = cur.fetchone()
    if employee is None:
        raise HTTPException(status_code=404, detail="העובד לא נמצא")
    return employee


def _create_shift_assignment(
    cur,
    employee_id: int,
    project_name: str,
    location: str,
    date: str,
    start_time: str,
    end_time: str,
) -> int:
    project_id = None
    if project_name:
        cur.execute("SELECT id FROM projects WHERE name = ?", (project_name,))
        row = cur.fetchone()
        if row:
            project_id = row["id"]
        else:
            cur.execute("INSERT INTO projects (name) VALUES (?)", (project_name,))
            project_id = cur.lastrowid

    cur.execute(
        """
        INSERT INTO shifts (project_id, date, start_time, end_time, location)
        VALUES (?, ?, ?, ?, ?)
        """,
        (project_id, date, start_time, end_time, location),
    )
    shift_id = cur.lastrowid
    cur.execute(
        """
        INSERT OR IGNORE INTO ShiftAssignments (shift_id, employee_id)
        VALUES (?, ?)
        """,
        (shift_id, employee_id),
    )
    return shift_id


def _hash_password(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_email(value: str) -> str:
    return value.strip().lower()


def _session_employee_id(request: Request) -> Optional[int]:
    value = request.session.get("employee_id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _redirect(url: str, **params) -> RedirectResponse:
    target = url
    if params:
        query = urlencode(params, doseq=True)
        separator = "&" if "?" in url else "?"
        target = f"{url}{separator}{query}"
    return RedirectResponse(target, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/employees", response_class=HTMLResponse)
def employee_index(request: Request):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                e.id,
                e.name,
                e.email,
                e.phone,
                e.active,
                COUNT(sa.shift_id) AS assigned_shifts
            FROM employees e
            LEFT JOIN ShiftAssignments sa ON sa.employee_id = e.id
            GROUP BY e.id
            ORDER BY e.active DESC, e.name
            """
        )
        employees = cur.fetchall()
    finally:
        conn.close()

    return templates.TemplateResponse(
        "employee_list.html",
        {
            "request": request,
            "employees": employees,
        },
    )


@router.get("/employees/{employee_id}/profile", response_class=HTMLResponse)
def employee_profile(employee_id: int, request: Request):
    conn = get_connection()
    try:
        cur = conn.cursor()
        employee = _fetch_employee(cur, employee_id)
        cur.execute(
            """
            SELECT COUNT(*) AS total
            FROM ShiftAssignments
            WHERE employee_id = ?
            """,
            (employee_id,),
        )
        assignment_summary = cur.fetchone()
    finally:
        conn.close()

    return templates.TemplateResponse(
        "employee_profile.html",
        {
            "request": request,
            "employee": employee,
            "assignment_count": assignment_summary["total"] if assignment_summary else 0,
        },
    )


@router.get("/employees/{employee_id}/schedule", response_class=HTMLResponse)
def employee_schedule(employee_id: int, request: Request):
    conn = get_connection()
    try:
        cur = conn.cursor()
        employee = _fetch_employee(cur, employee_id)
        cur.execute(
            """
            SELECT
                s.id,
                s.date,
                s.start_time,
                s.end_time,
                s.location,
                COALESCE(p.name, 'לא הוגדר פרויקט') AS project
            FROM shifts s
            INNER JOIN ShiftAssignments sa ON sa.shift_id = s.id
            LEFT JOIN projects p ON p.id = s.project_id
            WHERE sa.employee_id = ?
            ORDER BY s.date, s.start_time
            """,
            (employee_id,),
        )
        shifts = cur.fetchall()
        cur.execute("SELECT name FROM projects ORDER BY name")
        projects = [row["name"] for row in cur.fetchall()]
    finally:
        conn.close()

    return templates.TemplateResponse(
        "employee_schedule.html",
        {
            "request": request,
            "employee": employee,
            "shifts": shifts,
            "projects": projects,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@router.post("/employees/{employee_id}/schedule")
def add_employee_shift(
    employee_id: int,
    project_name: Optional[str] = Form(default=""),
    location: str = Form(...),
    date: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
):
    project_name = (project_name or "").strip()
    location = (location or "").strip()
    date = date.strip()
    start_time = start_time.strip()
    end_time = end_time.strip()

    if not location or not date or not start_time or not end_time:
        return _redirect(
            f"/employees/{employee_id}/schedule",
            error="נא למלא מיקום, תאריך ושעות ההתחלה/סיום",
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        _fetch_employee(cur, employee_id)

        _create_shift_assignment(
            cur,
            employee_id=employee_id,
            project_name=project_name,
            location=location,
            date=date,
            start_time=start_time,
            end_time=end_time,
        )
        conn.commit()
    finally:
        conn.close()

    return _redirect(
        f"/employees/{employee_id}/schedule",
        message="המשמרת נוספה בהצלחה",
    )


@router.get("/portal/login", response_class=HTMLResponse)
def employee_portal_login(request: Request):
    if _session_employee_id(request):
        return _redirect("/portal/shifts")
    return templates.TemplateResponse(
        "employee_portal_login.html",
        {
            "request": request,
            "error": request.query_params.get("error"),
            "message": request.query_params.get("message"),
            "email": request.query_params.get("email", ""),
        },
    )


@router.post("/portal/login")
def employee_portal_login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    email_normalized = _normalize_email(email)
    password = password.strip()

    if not email_normalized or not password:
        return templates.TemplateResponse(
            "employee_portal_login.html",
            {
                "request": request,
                "error": "נא להזין דוא\"ל וסיסמה",
                "email": email,
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, password_hash
            FROM employees
            WHERE LOWER(email) = ?
            """,
            (email_normalized,),
        )
        employee = cur.fetchone()
    finally:
        conn.close()

    if not employee or not employee["password_hash"]:
        return templates.TemplateResponse(
            "employee_portal_login.html",
            {
                "request": request,
                "error": "פרטי ההתחברות שגויים או שלא הוגדרה סיסמה לעובד זה",
                "email": email,
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    if employee["password_hash"] != _hash_password(password):
        return templates.TemplateResponse(
            "employee_portal_login.html",
            {
                "request": request,
                "error": "פרטי ההתחברות שגויים",
                "email": email,
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    request.session["employee_id"] = int(employee["id"])
    return _redirect("/portal/shifts", message="ברוך/ה הבא/ה למערכת")


@router.get("/portal/logout")
def employee_portal_logout(request: Request):
    request.session.pop("employee_id", None)
    return _redirect("/portal/login", message="התנתקת בהצלחה")


@router.get("/portal/shifts", response_class=HTMLResponse)
def employee_portal_shifts(request: Request):
    employee_id = _session_employee_id(request)
    if not employee_id:
        return _redirect("/portal/login", error="יש להתחבר לפני שמזינים משמרות")

    conn = get_connection()
    try:
        cur = conn.cursor()
        employee = _fetch_employee(cur, employee_id)
        cur.execute(
            """
            SELECT
                s.id,
                s.date,
                s.start_time,
                s.end_time,
                s.location,
                COALESCE(p.name, 'לא הוגדר פרויקט') AS project
            FROM shifts s
            INNER JOIN ShiftAssignments sa ON sa.shift_id = s.id
            LEFT JOIN projects p ON p.id = s.project_id
            WHERE sa.employee_id = ?
            ORDER BY s.date DESC, s.start_time DESC
            LIMIT 50
            """,
            (employee_id,),
        )
        shifts = cur.fetchall()
        cur.execute("SELECT name FROM projects ORDER BY name")
        projects = [row["name"] for row in cur.fetchall()]
    finally:
        conn.close()

    return templates.TemplateResponse(
        "employee_portal_shifts.html",
        {
            "request": request,
            "employee": employee,
            "shifts": shifts,
            "projects": projects,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@router.post("/portal/shifts")
def employee_portal_add_shift(
    request: Request,
    project_name: Optional[str] = Form(default=""),
    location: str = Form(...),
    date: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
):
    employee_id = _session_employee_id(request)
    if not employee_id:
        return _redirect("/portal/login", error="יש להתחבר לפני שמזינים משמרות")

    project_name = (project_name or "").strip()
    location = (location or "").strip()
    date = date.strip()
    start_time = start_time.strip()
    end_time = end_time.strip()

    if not location or not date or not start_time or not end_time:
        return _redirect(
            "/portal/shifts",
            error="נא למלא מיקום, תאריך ושעות התחלה/סיום",
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        _fetch_employee(cur, employee_id)
        _create_shift_assignment(
            cur,
            employee_id=employee_id,
            project_name=project_name,
            location=location,
            date=date,
            start_time=start_time,
            end_time=end_time,
        )
        conn.commit()
    finally:
        conn.close()

    return _redirect(
        "/portal/shifts",
        message="המשמרת נשמרה בהצלחה",
    )


def _format_constraint_value(raw_value: str) -> str:
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return raw_value or "-"

    if isinstance(parsed, dict):
        return ", ".join(f"{key}: {value}" for key, value in parsed.items()) or "-"
    if isinstance(parsed, list):
        return ", ".join(str(item) for item in parsed) or "-"
    return str(parsed)


@router.get("/employees/{employee_id}/availability", response_class=HTMLResponse)
def employee_availability(employee_id: int, request: Request):
    conn = get_connection()
    try:
        cur = conn.cursor()
        employee = _fetch_employee(cur, employee_id)
        cur.execute(
            """
            SELECT
                id,
                kind,
                scope,
                value_json,
                valid_from,
                valid_to
            FROM EmployeeConstraints
            WHERE employee_id = ?
            ORDER BY
                COALESCE(valid_from, '') ASC,
                id ASC
            """,
            (employee_id,),
        )
        constraints_rows = cur.fetchall()
    finally:
        conn.close()

    constraints: List[Dict] = []
    for row in constraints_rows:
        as_dict = dict(row)
        as_dict["display_value"] = _format_constraint_value(as_dict.get("value_json"))
        constraints.append(as_dict)

    return templates.TemplateResponse(
        "employee_availability.html",
        {
            "request": request,
            "employee": employee,
            "constraints": constraints,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@router.post("/employees/{employee_id}/availability")
def add_employee_constraint(
    employee_id: int,
    kind: str = Form(...),
    scope: str = Form(...),
    value: str = Form(...),
    valid_from: Optional[str] = Form(default=""),
    valid_to: Optional[str] = Form(default=""),
):
    kind = kind.strip()
    scope = scope.strip()
    value = value.strip()
    valid_from = valid_from.strip() or None
    valid_to = valid_to.strip() or None

    if not kind or not scope or not value:
        return _redirect(
            f"/employees/{employee_id}/availability",
            error="נא למלא את סוג ההגבלה, היקפה והערך",
        )

    try:
        # ננסה לפרש כ-JSON לקבלת ערכים מורכבים, ואם לא – נשמור כמחרוזת
        parsed_value = json.loads(value)
    except json.JSONDecodeError:
        parsed_value = value

    conn = get_connection()
    try:
        cur = conn.cursor()
        _fetch_employee(cur, employee_id)
        cur.execute(
            """
            INSERT INTO EmployeeConstraints (
                employee_id,
                kind,
                scope,
                value_json,
                valid_from,
                valid_to
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                employee_id,
                kind,
                scope,
                json.dumps(parsed_value, ensure_ascii=False),
                valid_from,
                valid_to,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return _redirect(
        f"/employees/{employee_id}/availability",
        message="העדפה נשמרה בהצלחה",
    )
