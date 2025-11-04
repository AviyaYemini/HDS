import hashlib
import json
from typing import Dict, List, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette import status

from app.db import get_connection
from app.utils import calculate_shift_hours
from app.routes.admin import build_admin_report_data

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
    hours = calculate_shift_hours(date, start_time, end_time)
    if hours <= 0:
        raise ValueError("שעות העבודה חייבות להיות חיוביות")
    if hours > 16:
        raise ValueError("לא ניתן לדווח על משמרת מעל 16 שעות רצופות")

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


def _require_login(request: Request, admin: bool = False) -> Optional[RedirectResponse]:
    employee_id = _session_employee_id(request)
    if not employee_id:
        return _redirect("/login", error="יש להתחבר למערכת כדי לצפות בעמוד זה")
    if admin and not request.session.get("is_admin"):
        return _redirect("/login", error="אין לך הרשאות מתאימות")
    return None


def _build_employee_report(employee_id: int, start_date: Optional[str], end_date: Optional[str]) -> Dict:
    start_date = (start_date or "").strip() or None
    end_date = (end_date or "").strip() or None

    conn = get_connection()
    try:
        cur = conn.cursor()
        employee = _fetch_employee(cur, employee_id)

        query = """
            SELECT
                s.date,
                s.start_time,
                s.end_time,
                s.location,
                COALESCE(p.name, 'לא הוגדר פרויקט') AS project,
                COALESCE(p.hourly_rate, 0) AS hourly_rate
            FROM shifts s
            INNER JOIN ShiftAssignments sa ON sa.shift_id = s.id
            LEFT JOIN projects p ON p.id = s.project_id
            WHERE sa.employee_id = ?
        """
        params: List[Optional[str]] = [employee_id]
        if start_date:
            query += " AND s.date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND s.date <= ?"
            params.append(end_date)

        query += " ORDER BY s.date ASC, s.start_time ASC"
        cur.execute(query, params)
        rows = cur.fetchall()
    finally:
        conn.close()

    report_rows: List[Dict] = []
    total_hours = 0.0
    total_amount = 0.0
    project_summary: Dict[str, Dict[str, float]] = {}

    for row in rows:
        hours = calculate_shift_hours(row["date"], row["start_time"], row["end_time"])
        rate = row["hourly_rate"] or 0
        amount = round(hours * rate, 2)
        total_hours += hours
        total_amount += amount

        project_key = row["project"]
        summary = project_summary.setdefault(
            project_key,
            {"project": project_key, "hours": 0.0, "amount": 0.0},
        )
        summary["hours"] += hours
        summary["amount"] += amount

        report_rows.append(
            {
                "date": row["date"],
                "location": row["location"],
                "project": row["project"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "hours": hours,
                "hourly_rate": rate,
                "amount": amount,
            }
        )

    project_totals = sorted(
        (
            {
                "project": data["project"],
                "hours": round(data["hours"], 2),
                "amount": round(data["amount"], 2),
            }
            for data in project_summary.values()
        ),
        key=lambda item: item["amount"],
        reverse=True,
    )

    return {
        "employee": employee,
        "rows": report_rows,
        "total_hours": round(total_hours, 2),
        "total_amount": round(total_amount, 2),
        "project_totals": project_totals,
        "start_date": start_date or "",
        "end_date": end_date or "",
    }


@router.get("/employees", response_class=HTMLResponse)
def employee_index(request: Request):
    if (redirect := _require_login(request, admin=True)):
        return redirect
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
    if (redirect := _require_login(request, admin=True)):
        return redirect
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
    if (redirect := _require_login(request, admin=True)):
        return redirect
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
    request: Request,
    employee_id: int,
    project_name: Optional[str] = Form(default=""),
    location: str = Form(...),
    date: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
):
    if (redirect := _require_login(request, admin=True)):
        return redirect
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

        try:
            _create_shift_assignment(
                cur,
                employee_id=employee_id,
                project_name=project_name,
                location=location,
                date=date,
                start_time=start_time,
                end_time=end_time,
            )
        except ValueError as exc:
            return _redirect(
                f"/employees/{employee_id}/schedule",
                error=str(exc),
            )
        conn.commit()
    finally:
        conn.close()

    return _redirect(
        f"/employees/{employee_id}/schedule",
        message="המשמרת נוספה בהצלחה",
    )


@router.get("/login", response_class=HTMLResponse)
@router.get("/portal/login", response_class=HTMLResponse)
def employee_portal_login(request: Request):
    if _session_employee_id(request):
        return _redirect("/")
    return templates.TemplateResponse(
        "employee_portal_login.html",
        {
            "request": request,
            "error": request.query_params.get("error"),
            "message": request.query_params.get("message"),
            "email": request.query_params.get("email", ""),
        },
    )


@router.post("/login")
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
            SELECT id, name, password_hash, is_admin, active
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

    if not employee["active"]:
        return templates.TemplateResponse(
            "employee_portal_login.html",
            {
                "request": request,
                "error": "חשבון זה אינו פעיל",
                "email": email,
            },
            status_code=status.HTTP_403_FORBIDDEN,
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
    request.session["is_admin"] = bool(employee["is_admin"])
    return _redirect("/", message="ברוך/ה הבא/ה למערכת")


@router.get("/logout")
@router.get("/portal/logout")
def employee_portal_logout(request: Request):
    request.session.pop("employee_id", None)
    request.session.pop("is_admin", None)
    return _redirect("/login", message="התנתקת בהצלחה")


@router.get("/portal/shifts", response_class=HTMLResponse)
def employee_portal_shifts(request: Request):
    employee_id = _session_employee_id(request)
    if not employee_id:
        return _redirect("/login", error="יש להתחבר לפני שמזינים משמרות")

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
        return _redirect("/login", error="יש להתחבר לפני שמזינים משמרות")

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
        try:
            _create_shift_assignment(
                cur,
                employee_id=employee_id,
                project_name=project_name,
                location=location,
                date=date,
                start_time=start_time,
                end_time=end_time,
            )
        except ValueError as exc:
            return _redirect(
                "/portal/shifts",
                error=str(exc),
            )
        conn.commit()
    finally:
        conn.close()

    return _redirect(
        "/portal/shifts",
        message="המשמרת נשמרה בהצלחה",
    )


@router.get("/portal/report", response_class=HTMLResponse)
def employee_portal_report(
    request: Request,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if (redirect := _require_login(request)):
        return redirect

    params: Dict[str, str] = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return _redirect("/reports", **params)


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
    if (redirect := _require_login(request, admin=True)):
        return redirect
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
    request: Request,
    employee_id: int,
    kind: str = Form(...),
    scope: str = Form(...),
    value: str = Form(...),
    valid_from: Optional[str] = Form(default=""),
    valid_to: Optional[str] = Form(default=""),
):
    if (redirect := _require_login(request, admin=True)):
        return redirect
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
@router.get("/reports", response_class=HTMLResponse)
def unified_reports(
    request: Request,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    admin_start: Optional[str] = None,
    admin_end: Optional[str] = None,
):
    if (redirect := _require_login(request)):
        return redirect

    employee_id = _session_employee_id(request)
    personal_data = _build_employee_report(employee_id, start_date, end_date)
    context = {
        "request": request,
        "personal": personal_data,
        "is_admin": bool(request.session.get("is_admin")),
    }

    if request.session.get("is_admin"):
        admin_data = build_admin_report_data(
            request,
            admin_start,
            admin_end,
            request.query_params.getlist("project"),
        )
        if isinstance(admin_data, RedirectResponse):
            return admin_data
        context["admin_reports"] = admin_data

    return templates.TemplateResponse("reports.html", context)
