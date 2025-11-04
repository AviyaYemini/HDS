from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from starlette import status

from app.db import get_connection
from app.utils import (
    calculate_shift_hours,
    build_constraint_profile,
    constraint_allows_shift,
    normalize_shift_key,
)

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="app/templates")

SHIFT_TEMPLATES = {
    "morning": {"start": "06:00", "end": "14:00", "label": "בוקר"},
    "afternoon": {"start": "14:00", "end": "22:00", "label": "צהריים"},
    "night": {"start": "22:00", "end": "06:00", "label": "לילה"},
}

SHIFT_ORDER = ["morning", "afternoon", "night"]


def _redirect(url: str, **params) -> RedirectResponse:
    target = url
    if params:
        query = urlencode(params, doseq=True)
        separator = "&" if "?" in url else "?"
        target = f"{url}{separator}{query}"
    return RedirectResponse(target, status_code=status.HTTP_303_SEE_OTHER)


def _safe_positive_int(value: str, default: int = 0) -> int:
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else default
    except (TypeError, ValueError):
        return default


def _require_admin(request: Request) -> Optional[RedirectResponse]:
    employee_id = request.session.get("employee_id")
    if not employee_id:
        return _redirect("/login", error="נא להתחבר למערכת")
    if not request.session.get("is_admin"):
        return _redirect("/login", error="אין לך הרשאות מתאימות")
    return None


def _fetch_project_record(project_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                name,
                hourly_rate,
                active,
                morning_required,
                afternoon_required,
                night_required
            FROM projects
            WHERE id = ?
            """,
            (project_id,),
        )
        project = cur.fetchone()
    finally:
        conn.close()
    return project


def _ensure_shift(cur, project_id: int, date_str: str, start_time: str, end_time: str, location: str) -> int:
    loc_value = location or ""
    cur.execute(
        """
        SELECT id
        FROM shifts
        WHERE project_id = ?
          AND date = ?
          AND start_time = ?
          AND end_time = ?
          AND IFNULL(location, '') = ?
        """,
        (project_id, date_str, start_time, end_time, loc_value),
    )
    row = cur.fetchone()
    if row:
        return row["id"]

    cur.execute(
        """
        INSERT INTO shifts (project_id, date, start_time, end_time, location)
        VALUES (?, ?, ?, ?, ?)
        """,
        (project_id, date_str, start_time, end_time, location),
    )
    return cur.lastrowid


def _load_active_employees_with_constraints(cur):
    cur.execute(
        """
        SELECT id, name
        FROM employees
        WHERE active = 1
        ORDER BY name
        """
    )
    employees = cur.fetchall()
    employee_ids = [row["id"] for row in employees]
    constraints_map: Dict[int, List[Dict]] = defaultdict(list)

    if employee_ids:
        placeholders = ",".join("?" for _ in employee_ids)
        cur.execute(
            f"""
            SELECT employee_id, kind, scope, value_json, valid_from, valid_to
            FROM EmployeeConstraints
            WHERE employee_id IN ({placeholders})
            """,
            employee_ids,
        )
        for row in cur.fetchall():
            constraints_map[row["employee_id"]].append(dict(row))

    return employees, constraints_map


def _build_generation_context(request: Request, project, extra: Optional[Dict] = None) -> Dict:
    context = {
        "request": request,
        "project": project,
        "requirements": {
            "morning": project["morning_required"] or 0,
            "afternoon": project["afternoon_required"] or 0,
            "night": project["night_required"] or 0,
        },
        "shift_templates": SHIFT_TEMPLATES,
    }
    if extra:
        context.update(extra)
    return context


def build_admin_report_data(
    request: Request,
    start_date: Optional[str],
    end_date: Optional[str],
    project_params: Optional[List[str]] = None,
):
    if (redirect := _require_admin(request)):
        return redirect

    start_date = (start_date or "").strip() or ""
    end_date = (end_date or "").strip() or ""

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM projects ORDER BY name")
        all_projects = cur.fetchall()
    finally:
        conn.close()

    selected_ids: List[int] = []
    raw_selected = project_params or []
    for value in raw_selected:
        try:
            selected_ids.append(int(value))
        except (TypeError, ValueError):
            continue

    report_rows: List[Dict] = []
    project_summary: Dict[int, Dict] = {}
    employee_summary: Dict[int, Dict] = {}
    total_hours = 0.0
    total_amount = 0.0

    if selected_ids:
        conn = get_connection()
        try:
            cur = conn.cursor()
            placeholders = ",".join("?" for _ in selected_ids)
            query = f"""
                SELECT
                    s.id AS shift_id,
                    s.date,
                    s.start_time,
                    s.end_time,
                    s.location,
                    p.id AS project_id,
                    p.name AS project_name,
                    p.hourly_rate,
                    sa.employee_id,
                    e.name AS employee_name
                FROM shifts s
                INNER JOIN projects p ON p.id = s.project_id
                LEFT JOIN ShiftAssignments sa ON sa.shift_id = s.id
                LEFT JOIN employees e ON e.id = sa.employee_id
                WHERE s.project_id IN ({placeholders})
            """
            params: List = list(selected_ids)
            if start_date:
                query += " AND s.date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND s.date <= ?"
                params.append(end_date)

            query += " ORDER BY s.date ASC, s.start_time ASC, p.name ASC"
            cur.execute(query, params)
            rows = cur.fetchall()
        finally:
            conn.close()

        shift_hours: Dict[int, float] = {}

        for row in rows:
            shift_id = row["shift_id"]
            if shift_id not in shift_hours:
                shift_hours[shift_id] = calculate_shift_hours(
                    row["date"], row["start_time"], row["end_time"]
                )
            hours = shift_hours[shift_id]
            if hours <= 0:
                continue

            project_id = row["project_id"]
            rate = row["hourly_rate"] or 0
            amount = hours * rate

            project_entry = project_summary.setdefault(
                project_id,
                {
                    "project_id": project_id,
                    "project_name": row["project_name"],
                    "hours": 0.0,
                    "amount": 0.0,
                    "assignments": 0,
                    "employee_ids": set(),  # type: ignore
                },
            )
            project_entry["hours"] += hours
            project_entry["amount"] += amount
            project_entry["assignments"] += 1
            employee_id = row["employee_id"]
            if employee_id:
                cast_employee_ids: Set[int] = project_entry["employee_ids"]  # type: ignore
                cast_employee_ids.add(employee_id)

                employee_entry = employee_summary.setdefault(
                    employee_id,
                    {
                        "employee_id": employee_id,
                        "employee_name": row["employee_name"] or "ללא שם",
                        "hours": 0.0,
                        "amount": 0.0,
                        "assignments": 0,
                    },
                )
                employee_entry["hours"] += hours
                employee_entry["amount"] += amount
                employee_entry["assignments"] += 1

            total_hours += hours
            total_amount += amount

            report_rows.append(
                {
                    "project_name": row["project_name"],
                    "employee_name": row["employee_name"] or "-",
                    "date": row["date"],
                    "location": row["location"] or "-",
                    "start_time": row["start_time"],
                    "end_time": row["end_time"],
                    "hours": hours,
                    "amount": amount,
                }
            )

    project_totals = sorted(
        (
            {
                "project_id": entry["project_id"],
                "project_name": entry["project_name"],
                "hours": round(entry["hours"], 2),
                "amount": round(entry["amount"], 2),
                "assignments": entry["assignments"],
                "employee_count": len(entry["employee_ids"]),
            }
            for entry in project_summary.values()
        ),
        key=lambda item: item["hours"],
        reverse=True,
    )

    employee_totals = sorted(
        (
            {
                "employee_id": entry["employee_id"],
                "employee_name": entry["employee_name"],
                "hours": round(entry["hours"], 2),
                "amount": round(entry["amount"], 2),
                "assignments": entry["assignments"],
            }
            for entry in employee_summary.values()
        ),
        key=lambda item: item["hours"],
        reverse=True,
    )

    return {
        "projects": all_projects,
        "selected_ids": selected_ids,
        "start_date": start_date,
        "end_date": end_date,
        "project_totals": project_totals,
        "employee_totals": employee_totals,
        "report_rows": report_rows,
        "total_hours": round(total_hours, 2),
        "total_amount": round(total_amount, 2),
    }


def _generate_schedule_for_project(
    cur,
    project,
    employees,
    constraints_map,
    start_date: datetime,
    end_date: datetime,
    requirements: Dict[str, int],
    location: str,
):
    date_cursor = start_date.date()
    end_date_only = end_date.date()
    date_list: List[str] = []
    while date_cursor <= end_date_only:
        date_list.append(date_cursor.isoformat())
        date_cursor += timedelta(days=1)

    employee_ids = [row["id"] for row in employees]
    employee_lookup = {row["id"]: row for row in employees}

    constraint_profiles = {
        employee_id: build_constraint_profile(constraints_map.get(employee_id, []))
        for employee_id in employee_ids
    }

    employee_load = {employee_id: 0 for employee_id in employee_ids}
    preferred_map = {
        emp_id: constraint_profiles[emp_id].get("preferred_shifts", set())
        for emp_id in employee_ids
    }
    disliked_map = {
        emp_id: constraint_profiles[emp_id].get("disliked_shifts", set())
        for emp_id in employee_ids
    }
    assignments_by_employee_date: Dict[int, Set[str]] = defaultdict(set)

    if employee_ids:
        placeholders = ",".join("?" for _ in employee_ids)
        cur.execute(
            f"""
            SELECT sa.employee_id, s.date
            FROM ShiftAssignments sa
            INNER JOIN shifts s ON s.id = sa.shift_id
            WHERE sa.employee_id IN ({placeholders})
              AND s.date BETWEEN ? AND ?
            """,
            employee_ids + [date_list[0], date_list[-1]],
        )
        for row in cur.fetchall():
            assignments_by_employee_date[row["employee_id"]].add(row["date"])
            employee_load[row["employee_id"]] = employee_load.get(row["employee_id"], 0) + 1

    assignments_created: List[Dict[str, Any]] = []
    warnings: List[str] = []
    shifts_created: Set[str] = set()

    for date_str in date_list:
        for shift_key in SHIFT_ORDER:
            required = requirements.get(shift_key, 0) or 0
            if required <= 0:
                continue

            template = SHIFT_TEMPLATES[shift_key]
            start_time = template["start"]
            end_time = template["end"]
            hours = calculate_shift_hours(date_str, start_time, end_time)
            if hours <= 0 or hours > 16:
                warnings.append(
                    f"משמרת {template['label']} בתאריך {date_str} לא תקינה (משך {hours} שעות)."
                )
                continue

            shift_id = None
            slots_filled = 0

            normalized_shift = normalize_shift_key(shift_key)
            for _ in range(required):
                candidate_order = sorted(
                    employee_ids,
                    key=lambda emp_id: (
                        employee_load.get(emp_id, 0),
                        0 if normalized_shift in preferred_map.get(emp_id, set()) else 1,
                        1 if normalized_shift in disliked_map.get(emp_id, set()) else 0,
                        employee_lookup[emp_id]["name"],
                    ),
                )
                chosen_employee = None

                for employee_id in candidate_order:
                    if date_str in assignments_by_employee_date[employee_id]:
                        continue
                    if normalized_shift in disliked_map.get(employee_id, set()) and normalized_shift not in preferred_map.get(employee_id, set()):
                        continue
                    profile = constraint_profiles.get(employee_id, {})
                    if not constraint_allows_shift(profile, date_str, shift_key):
                        continue
                    chosen_employee = employee_id
                    break

                if chosen_employee is None:
                    warnings.append(
                        f"לא נמצא עובד זמין למשמרת {template['label']} בתאריך {date_str}"
                    )
                    break

                if shift_id is None:
                    shift_id = _ensure_shift(
                        cur,
                        project_id=project["id"],
                        date_str=date_str,
                        start_time=start_time,
                        end_time=end_time,
                        location=location,
                    )
                    shifts_created.add(f"{date_str}-{shift_key}")

                cur.execute(
                    """
                    INSERT OR IGNORE INTO ShiftAssignments (shift_id, employee_id)
                    VALUES (?, ?)
                    """,
                    (shift_id, chosen_employee),
                )

                if cur.rowcount == 0:
                    continue

                assignments_by_employee_date[chosen_employee].add(date_str)
                employee_load[chosen_employee] = employee_load.get(chosen_employee, 0) + 1
                assignments_created.append(
                    {
                        "date": date_str,
                        "shift": template["label"],
                        "employee": employee_lookup[chosen_employee]["name"],
                        "hours": hours,
                    }
                )
                slots_filled += 1

    return {
        "assignments_created": assignments_created,
        "total_assignments": len(assignments_created),
        "shifts_created": len(shifts_created),
        "warnings": warnings,
    }


@router.get("/", response_class=HTMLResponse)
def admin_root(request: Request):
    if (redirect := _require_admin(request)):
        return redirect
    return _redirect("/admin/overview")


@router.get("/availability", response_class=HTMLResponse)
def availability_dashboard(request: Request):
    if (redirect := _require_admin(request)):
        return redirect
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, email, phone, active
            FROM employees
            ORDER BY active DESC, name
            """
        )
        employees = cur.fetchall()

        cur.execute(
            """
            SELECT
                ec.id,
                ec.employee_id,
                ec.kind,
                ec.scope,
                ec.value_json,
                ec.valid_from,
                ec.valid_to
            FROM EmployeeConstraints ec
            ORDER BY ec.employee_id, COALESCE(ec.valid_from, '') ASC, ec.id ASC
            """
        )
        constraints_rows = cur.fetchall()
    finally:
        conn.close()

    availability_map: Dict[int, List[Dict]] = {}
    for row in constraints_rows:
        availability_map.setdefault(row["employee_id"], []).append(dict(row))

    return templates.TemplateResponse(
        "admin_availability.html",
        {
            "request": request,
            "employees": employees,
            "availability_map": availability_map,
        },
    )


@router.get("/projects", response_class=HTMLResponse)
def projects_dashboard(request: Request):
    if (redirect := _require_admin(request)):
        return redirect
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                p.id AS project_id,
                p.name,
                p.hourly_rate,
                p.active,
                p.morning_required,
                p.afternoon_required,
                p.night_required,
                s.id AS shift_id,
                s.date,
                s.start_time,
                s.end_time,
                sa.employee_id
            FROM projects p
            LEFT JOIN shifts s ON s.project_id = p.id
            LEFT JOIN ShiftAssignments sa ON sa.shift_id = s.id
            ORDER BY p.name, s.date, s.start_time
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    projects: Dict[int, Dict] = {}
    for row in rows:
        project_id = row["project_id"]
        project_summary = projects.setdefault(
            project_id,
            {
                "id": project_id,
                "name": row["name"],
                "hourly_rate": round(row["hourly_rate"] or 0, 2),
                "active": bool(row["active"]),
                "morning_required": row["morning_required"] or 0,
                "afternoon_required": row["afternoon_required"] or 0,
                "night_required": row["night_required"] or 0,
                "assigned_employees": set(),  # type: ignore
                "assignment_count": 0,
                "person_hours": 0.0,
                "payout": 0.0,
                "shift_hours": {},  # shift_id -> hours
            },
        )

        shift_id = row["shift_id"]
        if shift_id:
            if shift_id not in project_summary["shift_hours"]:
                hours = calculate_shift_hours(
                    row["date"],
                    row["start_time"],
                    row["end_time"],
                )
                project_summary["shift_hours"][shift_id] = hours
            hours = project_summary["shift_hours"][shift_id]

            employee_id = row["employee_id"]
            if employee_id:
                cast_employees: Set[int] = project_summary["assigned_employees"]  # type: ignore
                cast_employees.add(employee_id)
                project_summary["assignment_count"] += 1
                project_summary["person_hours"] += hours
                project_summary["payout"] += hours * (project_summary["hourly_rate"] or 0)

    project_list = []
    for project in projects.values():
        project_list.append(
            {
                "id": project["id"],
                "name": project["name"],
                "hourly_rate": project["hourly_rate"],
                "active": project["active"],
                "employee_count": len(project["assigned_employees"]),
                "assignment_count": project["assignment_count"],
                "person_hours": round(project["person_hours"], 2),
                "payout": round(project["payout"], 2),
                "shift_count": len(project["shift_hours"]),
                "morning_required": project["morning_required"],
                "afternoon_required": project["afternoon_required"],
                "night_required": project["night_required"],
            }
        )

    # Handle projects with no shifts yet
    if not rows:
        # fetch plain list
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, name, hourly_rate, active, morning_required, afternoon_required, night_required
                FROM projects
                ORDER BY name
                """
            )
            for row in cur.fetchall():
                project_list.append(
                    {
                        "id": row["id"],
                        "name": row["name"],
                        "hourly_rate": round(row["hourly_rate"] or 0, 2),
                        "active": bool(row["active"]),
                        "morning_required": row["morning_required"] or 0,
                        "afternoon_required": row["afternoon_required"] or 0,
                        "night_required": row["night_required"] or 0,
                        "employee_count": 0,
                        "assignment_count": 0,
                        "person_hours": 0.0,
                        "payout": 0.0,
                        "shift_count": 0,
                    }
                )
        finally:
            conn.close()

    project_list.sort(key=lambda project: project["name"])

    return templates.TemplateResponse(
        "admin_projects.html",
        {
            "request": request,
            "projects": project_list,
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
        },
    )


@router.post("/projects")
def create_project(
    request: Request,
    name: str = Form(...),
    hourly_rate: str = Form(default="0"),
    morning_required: str = Form(default="0"),
    afternoon_required: str = Form(default="0"),
    night_required: str = Form(default="0"),
):
    if (redirect := _require_admin(request)):
        return redirect
    name = name.strip()
    if not name:
        return _redirect("/admin/projects", error="יש להזין שם לפרויקט")

    try:
        rate_value = float(hourly_rate or 0)
    except ValueError:
        return _redirect("/admin/projects", error="תעריף שעתי לא חוקי")

    morning_value = _safe_positive_int(morning_required)
    afternoon_value = _safe_positive_int(afternoon_required)
    night_value = _safe_positive_int(night_required)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO projects (
                name,
                hourly_rate,
                active,
                morning_required,
                afternoon_required,
                night_required
            ) VALUES (?, ?, 1, ?, ?, ?)
            """,
            (name, rate_value, morning_value, afternoon_value, night_value),
        )
        conn.commit()
    finally:
        conn.close()

    return _redirect("/admin/projects", message="הפרויקט נוסף בהצלחה")


@router.post("/projects/{project_id}/status")
def update_project_status(
    request: Request,
    project_id: int,
    active: str = Form(...),
):
    if (redirect := _require_admin(request)):
        return redirect
    new_status = 1 if active == "open" else 0
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE projects SET active = ? WHERE id = ?",
            (new_status, project_id),
        )
        conn.commit()
    finally:
        conn.close()

    message = "הפרויקט נפתח מחדש" if new_status else "הפרויקט נסגר לשיבוץ"
    return _redirect("/admin/projects", message=message)


@router.post("/projects/{project_id}/rate")
def update_project_rate(
    request: Request,
    project_id: int,
    hourly_rate: str = Form(...),
):
    if (redirect := _require_admin(request)):
        return redirect
    try:
        rate_value = float(hourly_rate)
    except ValueError:
        return _redirect("/admin/projects", error="נא להזין ערך מספרי לתעריף")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE projects SET hourly_rate = ? WHERE id = ?",
            (rate_value, project_id),
        )
        conn.commit()
    finally:
        conn.close()

    return _redirect("/admin/projects", message="התעריף עודכן בהצלחה")


@router.post("/projects/{project_id}/requirements")
def update_project_requirements(
    request: Request,
    project_id: int,
    morning_required: str = Form(...),
    afternoon_required: str = Form(...),
    night_required: str = Form(...),
):
    if (redirect := _require_admin(request)):
        return redirect
    morning_value = _safe_positive_int(morning_required)
    afternoon_value = _safe_positive_int(afternoon_required)
    night_value = _safe_positive_int(night_required)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE projects
            SET morning_required = ?,
                afternoon_required = ?,
                night_required = ?
            WHERE id = ?
            """,
            (morning_value, afternoon_value, night_value, project_id),
        )
        conn.commit()
    finally:
        conn.close()

    return _redirect("/admin/projects", message="דרישות המשמרות עודכנו")


@router.get("/projects/{project_id}/report", response_class=HTMLResponse)
def project_report(project_id: int, request: Request):
    if (redirect := _require_admin(request)):
        return redirect
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, hourly_rate, active FROM projects WHERE id = ?",
            (project_id,),
        )
        project = cur.fetchone()
        if not project:
            return _redirect("/admin/projects", error="פרויקט לא נמצא")

        cur.execute(
            """
            SELECT
                e.id AS employee_id,
                e.name AS employee_name,
                s.id AS shift_id,
                s.date,
                s.start_time,
                s.end_time,
                s.location
            FROM shifts s
            INNER JOIN ShiftAssignments sa ON sa.shift_id = s.id
            INNER JOIN employees e ON e.id = sa.employee_id
            WHERE s.project_id = ?
            ORDER BY s.date ASC, s.start_time ASC, e.name ASC
            """,
            (project_id,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    employee_totals: Dict[int, Dict[str, float]] = {}
    total_hours = 0.0
    total_payout = 0.0
    detailed_rows: List[Dict] = []

    for row in rows:
        hours = calculate_shift_hours(row["date"], row["start_time"], row["end_time"])
        amount = hours * (project["hourly_rate"] or 0)
        total_hours += hours
        total_payout += amount

        employee_entry = employee_totals.setdefault(
            row["employee_id"],
            {
                "employee_id": row["employee_id"],
                "name": row["employee_name"],
                "hours": 0.0,
                "amount": 0.0,
            },
        )
        employee_entry["hours"] += hours
        employee_entry["amount"] += amount

        detailed_rows.append(
            {
                "employee_name": row["employee_name"],
                "date": row["date"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "location": row["location"],
                "hours": hours,
                "amount": amount,
            }
        )

    employee_summary = sorted(
        (
            {
                "employee_id": entry["employee_id"],
                "name": entry["name"],
                "hours": round(entry["hours"], 2),
                "amount": round(entry["amount"], 2),
            }
            for entry in employee_totals.values()
        ),
        key=lambda item: item["hours"],
        reverse=True,
    )

    return templates.TemplateResponse(
        "admin_project_report.html",
        {
            "request": request,
            "project": project,
            "employee_summary": employee_summary,
            "detailed_rows": detailed_rows,
            "total_hours": round(total_hours, 2),
            "total_payout": round(total_payout, 2),
        },
    )


@router.get("/projects/{project_id}/generate", response_class=HTMLResponse)
def project_generate_form(project_id: int, request: Request):
    if (redirect := _require_admin(request)):
        return redirect
    project = _fetch_project_record(project_id)
    if not project:
        return _redirect("/admin/projects", error="הפרויקט לא נמצא")

    context = _build_generation_context(
        request,
        project,
        {
            "message": request.query_params.get("message"),
            "error": request.query_params.get("error"),
            "schedule_result": None,
            "warnings": [],
        },
    )
    context.setdefault("overrides", context["requirements"])
    return templates.TemplateResponse("admin_project_generate.html", context)


@router.post("/projects/{project_id}/generate", response_class=HTMLResponse)
def project_generate_submit(
    project_id: int,
    request: Request,
    start_date: str = Form(...),
    end_date: str = Form(...),
    location: str = Form(default=""),
    morning_override: str = Form(default=""),
    afternoon_override: str = Form(default=""),
    night_override: str = Form(default=""),
):
    if (redirect := _require_admin(request)):
        return redirect
    project = _fetch_project_record(project_id)
    if not project:
        return _redirect("/admin/projects", error="הפרויקט לא נמצא")

    context_extra = {"schedule_result": None, "warnings": []}
    context = _build_generation_context(request, project, context_extra)
    context.setdefault("overrides", context["requirements"])

    try:
        start_dt = datetime.strptime(start_date.strip(), "%Y-%m-%d")
        end_dt = datetime.strptime(end_date.strip(), "%Y-%m-%d")
    except ValueError:
        context["error"] = "תאריכים אינם בתוקף"
        return templates.TemplateResponse("admin_project_generate.html", context)

    if end_dt < start_dt:
        context["error"] = "תאריך הסיום חייב להיות אחרי תאריך ההתחלה"
        return templates.TemplateResponse("admin_project_generate.html", context)

    overrides = {
        "morning": _safe_positive_int(morning_override, context["requirements"]["morning"]),
        "afternoon": _safe_positive_int(afternoon_override, context["requirements"]["afternoon"]),
        "night": _safe_positive_int(night_override, context["requirements"]["night"]),
    }

    conn = get_connection()
    try:
        cur = conn.cursor()
        employees, constraints_map = _load_active_employees_with_constraints(cur)
        if not employees:
            context["error"] = "אין עובדים פעילים זמינים לשיבוץ"
            return templates.TemplateResponse("admin_project_generate.html", context)

        schedule_result = _generate_schedule_for_project(
            cur,
            project,
            employees,
            constraints_map,
            start_dt,
            end_dt,
            overrides,
            location.strip() or project["name"],
        )
        conn.commit()
    finally:
        conn.close()

    context["schedule_result"] = schedule_result
    context["generated_range"] = {
        "start": start_dt.date().isoformat(),
        "end": end_dt.date().isoformat(),
    }
    context["overrides"] = overrides
    context["message"] = f"נוצרו {schedule_result['total_assignments']} שיוכים חדשים"
    context["warnings"] = schedule_result.get("warnings", [])

    return templates.TemplateResponse("admin_project_generate.html", context)


@router.get("/projects/{project_id}/schedule.ics")
def project_schedule_calendar(
    request: Request,
    project_id: int,
    start_date: str,
    end_date: str,
):
    if (redirect := _require_admin(request)):
        return redirect
    project = _fetch_project_record(project_id)
    if not project:
        return _redirect("/admin/projects", error="הפרויקט לא נמצא")

    try:
        start_dt = datetime.strptime(start_date.strip(), "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date.strip(), "%Y-%m-%d").date()
    except ValueError:
        return _redirect("/admin/projects", error="טווח תאריכים אינו חוקי")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                s.date,
                s.start_time,
                s.end_time,
                IFNULL(s.location, '') AS location,
                e.name AS employee_name,
                p.name AS project_name
            FROM shifts s
            INNER JOIN projects p ON p.id = s.project_id
            LEFT JOIN ShiftAssignments sa ON sa.shift_id = s.id
            LEFT JOIN employees e ON e.id = sa.employee_id
            WHERE s.project_id = ?
              AND s.date BETWEEN ? AND ?
            ORDER BY s.date ASC, s.start_time ASC, e.name ASC
            """,
            (project_id, start_dt.isoformat(), end_dt.isoformat()),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//HDS Scheduler//EN",
    ]

    for idx, row in enumerate(rows):
        start_combined = datetime.strptime(f"{row['date']} {row['start_time']}", "%Y-%m-%d %H:%M")
        end_combined = datetime.strptime(f"{row['date']} {row['end_time']}", "%Y-%m-%d %H:%M")
        if end_combined <= start_combined:
            end_combined += timedelta(days=1)
        uid = f"sched-{project_id}-{idx}-{int(start_combined.timestamp())}@hds"
        summary = f"{row['project_name']} - {row['employee_name'] or 'לא שויך'}"
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{timestamp}",
                f"DTSTART:{start_combined.strftime('%Y%m%dT%H%M%S')}",
                f"DTEND:{end_combined.strftime('%Y%m%dT%H%M%S')}",
                f"SUMMARY:{summary}",
                f"LOCATION:{row['location'] or row['project_name']}",
                "END:VEVENT",
            ]
        )

    lines.append("END:VCALENDAR")
    content = "\r\n".join(lines)
    filename = f"project_{project_id}_{start_dt}_{end_dt}.ics"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return Response(content, media_type="text/calendar", headers=headers)


@router.get("/reports", response_class=HTMLResponse)
def admin_reports(
    request: Request,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    data = build_admin_report_data(request, start_date, end_date, request.query_params.getlist("project"))
    if isinstance(data, RedirectResponse):
        return data
    context = {"request": request}
    context.update(data)
    return templates.TemplateResponse("admin_reports.html", context)


@router.get("/overview", response_class=HTMLResponse)
def business_overview(
    request: Request,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if (redirect := _require_admin(request)):
        return redirect
    start_date = (start_date or "").strip() or None
    end_date = (end_date or "").strip() or None

    conn = get_connection()
    try:
        cur = conn.cursor()
        query = """
            SELECT
                s.id AS shift_id,
                s.date,
                s.start_time,
                s.end_time,
                s.location,
                p.id AS project_id,
                p.name AS project_name,
                p.hourly_rate,
                p.active AS project_active,
                sa.employee_id,
                e.name AS employee_name
            FROM shifts s
            LEFT JOIN projects p ON p.id = s.project_id
            LEFT JOIN ShiftAssignments sa ON sa.shift_id = s.id
            LEFT JOIN employees e ON e.id = sa.employee_id
            WHERE 1=1
        """
        params: List[Optional[str]] = []
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

    shift_hours: Dict[int, float] = {}
    project_metrics: Dict[int, Dict[str, float]] = {}
    employee_metrics: Dict[int, Dict[str, float]] = {}

    total_person_hours = 0.0
    total_payout = 0.0

    for row in rows:
        shift_id = row["shift_id"]
        if shift_id not in shift_hours:
            shift_hours[shift_id] = calculate_shift_hours(
                row["date"], row["start_time"], row["end_time"]
            )
        hours = shift_hours[shift_id]

        project_id = row["project_id"]
        project_entry = project_metrics.setdefault(
            project_id or -1,
            {
                "project_id": project_id,
                "project_name": row["project_name"] or "ללא פרויקט",
                "hours": 0.0,
                "amount": 0.0,
                "assignments": 0,
                "employee_ids": set(),  # type: ignore
                "active": bool(row["project_active"]) if project_id else False,
            },
        )

        employee_id = row["employee_id"]
        if employee_id:
            project_entry["assignments"] += 1
            cast_employees = project_entry["employee_ids"]  # type: ignore
            cast_employees.add(employee_id)
            project_entry["hours"] += hours
            amount = hours * (row["hourly_rate"] or 0)
            project_entry["amount"] += amount
            total_person_hours += hours
            total_payout += amount

            employee_entry = employee_metrics.setdefault(
                employee_id,
                {
                    "employee_id": employee_id,
                    "employee_name": row["employee_name"] or "ללא שם",
                    "hours": 0.0,
                    "amount": 0.0,
                    "assignments": 0,
                },
            )
            employee_entry["hours"] += hours
            employee_entry["amount"] += amount
            employee_entry["assignments"] += 1

    total_shifts = len(shift_hours)
    covered_shifts = sum(1 for entry in project_metrics.values() if entry["assignments"])

    project_overview = sorted(
        (
            {
                "project_id": entry["project_id"],
                "project_name": entry["project_name"],
                "hours": round(entry["hours"], 2),
                "amount": round(entry["amount"], 2),
                "assignments": entry["assignments"],
                "employee_count": len(entry["employee_ids"]),
                "active": entry["active"],
            }
            for entry in project_metrics.values()
        ),
        key=lambda item: item["hours"],
        reverse=True,
    )

    employee_overview = sorted(
        (
            {
                "employee_id": entry["employee_id"],
                "employee_name": entry["employee_name"],
                "hours": round(entry["hours"], 2),
                "amount": round(entry["amount"], 2),
                "assignments": entry["assignments"],
            }
            for entry in employee_metrics.values()
        ),
        key=lambda item: item["hours"],
        reverse=True,
    )

    return templates.TemplateResponse(
        "admin_overview.html",
        {
            "request": request,
            "start_date": start_date or "",
            "end_date": end_date or "",
            "total_shifts": total_shifts,
            "covered_shifts": covered_shifts,
            "total_person_hours": round(total_person_hours, 2),
            "total_payout": round(total_payout, 2),
            "project_overview": project_overview,
            "employee_overview": employee_overview,
        },
    )
