from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from app.db import init_db, get_connection
from app.routes import employee  # מודול הנתיבים של העובד

# יצירת האפליקציה הראשית
app = FastAPI(title="מערכת סידור עבודה - חברת אבטחה")

# הגדרת סשנים (בשביל לשמור מידע על העובד המחובר)
app.add_middleware(SessionMiddleware, secret_key="supersecretkey")

# חיבור תקיות סטטיות ותבניות
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
app.state.templates = templates

# ייבוא מסד הנתונים והקמת הטבלאות אם לא קיימות
init_db()

# חיבור הנתיבים של העובדים
app.include_router(employee.router)

# דף הבית – מציג את סידור העבודה הכללי
@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            s.id,
            COALESCE(GROUP_CONCAT(e.name, ', '), '-') AS employees,
            p.name AS project,
            s.location,
            s.date,
            s.start_time,
            s.end_time
        FROM shifts s
        LEFT JOIN projects p ON s.project_id = p.id
        LEFT JOIN ShiftAssignments sa ON sa.shift_id = s.id
        LEFT JOIN employees e ON sa.employee_id = e.id
        GROUP BY s.id, p.name, s.location, s.date, s.start_time, s.end_time
        ORDER BY s.date, s.start_time
    """)
    shifts = cur.fetchall()
    conn.close()
    return templates.TemplateResponse(
        "schedule.html",
        {"request": request, "shifts": shifts}
    )


@app.get("/reports", response_class=HTMLResponse)
def reports(request: Request):
    return templates.TemplateResponse(
        "reports.html",
        {"request": request}
    )

# מאפשר להריץ ישירות עם python app/main.py
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
