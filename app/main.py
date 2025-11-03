from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .db import init_db, get_connection

app = FastAPI(title="מערכת סידור עבודה - חברת אבטחה")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

init_db()  # יוודא שהטבלאות קיימות

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.id, e.name AS employee, p.name AS project, s.date, s.start_time, s.end_time
        FROM shifts s
        LEFT JOIN employees e ON s.employee_id = e.id
        LEFT JOIN projects p ON s.project_id = p.id
        ORDER BY s.date
    """)
    shifts = cur.fetchall()
    conn.close()
    return templates.TemplateResponse("schedule.html", {"request": request, "shifts": shifts})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)