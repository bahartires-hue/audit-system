from __future__ import annotations
import uuid
from fastapi.testclient import TestClient
from app.auth_core import hash_password
from app.db import SessionLocal
from app.main import app
from app.models import User

CSRF_TOKEN = "test-csrf-token-attendance"


def _csrf_headers():
    return {"X-CSRF-Token": CSRF_TOKEN}


def _login():
    uname = f"hratt_{uuid.uuid4().hex[:8]}"
    db = SessionLocal()
    try:
        db.add(User(id=uuid.uuid4().hex, username=uname, email=f"{uname}@example.com",
                     password_hash=hash_password("RightPass123"), is_active=1, plan_name="free", role_name="admin", is_admin=1))
        db.commit()
    finally:
        db.close()
    client = TestClient(app, base_url="https://testserver")
    client.cookies.set("auditflow_csrf", CSRF_TOKEN)
    res = client.post("/auth/login", json={"username": uname, "password": "RightPass123"}, headers=_csrf_headers())
    assert res.status_code == 200, res.text
    return client


def test_attendance_early_leave_and_monthly_calendar_data():
    c = _login()
    h = {"X-CSRF-Token": c.cookies.get("auditflow_csrf", domain="testserver.local")}

    r = c.post("/api/hr/employees", json={"name": "تقويم اختبار", "basic_salary": 6000}, headers=h)
    assert r.status_code == 200, r.text
    emp_id = r.json()["id"]

    r1 = c.post("/api/hr/attendance", json={
        "employee_id": emp_id, "date": "2026-05-05", "check_in": "08:00", "check_out": "17:00",
    }, headers=h)
    assert r1.status_code == 200, r1.text
    assert r1.json()["is_early_leave"] is False

    r2 = c.post("/api/hr/attendance", json={
        "employee_id": emp_id, "date": "2026-05-06", "check_in": "08:00", "check_out": "13:00",
        "is_early_leave": True,
    }, headers=h)
    assert r2.status_code == 200, r2.text
    assert r2.json()["is_early_leave"] is True

    r3 = c.post("/api/hr/attendance", json={
        "employee_id": emp_id, "date": "2026-05-07", "is_absent": True,
    }, headers=h)
    assert r3.status_code == 200, r3.text

    r4 = c.post("/api/hr/attendance", json={
        "employee_id": emp_id, "date": "2026-06-01", "check_in": "08:00", "check_out": "17:00",
    }, headers=h)
    assert r4.status_code == 200, r4.text

    # month-scoped fetch used by the calendar: only May 2026 rows
    lst = c.get(f"/api/hr/attendance?employee_id={emp_id}&month=5&year=2026", headers=h)
    assert lst.status_code == 200
    items = lst.json()["items"]
    assert len(items) == 3
    by_date = {i["date"]: i for i in items}
    assert by_date["2026-05-05"]["is_early_leave"] is False
    assert by_date["2026-05-06"]["is_early_leave"] is True
    assert by_date["2026-05-07"]["is_absent"] is True

    # June row must NOT leak into May's scoped fetch
    assert "2026-06-01" not in by_date

    page = c.get(f"/hr/employee?id={emp_id}&tab=attendance")
    assert page.status_code == 200
    body = page.text
    for marker in ["attCalGrid", "attCalPrevBtn", "attCalNextBtn", "attEarlyLeave", "renderAttendanceCalendar"]:
        assert marker in body, marker
