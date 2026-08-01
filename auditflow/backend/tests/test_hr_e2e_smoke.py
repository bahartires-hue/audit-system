from __future__ import annotations
import uuid, datetime as dt
from fastapi.testclient import TestClient
from app.auth_core import hash_password
from app.db import SessionLocal
from app.main import app
from app.models import User

CSRF_TOKEN = "test-csrf-token"

def _csrf_headers():
    return {"X-CSRF-Token": CSRF_TOKEN}

def _login():
    uname = f"hrtest_{uuid.uuid4().hex[:8]}"
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

def test_full_hr_flow():
    c = _login()
    h = {"X-CSRF-Token": c.cookies.get("auditflow_csrf", domain="testserver.local")}

    # 1) create employee w/ new 360 fields
    r = c.post("/api/hr/employees", json={
        "name": "أحمد الشمري", "nationality": "سعودي", "national_id": "1011223344",
        "position": "محاسب", "department": "المالية", "branch_name": "الرئيسي",
        "basic_salary": 8000, "hire_date": "2023-01-15",
        "gender": "male", "address": "الرياض", "bank_name": "الراجحي", "iban": "SA0000000000000000000000",
        "bank_salary": 8000,
    }, headers=h)
    assert r.status_code == 200, r.text
    emp = r.json()
    eid = emp["id"]
    assert emp["gender"] == "male" and emp["bank_name"] == "الراجحي"

    # 2) residency document (SSOT)
    r = c.post(f"/api/hr/employees/{eid}/documents", data={
        "doc_type": "إقامة", "doc_number": "1011223344", "expiry_date": "2027-01-01",
    }, headers=h)
    assert r.status_code == 200, r.text

    r = c.get(f"/api/hr/employees/{eid}", headers=h)
    assert r.status_code == 200
    assert r.json()["residency_expiry"] == "2027-01-01", "SSOT sync broken!"

    # 3) contract
    r = c.post(f"/api/hr/employees/{eid}/contracts", data={
        "contract_type": "دائم", "start_date": "2023-01-15", "salary": "8000", "annual_leave_days": "21",
    }, headers=h)
    assert r.status_code == 200, r.text

    # 4) advance
    r = c.post("/api/hr/advances", json={"employee_id": eid, "amount": 1000, "installments_count": 2, "date": "2026-07-01"}, headers=h)
    assert r.status_code == 200, r.text
    adv = r.json()
    assert adv["remaining_amount"] == 1000

    # 5) withdrawal (NEW)
    r = c.post("/api/hr/withdrawals", json={"employee_id": eid, "amount": 500, "reason": "طارئة", "date": "2026-07-15"}, headers=h)
    assert r.status_code == 200, r.text
    wid = r.json()["id"]
    assert r.json()["status"] == "pending"

    # 6) travel (NEW)
    r = c.post("/api/hr/travel", json={"employee_id": eid, "travel_type": "international", "destination": "دبي", "departure_date": "2026-07-01", "return_date": "2026-07-10", "purpose": "تدريب"}, headers=h)
    assert r.status_code == 200, r.text
    tid = r.json()["id"]
    assert r.json()["status"] in ("completed", "ongoing", "scheduled")

    r = c.post(f"/api/hr/travel/{tid}/return", headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "completed"

    # 7) custody (NEW)
    r = c.post("/api/hr/custody", json={"employee_id": eid, "item_name": "لابتوب Dell", "serial_number": "SN123", "value": 4000, "assigned_date": "2026-01-01"}, headers=h)
    assert r.status_code == 200, r.text
    cust_id = r.json()["id"]
    assert r.json()["status"] == "assigned"

    # 8) leave + decision
    r = c.post("/api/hr/leaves", json={"employee_id": eid, "leave_type": "سنوية", "start_date": "2026-07-01", "end_date": "2026-07-05", "paid": True}, headers=h)
    assert r.status_code == 200, r.text
    lid = r.json()["id"]
    r = c.post(f"/api/hr/leaves/{lid}/decision", json={"decision": "accepted"}, headers=h)
    assert r.status_code == 200, r.text

    # 9) deduction/allowance/commission
    c.post("/api/hr/deductions", json={"employee_id": eid, "amount": 100, "reason": "تأخير"}, headers=h)
    c.post("/api/hr/allowances", json={"employee_id": eid, "amount": 200, "allowance_type": "سكن", "recurring": False}, headers=h)
    c.post("/api/hr/commissions", json={"employee_id": eid, "sales_amount": 10000, "percentage": 5}, headers=h)

    # 10) payroll create — should consume advance installment + withdrawal + deduction + allowance + commission
    r = c.post("/api/hr/payrolls", json={"employee_id": eid, "month": 7, "year": 2026, "extra_allowances": [], "extra_deductions": []}, headers=h)
    assert r.status_code == 200, r.text
    payroll = r.json()
    pid = payroll["id"]

    # withdrawal should now be settled
    r = c.get("/api/hr/withdrawals", params={"employee_id": eid}, headers=h)
    w = [x for x in r.json()["items"] if x["id"] == wid][0]
    assert w["status"] == "settled", f"withdrawal not settled: {w}"
    assert w["applied"] is True

    r = c.post(f"/api/hr/payrolls/{pid}/pay", headers=h)
    assert r.status_code == 200, r.text

    # 11) profile-summary (Employee 360 aggregate)
    r = c.get(f"/api/hr/employees/{eid}/profile-summary", headers=h)
    assert r.status_code == 200, r.text
    summary = r.json()
    assert summary["quick_info"]["name"] == "أحمد الشمري"
    assert summary["quick_info"]["contract_status"] == "ساري"
    assert summary["stats"]["advances_total"] == 1000
    assert summary["stats"]["active_custody_count"] == 1
    assert summary["stats"]["travel_count"] == 1
    assert summary["quick_info"]["last_net_salary"] is not None

    # 12) timeline (NEW)
    r = c.get(f"/api/hr/employees/{eid}/timeline", headers=h)
    assert r.status_code == 200, r.text
    tl = r.json()["items"]
    actions = [x["action"] for x in tl]
    for expected in ["hr.employee.create", "hr.document.create", "hr.contract.create", "hr.advance.create",
                      "hr.withdrawal.create", "hr.travel.create", "hr.travel.return", "hr.custody.create",
                      "hr.leave.create", "hr.leave.decision", "hr.deduction.create", "hr.allowance.create",
                      "hr.commission.create", "hr.payroll.create", "hr.payroll.pay"]:
        assert expected in actions, f"missing timeline action: {expected}\ngot: {actions}"
    labels = {x["action"]: x["label"] for x in tl}
    assert "أحمد" not in labels["hr.employee.create"]  # generic label, not name-specific
    assert labels["hr.travel.return"] == "تم تسجيل العودة من السفر"

    # 13) salary change timeline
    r = c.patch(f"/api/hr/employees/{eid}", json={"basic_salary": 9000}, headers=h)
    assert r.status_code == 200, r.text
    r = c.get(f"/api/hr/employees/{eid}/timeline", headers=h)
    salary_events = [x for x in r.json()["items"] if x["action"] == "hr.employee.salary_change"]
    assert len(salary_events) == 1, salary_events
    assert "8000" in salary_events[0]["label"] and "9000" in salary_events[0]["label"]

    # 14) dashboard-summary new fields
    r = c.get("/api/hr/dashboard-summary", headers=h)
    assert r.status_code == 200, r.text
    ds = r.json()
    for key in ["passport_expired_count", "contracts_expired_count", "travel_active_count", "leaves_today_count",
                "advances_total", "withdrawals_total", "allowances_total", "deductions_total", "commissions_total", "eos_total"]:
        assert key in ds, f"missing dashboard key {key}"

    # 15) alerts feed new types
    r = c.get("/api/hr/alerts/feed", headers=h)
    assert r.status_code == 200, r.text
    types = {x["type"] for x in r.json()["items"]}
    assert "advance_active" in types, types

    # 16) admin view-only pages still list correctly (SSOT: public pages = view/filter only)
    r = c.get("/api/hr/withdrawals", headers=h)
    assert len(r.json()["items"]) == 1
    r = c.get("/api/hr/travel", headers=h)
    assert len(r.json()["items"]) == 1
    r = c.get("/api/hr/custody", headers=h)
    assert len(r.json()["items"]) == 1

    print("ALL HR E2E SMOKE ASSERTIONS PASSED")
