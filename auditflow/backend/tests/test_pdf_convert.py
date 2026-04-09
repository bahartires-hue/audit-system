from __future__ import annotations

import io
import uuid

from fastapi.testclient import TestClient
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from app.auth_core import CSRF_COOKIE, SESSION_COOKIE, create_session, hash_password, issue_csrf_token
from app.db import SessionLocal
from app.main import app
from app.models import User


def _sample_pdf_bytes() -> bytes:
    buff = io.BytesIO()
    c = canvas.Canvas(buff, pagesize=A4)
    lines = [
        "التاريخ البيان مدين دائن",
        "04/01/2026 عليكم فاتورة مبيعات 15000.00 0",
        "05/01/2026 دفعه من الحساب 0 10000.00",
    ]
    y = 800
    for line in lines:
        c.drawString(60, y, line)
        y -= 20
    c.save()
    return buff.getvalue()


def _auth_client() -> TestClient:
    uname = f"pdf_user_{uuid.uuid4().hex[:8]}"
    db = SessionLocal()
    try:
        user = User(
            id=uuid.uuid4().hex,
            username=uname,
            email=f"{uname}@example.com",
            password_hash=hash_password("Pass123456"),
            is_active=1,
            plan_name="free",
            role_name="user",
        )
        db.add(user)
        db.commit()
        token = create_session(db, user.id)
    finally:
        db.close()

    client = TestClient(app)
    csrf = issue_csrf_token()
    client.cookies.set(SESSION_COOKIE, token)
    client.cookies.set(CSRF_COOKIE, csrf)
    client.headers.update({"X-CSRF-Token": csrf})
    return client


def test_convert_pdf_to_excel_ok():
    client = _auth_client()
    pdf_bytes = _sample_pdf_bytes()
    res = client.post(
        "/convert/pdf-to-excel",
        files={"file": ("sample.pdf", pdf_bytes, "application/pdf")},
    )
    assert res.status_code == 200
    assert "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in (res.headers.get("content-type") or "")
    assert len(res.content) > 100
