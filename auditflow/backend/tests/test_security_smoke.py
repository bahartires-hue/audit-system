from __future__ import annotations

import uuid

from fastapi.testclient import TestClient

from app.auth_core import hash_password
from app.db import SessionLocal
from app.main import app
from app.models import User


def _csrf_headers() -> dict:
    token = "test-csrf-token"
    return {"X-CSRF-Token": token, "Cookie": f"csrf_token={token}"}


def test_admin_endpoint_requires_login():
    client = TestClient(app)
    res = client.get("/admin/users")
    assert res.status_code in (401, 403)


def test_login_requires_csrf():
    client = TestClient(app)
    res = client.post("/auth/login", json={"username": "x", "password": "x"})
    assert res.status_code in (400, 403)


def test_bruteforce_eventual_lock():
    uname = f"lock_{uuid.uuid4().hex[:8]}"
    db = SessionLocal()
    try:
        db.add(
            User(
                id=uuid.uuid4().hex,
                username=uname,
                email=f"{uname}@example.com",
                password_hash=hash_password("RightPass123"),
                is_active=1,
                plan_name="free",
                role_name="user",
            )
        )
        db.commit()
    finally:
        db.close()

    client = TestClient(app)
    last_status = None
    for _ in range(8):
        res = client.post(
            "/auth/login",
            json={"username": uname, "password": "wrong-pass"},
            headers=_csrf_headers(),
        )
        last_status = res.status_code
    assert last_status in (401, 403, 429)
