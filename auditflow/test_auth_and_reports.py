from fastapi.testclient import TestClient

from auditflow_single import app


def test_auth_me_returns_csrf():
    client = TestClient(app)
    r = client.get("/auth/me", headers={"Accept": "application/json"})
    assert r.status_code == 200
    data = r.json()
    assert "csrf_token" in data
    assert data["csrf_token"]


def test_register_login_flow():
    client = TestClient(app)
    me = client.get("/auth/me", headers={"Accept": "application/json"}).json()
    csrf = me["csrf_token"]
    payload = {"username": "test_user_1", "password": "12345678"}
    r = client.post("/auth/register", json=payload, headers={"X-CSRF-Token": csrf})
    assert r.status_code in (200, 400)
