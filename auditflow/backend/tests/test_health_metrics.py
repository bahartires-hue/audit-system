from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_healthz_ok():
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 200
    data = res.json()
    assert data["ok"] is True
    assert "time" in data


def test_metrics_ok():
    client = TestClient(app)
    res = client.get("/metrics")
    assert res.status_code == 200
    data = res.json()
    assert data["ok"] is True
    assert "users_total" in data
    assert "reports_total" in data
