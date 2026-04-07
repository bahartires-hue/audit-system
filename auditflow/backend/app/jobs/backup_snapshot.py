from __future__ import annotations

import datetime as dt
import gzip
import json
import os
from pathlib import Path

from app.db import SessionLocal
from app.models import AnalysisReport, User


def _backup_dir() -> Path:
    root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
    if root:
        p = Path(root) / "backups"
    else:
        p = Path(__file__).resolve().parents[3] / "backups"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _snapshot_payload() -> dict:
    db = SessionLocal()
    try:
        users = db.query(User).order_by(User.created_at.desc()).all()
        reports = db.query(AnalysisReport).order_by(AnalysisReport.created_at.desc()).limit(2000).all()
        return {
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
            "users": [
                {
                    "id": u.id,
                    "username": u.username,
                    "email": u.email,
                    "is_admin": int(u.is_admin or 0),
                    "is_active": int(u.is_active or 0),
                    "plan_name": u.plan_name,
                    "subscription_expires_at": u.subscription_expires_at.isoformat() + "Z" if u.subscription_expires_at else None,
                    "created_at": u.created_at.isoformat() + "Z" if u.created_at else None,
                }
                for u in users
            ],
            "reports": [
                {
                    "id": r.id,
                    "user_id": r.user_id,
                    "title": r.title,
                    "branch1_name": r.branch1_name,
                    "branch2_name": r.branch2_name,
                    "status": r.status,
                    "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
                    "total_ops": r.total_ops,
                    "matched_ops": r.matched_ops,
                    "mismatch_ops": r.mismatch_ops,
                    "errors_count": r.errors_count,
                    "warnings_count": r.warnings_count,
                    "archived": int(r.archived or 0),
                }
                for r in reports
            ],
        }
    finally:
        db.close()


def main() -> None:
    payload = _snapshot_payload()
    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    out = _backup_dir() / f"auditflow-snapshot-{ts}.json.gz"
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    with gzip.open(out, "wb") as gz:
        gz.write(raw)
    # Keep recent files only.
    keep = max(3, int((os.getenv("AUDITFLOW_BACKUP_KEEP") or "14").strip()))
    files = sorted(_backup_dir().glob("auditflow-snapshot-*.json.gz"))
    for f in files[:-keep]:
        try:
            f.unlink()
        except Exception:
            pass
    print(f"backup written: {out}")


if __name__ == "__main__":
    main()
