from __future__ import annotations

import datetime as dt
import logging
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from sqlalchemy import desc

from ...db import SessionLocal
from ...models import ImporterSnapshot

log = logging.getLogger("importer.snapshot")


def _auto_label(kind: str, meta: Dict[str, Any]) -> str:
    url = (meta.get("site_url") or meta.get("category_url") or "").strip()
    host = urlparse(url).netloc.replace("www.", "") if url else ""
    if not host:
        host = "عام" if kind == "universal" else "—"
    if kind == "universal":
        sk = (meta.get("site_key") or "").strip()
        prefix = f"سحب عام ({sk})" if sk else "سحب عام"
    else:
        prefix = "استيراد إطارات"
    return f"{prefix} · {host} · {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"


def save_importer_snapshot(user_id: str, kind: str, request_meta: Dict[str, Any], result: Dict[str, Any]) -> Optional[str]:
    sid = uuid.uuid4().hex
    db = SessionLocal()
    try:
        db.add(
            ImporterSnapshot(
                id=sid,
                user_id=user_id,
                kind=kind,
                label=_auto_label(kind, request_meta),
                request_json=dict(request_meta),
                result_json=dict(result),
            )
        )
        db.commit()
        return sid
    except Exception:
        log.warning("save_importer_snapshot failed", exc_info=True)
        db.rollback()
        return None
    finally:
        db.close()


def list_importer_snapshots(user_id: str, limit: int = 40) -> List[Dict[str, Any]]:
    lim = max(1, min(100, int(limit or 40)))
    db = SessionLocal()
    try:
        rows = (
            db.query(ImporterSnapshot)
            .filter(ImporterSnapshot.user_id == user_id)
            .order_by(desc(ImporterSnapshot.created_at))
            .limit(lim)
            .all()
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            cnt = int((r.result_json or {}).get("count") or 0)
            req = r.request_json or {}
            url_preview = (req.get("site_url") or req.get("category_url") or "")[:160]
            out.append(
                {
                    "id": r.id,
                    "kind": r.kind,
                    "label": r.label or "",
                    "count": cnt,
                    "created_at": r.created_at.isoformat() if r.created_at else "",
                    "url_preview": url_preview,
                }
            )
        return out
    finally:
        db.close()


def get_importer_snapshot(user_id: str, snapshot_id: str) -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        r = (
            db.query(ImporterSnapshot)
            .filter(ImporterSnapshot.id == snapshot_id, ImporterSnapshot.user_id == user_id)
            .one_or_none()
        )
        if not r:
            return None
        return {
            "id": r.id,
            "kind": r.kind,
            "label": r.label,
            "request_json": r.request_json or {},
            "result_json": r.result_json or {},
            "created_at": r.created_at.isoformat() if r.created_at else "",
        }
    finally:
        db.close()


def delete_importer_snapshot(user_id: str, snapshot_id: str) -> bool:
    db = SessionLocal()
    try:
        r = (
            db.query(ImporterSnapshot)
            .filter(ImporterSnapshot.id == snapshot_id, ImporterSnapshot.user_id == user_id)
            .one_or_none()
        )
        if not r:
            return False
        db.delete(r)
        db.commit()
        return True
    except Exception:
        log.warning("delete_importer_snapshot failed", exc_info=True)
        db.rollback()
        return False
    finally:
        db.close()
