from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from ..auth_core import require_csrf, require_user
from ..db import SessionLocal
from ..services.importer import run_import_pipeline

router = APIRouter(prefix="/importer", tags=["importer"])


class ImporterRequest(BaseModel):
    site_url: str


def _uploads_root() -> Path:
    data_root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
    app_root = Path(__file__).resolve().parents[3]  # auditflow/
    return (Path(data_root) / "uploads") if data_root else (app_root / "uploads")


@router.post("/scrape")
def scrape_importer(request: Request, body: ImporterRequest) -> Dict[str, Any]:
    require_csrf(request)
    db = SessionLocal()
    try:
        require_user(db, request)
    finally:
        db.close()

    site_url = (body.site_url or "").strip()
    if not site_url.startswith("http://") and not site_url.startswith("https://"):
        raise HTTPException(400, "أدخل رابطًا صحيحًا يبدأ بـ http:// أو https://")

    try:
        out = run_import_pipeline(site_url, _uploads_root())
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception:
        raise HTTPException(502, "فشل جلب البيانات من الموقع. تأكد من الرابط أو حاول لاحقًا.")
    items = out.get("items", [])
    for x in items:
        p = (x.get("image_local") or "").strip()
        if p:
            fname = Path(p).name
            x["image_preview"] = f"/importer/image?name={quote(fname)}"
        else:
            x["image_preview"] = ""
    return {
        "ok": True,
        "count": out.get("count", 0),
        "csv_path": out.get("csv_path", ""),
        "items": items,
    }


@router.get("/image")
def importer_image(name: str = Query(...)) -> FileResponse:
    safe = Path(name).name
    path = _uploads_root() / "products" / safe
    if not path.exists() or not path.is_file():
        raise HTTPException(404, "الصورة غير موجودة")
    return FileResponse(str(path))

