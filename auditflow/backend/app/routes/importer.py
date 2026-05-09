from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from ..auth_core import require_csrf, require_user
from ..db import SessionLocal
from ..services.importer import run_import_pipeline
from ..services.importer.scrape_jobs import complete_job, create_job, fail_job, get_job, update_job
from ..services.importer.universal_scraper import run_universal_import

router = APIRouter(prefix="/importer", tags=["importer"])


class ImporterRequest(BaseModel):
    site_url: str
    brand: str = ""
    size: str = ""
    limit: int = Field(default=500, ge=1, le=500)
    max_pages: int = Field(default=10, ge=1, le=500)
    multi_pages: bool = False


class UniversalImporterRequest(BaseModel):
    site_key: str
    category_url: str
    brand: str = ""
    max_pages: int = Field(default=10, ge=1, le=500)
    limit: int = Field(default=0, ge=0, le=500)


def _uploads_root() -> Path:
    data_root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
    app_root = Path(__file__).resolve().parents[3]  # auditflow/
    return (Path(data_root) / "uploads") if data_root else (app_root / "uploads")


def _attach_importer_previews(items: List[Any]) -> None:
    for x in items:
        p = (x.get("image_local") or "").strip()
        if p:
            fname = Path(p).name
            x["image_preview"] = f"/importer/image?name={quote(fname)}"
        else:
            x["image_preview"] = ""


@router.post("/scrape/start")
def scrape_importer_start(request: Request, body: ImporterRequest) -> Dict[str, Any]:
    require_csrf(request)
    db = SessionLocal()
    try:
        require_user(db, request)
    finally:
        db.close()

    site_url = (body.site_url or "").strip()
    if not site_url.startswith("http://") and not site_url.startswith("https://"):
        raise HTTPException(400, "أدخل رابطًا صحيحًا يبدأ بـ http:// أو https://")
    brand = (body.brand or "").strip()
    size = (body.size or "").strip()
    limit = int(body.limit or 500)
    max_pages = int(body.max_pages or 10)
    multi_pages = bool(body.multi_pages)
    path = (urlparse(site_url).path or "").strip("/")
    if not path and not brand and not size:
        raise HTTPException(400, "يرجى إدخال رابط صفحة ماركة أو تحديد ماركة/مقاس قبل السحب.")

    uploads = _uploads_root()
    job_id = create_job()

    def run() -> None:
        def cb(pct: int, msg: str) -> None:
            update_job(job_id, pct, msg)

        try:
            out = run_import_pipeline(
                site_url,
                uploads,
                brand=brand,
                size=size,
                limit=limit,
                max_pages=max_pages,
                multi_pages=multi_pages,
                progress_cb=cb,
            )
            items = out.get("items", [])
            _attach_importer_previews(items)
            complete_job(
                job_id,
                {
                    "ok": True,
                    "count": out.get("count", 0),
                    "csv_path": out.get("csv_path", ""),
                    "xlsx_path": out.get("xlsx_path", ""),
                    "salla_csv_path": out.get("salla_csv_path", ""),
                    "salla_xlsx_path": out.get("salla_xlsx_path", ""),
                    "items": items,
                },
            )
        except ValueError as e:
            fail_job(job_id, str(e))
        except Exception:
            fail_job(job_id, "فشل جلب البيانات من الموقع. تأكد من الرابط أو حاول لاحقًا.")

    threading.Thread(target=run, daemon=True).start()
    return {"ok": True, "job_id": job_id}


@router.get("/scrape/job/{job_id}")
def scrape_importer_job(request: Request, job_id: str) -> Dict[str, Any]:
    db = SessionLocal()
    try:
        require_user(db, request)
    finally:
        db.close()
    state = get_job(job_id)
    if not state:
        raise HTTPException(404, "مهمة غير موجودة أو انتهت صلاحيتها")
    return {"ok": True, **state}


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
    brand = (body.brand or "").strip()
    size = (body.size or "").strip()
    limit = int(body.limit or 500)
    max_pages = int(body.max_pages or 10)
    multi_pages = bool(body.multi_pages)
    path = (urlparse(site_url).path or "").strip("/")
    if not path and not brand and not size:
        raise HTTPException(400, "يرجى إدخال رابط صفحة ماركة أو تحديد ماركة/مقاس قبل السحب.")

    try:
        out = run_import_pipeline(
            site_url,
            _uploads_root(),
            brand=brand,
            size=size,
            limit=limit,
            max_pages=max_pages,
            multi_pages=multi_pages,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception:
        raise HTTPException(502, "فشل جلب البيانات من الموقع. تأكد من الرابط أو حاول لاحقًا.")
    items = out.get("items", [])
    _attach_importer_previews(items)
    return {
        "ok": True,
        "count": out.get("count", 0),
        "csv_path": out.get("csv_path", ""),
        "xlsx_path": out.get("xlsx_path", ""),
        "salla_csv_path": out.get("salla_csv_path", ""),
        "salla_xlsx_path": out.get("salla_xlsx_path", ""),
        "items": items,
    }


@router.post("/universal/scrape")
def scrape_importer_universal(request: Request, body: UniversalImporterRequest) -> Dict[str, Any]:
    require_csrf(request)
    db = SessionLocal()
    try:
        require_user(db, request)
    finally:
        db.close()

    category_url = (body.category_url or "").strip()
    site_key = (body.site_key or "").strip().lower()
    if not category_url.startswith("http://") and not category_url.startswith("https://"):
        raise HTTPException(400, "أدخل رابطًا صحيحًا يبدأ بـ http:// أو https://")
    if not site_key:
        raise HTTPException(400, "site_key مطلوب.")

    exports_root = _uploads_root().parent / "exports"
    try:
        out = run_universal_import(
            site_key=site_key,
            category_url=category_url,
            max_pages=int(body.max_pages or 10),
            limit=int(body.limit or 0),
            brand=(body.brand or "").strip(),
            exports_root=exports_root,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception:
        raise HTTPException(502, "فشل تشغيل السحب العام. تأكد من site_key والرابط.")

    return {
        "ok": True,
        "count": out.get("count", 0),
        "csv_path": out.get("csv_path", ""),
        "items": out.get("items", []),
    }


@router.get("/image")
def importer_image(name: str = Query(...)) -> FileResponse:
    safe = Path(name).name
    path = _uploads_root() / "products" / safe
    if not path.exists() or not path.is_file():
        raise HTTPException(404, "الصورة غير موجودة")
    return FileResponse(str(path))


@router.get("/csv")
def importer_csv(request: Request) -> FileResponse:
    db = SessionLocal()
    try:
        require_user(db, request)
    finally:
        db.close()
    csv_path = _uploads_root().parent / "exports" / "tire_products.csv"
    if not csv_path.exists() or not csv_path.is_file():
        raise HTTPException(404, "ملف CSV غير موجود. نفّذ السحب أولًا.")
    return FileResponse(str(csv_path), media_type="text/csv", filename="tire_products.csv")


@router.get("/xlsx")
def importer_xlsx(request: Request) -> FileResponse:
    db = SessionLocal()
    try:
        require_user(db, request)
    finally:
        db.close()
    xlsx_path = _uploads_root().parent / "exports" / "tire_products.xlsx"
    if not xlsx_path.exists() or not xlsx_path.is_file():
        raise HTTPException(404, "ملف Excel غير موجود. نفّذ السحب أولًا.")
    return FileResponse(
        str(xlsx_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="tire_products.xlsx",
    )


@router.get("/salla-xlsx")
def importer_salla_xlsx(request: Request) -> FileResponse:
    db = SessionLocal()
    try:
        require_user(db, request)
    finally:
        db.close()
    xlsx_path = _uploads_root().parent / "exports" / "salla_products_ready.xlsx"
    if not xlsx_path.exists() or not xlsx_path.is_file():
        raise HTTPException(404, "ملف Salla Excel غير موجود. نفّذ السحب أولًا.")
    return FileResponse(
        str(xlsx_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="salla_products_ready.xlsx",
    )


@router.get("/uploads-debug")
def importer_uploads_debug(request: Request) -> Dict[str, Any]:
    db = SessionLocal()
    try:
        require_user(db, request)
    finally:
        db.close()
    root = _uploads_root()
    products_dir = root / "products"
    products_dir.mkdir(parents=True, exist_ok=True)
    files = sorted([p for p in products_dir.iterdir() if p.is_file()], key=lambda x: x.stat().st_mtime, reverse=True)
    base = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    first5 = [p.name for p in files[:5]]
    return {
        "uploads_dir": str(root),
        "uploads_exists": root.exists(),
        "products_dir": str(products_dir),
        "products_exists": products_dir.exists(),
        "products_count": len(files),
        "first_5_images": first5,
        "first_5_public_urls": [f"{base}/uploads/products/{name}" if base else f"/uploads/products/{name}" for name in first5],
    }

