from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from .auth_core import log_event, require_csrf, require_user
from .db import SessionLocal as _SessionLocal
from .models import AnalysisReport, init_db
from .rate_limit import limiter
from .routers.auth_api import router as auth_router
from .services.analyzer import analyze as analyze_pairs
from .services.analyzer import compute_summary, process
from .services.reports import mismatches_to_csv_bytes, mismatches_to_excel_bytes, mismatches_to_pdf_bytes
from .services.storage import save_upload_file

app = FastAPI(title="OptimalMatch API | التطابق الأمثل")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SlowAPIMiddleware)


@app.middleware("http")
async def ui_cache_headers(request: Request, call_next):
    """يمنع احتجاز نسخ قديمة من الواجهة."""
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        response.headers["Pragma"] = "no-cache"
    elif path in ("/", "/analyze", "/settings", "/login", "/reports", "/help") or path.startswith("/report"):
        response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        response.headers["Pragma"] = "no-cache"
    return response


BASE_DIR = Path(__file__).resolve().parents[2]
_data_root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
UPLOAD_DIR = (Path(_data_root) / "uploads") if _data_root else (BASE_DIR / "uploads")
FRONTEND_DIR = BASE_DIR / "frontend"

app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
app.include_router(auth_router)

init_db()


def _wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "").lower()
    if "application/json" in accept:
        return False
    return ("text/html" in accept) or (accept == "" or "*/*" in accept)


def _require_login_page(request: Request, html_path: Path) -> FileResponse | RedirectResponse:
    db = _SessionLocal()
    try:
        require_user(db, request)
        return FileResponse(str(html_path))
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)
    finally:
        db.close()


@app.get("/", response_class=HTMLResponse)
def ui_home(request: Request):
    return _require_login_page(request, FRONTEND_DIR / "index.html")


@app.get("/analyze", response_class=HTMLResponse)
def ui_analyze(request: Request):
    return _require_login_page(request, FRONTEND_DIR / "analyze.html")


@app.get("/settings", response_class=HTMLResponse)
def ui_settings(request: Request):
    return _require_login_page(request, FRONTEND_DIR / "settings.html")


@app.get("/help", response_class=HTMLResponse)
def ui_help(request: Request):
    return _require_login_page(request, FRONTEND_DIR / "help.html")


@app.get("/login", response_class=HTMLResponse)
def ui_login(request: Request):
    from .auth_core import current_user_from_request

    db = _SessionLocal()
    try:
        u = current_user_from_request(db, request)
        if u:
            return RedirectResponse(url="/", status_code=302)
        return FileResponse(str(FRONTEND_DIR / "login.html"))
    finally:
        db.close()


def _classify_reason(entry: Dict[str, Any]) -> str:
    reason = entry.get("reason") or ""
    if entry.get("type") == "error":
        return "error"
    if "⚠️" in reason:
        return "warning"
    if "❌" in reason:
        return "error"
    if "لا يوجد مقابل" in reason:
        return "error"
    return "mismatch"


def _compute_entry_counts(entries: List[Dict[str, Any]]) -> Dict[str, int]:
    errors = sum(1 for e in entries if _classify_reason(e) == "error")
    warnings = sum(1 for e in entries if _classify_reason(e) == "warning")
    return {"errors": errors, "warnings": warnings}


@app.post("/analyze")
def analyze_api(
    request: Request,
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    b1: str = Form(...),
    b2: str = Form(...),
    title: Optional[str] = Form(None),
    strict_mirror_types: bool = Form(False),
):
    db = _SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        user_id = user.id
    finally:
        db.close()

    report_id = uuid.uuid4().hex
    try:
        saved1, original1 = save_upload_file(file1, UPLOAD_DIR / report_id / "file1")
        saved2, original2 = save_upload_file(file2, UPLOAD_DIR / report_id / "file2")
    except ValueError as e:
        raise HTTPException(400, str(e))

    try:
        d1 = process(saved1, original1, b1)
        d2 = process(saved2, original2, b2)
        mismatch_entries, counts = analyze_pairs(
            d1, d2, allow_same_direction=not strict_mirror_types
        )
    except HTTPException:
        raise
    except Exception as e:
        msg = str(e).strip() or e.__class__.__name__
        raise HTTPException(400, f"تعذّر تحليل الملفات: {msg}")

    summary = compute_summary(d1, d2, mismatch_entries)
    entry_counts = _compute_entry_counts(mismatch_entries)
    title_eff = (title or "").strip() or f"{b1.strip()} مقابل {b2.strip()}"

    created = AnalysisReport(
        id=report_id,
        user_id=user_id,
        title=title_eff,
        branch1_name=b1,
        branch2_name=b2,
        file1_original=original1,
        file2_original=original2,
        file1_path=saved1,
        file2_path=saved2,
        total_ops=summary["total_ops"],
        matched_ops=summary["matched_ops"],
        mismatch_ops=summary["mismatch_ops"],
        errors_count=entry_counts["errors"],
        warnings_count=entry_counts["warnings"],
        stats_json={
            "counts": counts,
            "branch1_total": len(d1),
            "branch2_total": len(d2),
        },
        analysis_json={
            "extracted_branch1": d1,
            "extracted_branch2": d2,
            "mismatches": mismatch_entries,
            "counts": counts,
        },
    )

    db = _SessionLocal()
    try:
        db.add(created)
        db.commit()
        log_event(db, "report.created", user_id, {"report_id": report_id, "title": title_eff})
    finally:
        db.close()

    return {"reportId": report_id}


@app.get("/reports")
def list_reports(
    request: Request,
    archived: str = Query("0"),
    q: str = Query(""),
):
    if _wants_html(request):
        return _require_login_page(request, FRONTEND_DIR / "reports.html")

    db = _SessionLocal()
    try:
        user = require_user(db, request)
        query = db.query(AnalysisReport).filter(AnalysisReport.user_id == user.id)
        ar = (archived or "0").strip().lower()
        if ar in ("0", "active", "false"):
            query = query.filter((AnalysisReport.archived.is_(None)) | (AnalysisReport.archived == 0))
        elif ar in ("1", "true", "archived"):
            query = query.filter(AnalysisReport.archived == 1)
        rows: List[AnalysisReport] = query.order_by(AnalysisReport.created_at.desc()).limit(500).all()
        qn = (q or "").strip().lower()
        items = []
        for r in rows:
            tags = r.tags_json if isinstance(r.tags_json, list) else []
            tag_str = " ".join(str(t) for t in tags).lower()
            title_l = (r.title or "").lower()
            b1 = (r.branch1_name or "").lower()
            b2 = (r.branch2_name or "").lower()
            if qn and qn not in title_l and qn not in b1 and qn not in b2 and qn not in tag_str:
                continue
            items.append(
                {
                    "id": r.id,
                    "title": r.title,
                    "branch1_name": r.branch1_name,
                    "branch2_name": r.branch2_name,
                    "status": r.status,
                    "created_at": r.created_at,
                    "tags": tags,
                    "archived": bool(int(r.archived or 0)),
                    "stats": {
                        "total_ops": r.total_ops,
                        "matched_ops": r.matched_ops,
                        "mismatch_ops": r.mismatch_ops,
                        "errors_count": r.errors_count,
                        "warnings_count": r.warnings_count,
                    },
                }
            )
        return {"items": items}
    finally:
        db.close()


@app.get("/report")
def get_report(request: Request, id: str = Query(...)):
    if _wants_html(request):
        return _require_login_page(request, FRONTEND_DIR / "report.html")

    db = _SessionLocal()
    try:
        user = require_user(db, request)
        r: AnalysisReport | None = (
            db.query(AnalysisReport)
            .filter(AnalysisReport.id == id, AnalysisReport.user_id == user.id)
            .first()
        )
        if not r:
            raise HTTPException(404, "Report not found")

        tags = r.tags_json if isinstance(r.tags_json, list) else []
        return {
            "id": r.id,
            "title": r.title,
            "branch1_name": r.branch1_name,
            "branch2_name": r.branch2_name,
            "status": r.status,
            "created_at": r.created_at,
            "tags": tags,
            "notes": r.notes or "",
            "archived": bool(int(r.archived or 0)),
            "stats": {
                "total_ops": r.total_ops,
                "matched_ops": r.matched_ops,
                "mismatch_ops": r.mismatch_ops,
                "errors_count": r.errors_count,
                "warnings_count": r.warnings_count,
            },
            "file1_original": r.file1_original,
            "file2_original": r.file2_original,
            "stats_json": r.stats_json,
            "analysis_json": r.analysis_json,
        }
    finally:
        db.close()


@app.patch("/report")
async def patch_report(
    request: Request,
    report_id: str = Query(..., alias="id"),
    body: Dict[str, Any] = Body(...),
):
    db = _SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        r: AnalysisReport | None = (
            db.query(AnalysisReport)
            .filter(AnalysisReport.id == report_id, AnalysisReport.user_id == user.id)
            .first()
        )
        if not r:
            raise HTTPException(404, "Report not found")

        if "title" in body and body["title"] is not None:
            r.title = str(body["title"]).strip() or r.title
        if "notes" in body:
            r.notes = str(body["notes"] or "") or None
        if "archived" in body:
            r.archived = 1 if bool(body["archived"]) else 0
        if "tags" in body:
            raw = body["tags"]
            if isinstance(raw, str):
                tags = [x.strip() for x in raw.split(",") if x.strip()]
            elif isinstance(raw, list):
                tags = [str(x).strip() for x in raw if str(x).strip()]
            else:
                raise HTTPException(400, "tags يجب أن تكون قائمة أو نصاً مفصولاً بفواصل")
            r.tags_json = tags[:50]

        db.commit()
        log_event(db, "report.updated", user.id, {"report_id": report_id})
        tags = r.tags_json if isinstance(r.tags_json, list) else []
        return {
            "id": r.id,
            "title": r.title,
            "tags": tags,
            "notes": r.notes or "",
            "archived": bool(int(r.archived or 0)),
        }
    finally:
        db.close()


@app.get("/download")
def download_report(request: Request, id: str = Query(...), format: str = Query("csv")):
    db = _SessionLocal()
    try:
        user = require_user(db, request)
        r: AnalysisReport | None = (
            db.query(AnalysisReport)
            .filter(AnalysisReport.id == id, AnalysisReport.user_id == user.id)
            .first()
        )
        if not r:
            raise HTTPException(404, "Report not found")

        mismatches = (r.analysis_json or {}).get("mismatches", []) or []
        fmt = (format or "csv").lower().strip()

        if fmt in ("excel", "xlsx"):
            excel_bytes = mismatches_to_excel_bytes(mismatches)
            filename = f"report_{r.id}.xlsx"
            return Response(
                content=excel_bytes,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

        if fmt == "pdf":
            try:
                pdf_bytes = mismatches_to_pdf_bytes(mismatches)
            except RuntimeError as e:
                raise HTTPException(500, str(e))
            filename = f"report_{r.id}.pdf"
            return Response(
                content=pdf_bytes,
                media_type="application/pdf",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

        if fmt == "csv":
            csv_bytes = mismatches_to_csv_bytes(mismatches)
            filename = f"report_{r.id}.csv"
            return Response(
                content=csv_bytes,
                media_type="text/csv; charset=utf-8",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

        raise HTTPException(400, "format يجب أن يكون: excel أو pdf أو csv")
    finally:
        db.close()


@app.delete("/reports")
def delete_reports(request: Request, id: str = Query(...)):
    db = _SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        r: AnalysisReport | None = (
            db.query(AnalysisReport)
            .filter(AnalysisReport.id == id, AnalysisReport.user_id == user.id)
            .first()
        )
        if not r:
            raise HTTPException(404, "Report not found")

        db.delete(r)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()
