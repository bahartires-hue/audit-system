from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

from .auth_core import require_csrf, require_user
from .db import SessionLocal as _SessionLocal
from .models import AnalysisReport, init_db
from .routers.auth_api import router as auth_router
from .services.analyzer import analyze as analyze_pairs
from .services.analyzer import compute_summary, process
from .services.reports import mismatches_to_csv_bytes, mismatches_to_excel_bytes, mismatches_to_pdf_bytes
from .services.storage import save_upload_file

app = FastAPI(title="AuditFlow API")

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def ui_cache_headers(request: Request, call_next):
    """يمنع احتجاز نسخ قديمة من الواجهة."""
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        response.headers["Pragma"] = "no-cache"
    elif path in ("/", "/analyze", "/settings", "/login", "/reports") or path.startswith("/report"):
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
    finally:
        db.close()

    return {"reportId": report_id}


@app.get("/reports")
def list_reports(request: Request):
    if _wants_html(request):
        return _require_login_page(request, FRONTEND_DIR / "reports.html")

    db = _SessionLocal()
    try:
        user = require_user(db, request)
        rows: List[AnalysisReport] = (
            db.query(AnalysisReport)
            .filter(AnalysisReport.user_id == user.id)
            .order_by(AnalysisReport.created_at.desc())
            .limit(200)
            .all()
        )
        items = []
        for r in rows:
            items.append(
                {
                    "id": r.id,
                    "title": r.title,
                    "branch1_name": r.branch1_name,
                    "branch2_name": r.branch2_name,
                    "status": r.status,
                    "created_at": r.created_at,
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

        return {
            "id": r.id,
            "title": r.title,
            "branch1_name": r.branch1_name,
            "branch2_name": r.branch2_name,
            "status": r.status,
            "created_at": r.created_at,
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
