from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from .db import SessionLocal as _SessionLocal  # type: ignore
from .models import AnalysisReport, init_db
from .services.analyzer import analyze as analyze_pairs
from .services.analyzer import compute_summary, process
from .services.reports import mismatches_to_csv_bytes
from .services.storage import save_upload_file

# NOTE: relative imports simplified below

app = FastAPI(title="AuditFlow API")

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


BASE_DIR = Path(__file__).resolve().parents[2]  # auditflow/backend
UPLOAD_DIR = BASE_DIR / "uploads"
FRONTEND_DIR = BASE_DIR / "frontend"

app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

init_db()


def _wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "").lower()
    if "application/json" in accept:
        return False
    # Browsers usually send text/html, but some clients may send */*.
    return ("text/html" in accept) or (accept == "" or "*/*" in accept)


@app.get("/", response_class=HTMLResponse)
def ui_home():
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.get("/analyze", response_class=HTMLResponse)
def ui_analyze(request: Request):
    # GET /analyze is the UI route (POST /analyze is the API)
    return FileResponse(str(FRONTEND_DIR / "analyze.html"))


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
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    b1: str = Form(...),
    b2: str = Form(...),
    title: Optional[str] = Form(None),
    strict_mirror_types: bool = Form(False),
):
    # store uploads
    report_id = uuid.uuid4().hex
    saved1, original1 = save_upload_file(file1, UPLOAD_DIR / report_id / "file1")
    saved2, original2 = save_upload_file(file2, UPLOAD_DIR / report_id / "file2")

    # analyze locally
    try:
        d1 = process(saved1, original1, b1)
        d2 = process(saved2, original2, b2)
        mismatch_entries, counts = analyze_pairs(d1, d2, allow_same_direction=not strict_mirror_types)
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
        return FileResponse(str(FRONTEND_DIR / "reports.html"))

    db = _SessionLocal()
    try:
        rows: List[AnalysisReport] = (
            db.query(AnalysisReport).order_by(AnalysisReport.created_at.desc()).limit(200).all()
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
        # report.html uses JS to fetch JSON via the same endpoint
        return FileResponse(str(FRONTEND_DIR / "report.html"))

    db = _SessionLocal()
    try:
        r: AnalysisReport | None = db.query(AnalysisReport).filter(AnalysisReport.id == id).first()
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
def download_report(id: str = Query(...)):
    db = _SessionLocal()
    try:
        r: AnalysisReport | None = db.query(AnalysisReport).filter(AnalysisReport.id == id).first()
        if not r:
            raise HTTPException(404, "Report not found")

        mismatches = (r.analysis_json or {}).get("mismatches", []) or []
        csv_bytes = mismatches_to_csv_bytes(mismatches)
        filename = f"report_{r.id}.csv"

        return Response(
            content=csv_bytes,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    finally:
        db.close()


@app.delete("/reports")
def delete_reports(id: str = Query(...)):
    db = _SessionLocal()
    try:
        r: AnalysisReport | None = db.query(AnalysisReport).filter(AnalysisReport.id == id).first()
        if not r:
            raise HTTPException(404, "Report not found")

        db.delete(r)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()

