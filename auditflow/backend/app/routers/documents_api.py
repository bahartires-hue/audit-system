from __future__ import annotations

import datetime as dt
import os
import uuid
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse

from ..auth_core import log_event, require_csrf, require_user, user_can_access_page_key
from ..db import SessionLocal
from ..models import Document, User

router = APIRouter(prefix="/api/documents", tags=["documents"])

DOC_TYPES = {"invoice", "contract", "file", "attachment"}
DOC_TYPE_LABELS_AR = {
    "invoice": "فاتورة",
    "contract": "عقد",
    "file": "ملف",
    "attachment": "مرفق",
}
MAX_DOC_MB = int(os.environ.get("AUDITFLOW_MAX_DOC_MB", "25"))
_ALLOWED_SUFFIXES = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip", ".txt",
)

# نفس منطق حساب مجلد الرفع في main.py (auditflow/uploads أو AUDITFLOW_DATA_ROOT/uploads)،
# محسوب محليًا هنا لتفادي استيراد دائري مع main.py.
BASE_DIR = Path(__file__).resolve().parents[3]
_data_root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
UPLOAD_DIR = (Path(_data_root) / "uploads") if _data_root else (BASE_DIR / "uploads")
DOCS_DIR = UPLOAD_DIR / "documents"
DOCS_DIR.mkdir(parents=True, exist_ok=True)


def _require_doc_access(db, request: Request):
    user = require_user(db, request)
    if not user_can_access_page_key(user, "op-suppliers"):
        raise HTTPException(403, "ليس لديك صلاحية للوصول لإدارة المستندات")
    return user


def _attachment_headers(download_name: str) -> dict:
    raw = (download_name or "download").strip() or "download"
    ext = Path(raw).suffix
    ascii_fallback = f"download{ext}" if ext else "download.bin"
    return {
        "Content-Disposition": f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{quote(raw, safe='')}"
    }


@router.post("")
async def upload_document(
    request: Request,
    file: UploadFile = File(...),
    doc_type: str = Form("file"),
    title: str = Form(""),
    notes: str = Form(""),
):
    db = SessionLocal()
    try:
        user = _require_doc_access(db, request)
        require_csrf(request)
        dt_key = (doc_type or "file").strip().lower()
        if dt_key not in DOC_TYPES:
            raise HTTPException(400, "نوع المستند غير مدعوم")
        original = file.filename or "مستند"
        suffix = Path(original).suffix.lower()
        if suffix not in _ALLOWED_SUFFIXES:
            raise HTTPException(400, "نوع الملف غير مدعوم")
        content = await file.read()
        if not content:
            raise HTTPException(400, "ملف فارغ")
        max_bytes = MAX_DOC_MB * 1024 * 1024
        if len(content) > max_bytes:
            raise HTTPException(400, f"حجم الملف أكبر من {MAX_DOC_MB} ميجابايت")
        saved_name = f"{uuid.uuid4().hex}{suffix}"
        saved_path = DOCS_DIR / saved_name
        with open(saved_path, "wb") as f:
            f.write(content)

        doc = Document(
            id=uuid.uuid4().hex,
            user_id=user.id,
            doc_type=dt_key,
            title=(title or original).strip()[:200] or original,
            original_filename=original,
            stored_filename=saved_name,
            size_bytes=len(content),
            notes=(notes or "").strip()[:2000],
        )
        db.add(doc)
        db.commit()
        log_event(db, "documents.upload", user.id, {"doc_id": doc.id, "doc_type": dt_key, "filename": original})
        return {
            "id": doc.id,
            "doc_type": doc.doc_type,
            "doc_type_label": DOC_TYPE_LABELS_AR.get(doc.doc_type, doc.doc_type),
            "title": doc.title,
            "original_filename": doc.original_filename,
            "size_bytes": doc.size_bytes,
            "created_at": doc.created_at.isoformat() + "Z",
        }
    finally:
        db.close()


@router.get("")
def list_documents(
    request: Request,
    doc_type: str = Query(""),
    q: str = Query(""),
    limit: int = Query(200),
):
    db = SessionLocal()
    try:
        _require_doc_access(db, request)
        lim = max(1, min(int(limit or 200), 1000))
        query = db.query(Document)
        dt_key = (doc_type or "").strip().lower()
        if dt_key:
            if dt_key not in DOC_TYPES:
                raise HTTPException(400, "نوع المستند غير مدعوم")
            query = query.filter(Document.doc_type == dt_key)
        rows = query.order_by(Document.created_at.desc()).limit(lim).all()
        qn = (q or "").strip().lower()
        user_ids = {r.user_id for r in rows}
        users = {}
        if user_ids:
            for u in db.query(User).filter(User.id.in_(user_ids)).all():
                users[u.id] = u.username
        items = []
        for d in rows:
            if qn and qn not in (d.title or "").lower() and qn not in (d.original_filename or "").lower():
                continue
            items.append(
                {
                    "id": d.id,
                    "doc_type": d.doc_type,
                    "doc_type_label": DOC_TYPE_LABELS_AR.get(d.doc_type, d.doc_type),
                    "title": d.title,
                    "original_filename": d.original_filename,
                    "size_bytes": d.size_bytes,
                    "notes": d.notes or "",
                    "uploaded_by": users.get(d.user_id, "-"),
                    "created_at": d.created_at.isoformat() + "Z" if d.created_at else None,
                }
            )
        return {"items": items, "types": [{"key": k, "label": v} for k, v in DOC_TYPE_LABELS_AR.items()]}
    finally:
        db.close()


@router.get("/{doc_id}/download")
def download_document(doc_id: str, request: Request):
    db = SessionLocal()
    try:
        _require_doc_access(db, request)
        d = db.query(Document).filter(Document.id == doc_id).first()
        if not d:
            raise HTTPException(404, "المستند غير موجود")
        path = DOCS_DIR / d.stored_filename
        if not path.exists():
            raise HTTPException(404, "الملف غير موجود على الخادم")
        return FileResponse(str(path), headers=_attachment_headers(d.original_filename))
    finally:
        db.close()


@router.delete("/{doc_id}")
async def delete_document(doc_id: str, request: Request):
    db = SessionLocal()
    try:
        user = _require_doc_access(db, request)
        require_csrf(request)
        d = db.query(Document).filter(Document.id == doc_id).first()
        if not d:
            raise HTTPException(404, "المستند غير موجود")
        if d.user_id != user.id and int(getattr(user, "is_admin", 0) or 0) != 1:
            raise HTTPException(403, "لا يمكنك حذف مستند رفعه مستخدم آخر")
        path = DOCS_DIR / d.stored_filename
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass
        db.delete(d)
        db.commit()
        log_event(db, "documents.delete", user.id, {"doc_id": doc_id})
        return {"ok": True}
    finally:
        db.close()
