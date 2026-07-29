from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

from ..auth_core import log_event, require_csrf, require_user, user_can_access_page_key
from ..db import SessionLocal
from ..models import CompanyProfile

router = APIRouter(prefix="/api/company-profile", tags=["company-profile"])

# نفس منطق حساب مجلد الرفع في main.py (auditflow/uploads أو AUDITFLOW_DATA_ROOT/uploads)،
# محسوب محليًا هنا لتفادي استيراد دائري مع main.py.
BASE_DIR = Path(__file__).resolve().parents[3]
_data_root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
UPLOAD_DIR = (Path(_data_root) / "uploads") if _data_root else (BASE_DIR / "uploads")
COMPANY_DIR = UPLOAD_DIR / "company"
COMPANY_DIR.mkdir(parents=True, exist_ok=True)

_ALLOWED_LOGO_SUFFIXES = (".png", ".jpg", ".jpeg", ".svg")
MAX_LOGO_MB = 5


def _require_read_access(db, request: Request):
    user = require_user(db, request)
    if not user_can_access_page_key(user, "op-inventory"):
        raise HTTPException(403, "ليس لديك صلاحية للوصول لهذه البيانات")
    return user


def _require_admin_access(db, request: Request):
    user = require_user(db, request)
    if not user_can_access_page_key(user, "op-inventory"):
        raise HTTPException(403, "ليس لديك صلاحية للوصول لهذه البيانات")
    if int(getattr(user, "is_admin", 0) or 0) != 1:
        raise HTTPException(403, "تعديل بيانات الشركة متاح للمدير فقط")
    return user


def _get_or_create(db) -> CompanyProfile:
    profile = db.query(CompanyProfile).filter(CompanyProfile.id == "default").first()
    if not profile:
        profile = CompanyProfile(id="default", currency="SAR", vat_percentage=15.0)
        db.add(profile)
        db.commit()
    return profile


def _profile_out(p: CompanyProfile) -> dict:
    return {
        "company_name": p.company_name,
        "trade_name": p.trade_name,
        "logo_url": p.logo_url,
        "commercial_register": p.commercial_register,
        "tax_number": p.tax_number,
        "address": p.address,
        "city": p.city,
        "country": p.country,
        "postal_code": p.postal_code,
        "phone": p.phone,
        "email": p.email,
        "website": p.website,
        "currency": p.currency,
        "vat_percentage": p.vat_percentage,
        "updated_at": p.updated_at.isoformat() + "Z" if p.updated_at else None,
    }


@router.get("")
def get_company_profile(request: Request):
    db = SessionLocal()
    try:
        _require_read_access(db, request)
        profile = _get_or_create(db)
        return _profile_out(profile)
    finally:
        db.close()


TEXT_FIELDS = [
    "company_name", "trade_name", "commercial_register", "tax_number",
    "address", "city", "country", "postal_code", "phone", "email", "website", "currency",
]


@router.patch("")
async def update_company_profile(request: Request):
    payload = await request.json()
    db = SessionLocal()
    try:
        user = _require_admin_access(db, request)
        require_csrf(request)
        profile = _get_or_create(db)
        for field in TEXT_FIELDS:
            if field in (payload or {}):
                val = str((payload or {}).get(field) or "").strip() or None
                setattr(profile, field, val)
        if "vat_percentage" in (payload or {}):
            try:
                vat = float(payload.get("vat_percentage") or 0)
            except (TypeError, ValueError):
                raise HTTPException(400, "نسبة الضريبة يجب أن تكون رقمًا")
            if vat < 0 or vat > 100:
                raise HTTPException(400, "نسبة الضريبة يجب أن تكون بين 0 و100")
            profile.vat_percentage = vat
        profile.updated_at = dt.datetime.utcnow()
        db.commit()
        log_event(db, "company_profile.updated", user.id, {})
        return _profile_out(profile)
    finally:
        db.close()


@router.post("/logo")
async def upload_company_logo(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        user = _require_admin_access(db, request)
        require_csrf(request)

        original = file.filename or "logo"
        lower = original.lower()
        if not any(lower.endswith(s) for s in _ALLOWED_LOGO_SUFFIXES):
            raise HTTPException(400, "صيغة الشعار يجب أن تكون PNG أو JPG أو SVG")
        content = await file.read()
        if not content:
            raise HTTPException(400, "الملف فارغ")
        if len(content) > MAX_LOGO_MB * 1024 * 1024:
            raise HTTPException(400, f"حجم الشعار أكبر من {MAX_LOGO_MB} ميجابايت")

        import uuid as _uuid

        suffix = Path(original).suffix.lower() or ".png"
        saved_name = f"{_uuid.uuid4().hex}{suffix}"
        saved_path = COMPANY_DIR / saved_name
        with open(saved_path, "wb") as f:
            f.write(content)

        profile = _get_or_create(db)
        profile.logo_url = f"/uploads/company/{saved_name}"
        profile.updated_at = dt.datetime.utcnow()
        db.commit()
        log_event(db, "company_profile.logo_updated", user.id, {})
        return _profile_out(profile)
    finally:
        db.close()
