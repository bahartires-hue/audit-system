from __future__ import annotations

import datetime as dt
import os
import uuid
from pathlib import Path

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse

from ..auth_core import log_event, require_csrf, require_user
from ..db import SessionLocal
from ..mailer import send_plain_email
from ..models import BankAccount, Plan, SubscriptionRequest, User
from .auth_api import _require_admin_user

router = APIRouter(tags=["billing"])

BASE_DIR = Path(__file__).resolve().parents[3]
_data_root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
UPLOAD_DIR = (Path(_data_root) / "uploads") if _data_root else (BASE_DIR / "uploads")
RECEIPTS_DIR = UPLOAD_DIR / "subscription_receipts"
RECEIPTS_DIR.mkdir(parents=True, exist_ok=True)

_ALLOWED_RECEIPT_SUFFIXES = (".pdf", ".jpg", ".jpeg", ".png", ".webp")
MAX_RECEIPT_MB = int(os.environ.get("AUDITFLOW_MAX_RECEIPT_MB", "15"))

STATUS_LABELS_AR = {
    "pending": "قيد المراجعة",
    "approved": "مقبول",
    "rejected": "مرفوض",
    "info_requested": "بانتظار معلومات إضافية",
}
PAYMENT_METHOD_LABELS_AR = {"bank_transfer": "حوالة بنكية", "cash": "نقداً"}


def seed_default_plans(db) -> None:
    if db.query(Plan).count() > 0:
        return
    defaults = [
        {"key": "free", "name_ar": "مجانية", "price_monthly": 0, "price_annual": 0, "max_users": 1,
         .test_unsed_variable",
         "features_json": ["مستخدم واحد", "الميزات الأساسية"], "sort_order": 0},
        {"key": "pro", "name_ar": "احترافية", "price_monthly": 149, "price_annual": 1490, "max_users": 5,
         "features_json": ["حتى 5 مستخدمين", "كل الميزات الأساسية", "دعم فني بالبريد"], "sort_order": 1},
        {"key": "enterprise", "name_ar": "مؤسسية", "price_monthly": 399, "price_annual": 3990, "max_users": None,
         "features_json": ["مستخدمون غير محدودين", "كل الميزات", "دعم فني مخصص"], "sort_order": 2},
    ]
    for p in defaults:
        db.add(Plan(id=uuid.uuid4().hex, is_active=1, **p))
    db.commit()


def _plan_public(p: Plan) -> dict:
    return {
        "id": p.id, "key": p.key, "name_ar": p.name_ar,
        "price_monthly": p.price_monthly, "price_annual": p.price_annual,
        "max_users": p.max_users, "features": p.features_json or [],
    }


def _bank_account_public(b: BankAccount) -> dict:
    return {
        "id": b.id, "bank_name": b.bank_name, "beneficiary_name": b.beneficiary_name,
        "account_number": b.account_number, "iban": b.iban,
    }


def _request_public(r: SubscriptionRequest, plan_by_id: dict) -> dict:
    plan = plan_by_id.get(r.plan_id)
    return {
        "id": r.id, "full_name": r.full_name, "company_name": r.company_name,
        "email": r.email, "phone": r.phone,
        "plan_id": r.plan_id, "plan_name": plan.name_ar if plan else r.plan_id,
        "billing_cycle": r.billing_cycle, "payment_method": r.payment_method,
        "payment_method_label": PAYMENT_METHOD_LABELS_AR.get(r.payment_method, r.payment_method),
        "has_receipt": bool(r.receipt_stored_filename),
        "status": r.status, "status_label": STATUS_LABELS_AR.get(r.status, r.status),
        "admin_notes": r.admin_notes,
        "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
        "reviewed_at": r.reviewed_at.isoformat() + "Z" if r.reviewed_at else None,
    }


@router.get("/api/plans")
def list_plans():
    db = SessionLocal()
    try:
        seed_default_plans(db)
        plans = db.query(Plan).filter(Plan.is_active == 1).order_by(Plan.sort_order.asc()).all()
        return {"items": [_plan_public(p) for p in plans]}
    finally:
        db.close()


@router.get("/api/bank-accounts")
def list_bank_accounts_public():
    db = SessionLocal()
    try:
        items = db.query(BankAccount).filter(BankAccount.is_active == 1).order_by(BankAccount.created_at.asc()).all()
        return {"items": [_bank_account_public(b) for b in items]}
    finally:
        db.close()


@router.post("/api/subscribe")
async def submit_subscription_request(
    request: Request,
    full_name: str = Form(...),
    company_name: str = Form(""),
    email: str = Form(...),
    phone: str = Form(...),
    plan_id: str = Form(...),
    billing_cycle: str = Form("monthly"),
    payment_method: str = Form(...),
    receipt: UploadFile | None = File(None),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        require_csrf(request)

        plan = db.query(Plan).filter(Plan.id == plan_id, Plan.is_active == 1).first()
        if not plan:
            raise HTTPException(400, "الخطة المحددة غير موجودة")
        if billing_cycle not in ("monthly", "annual"):
            raise HTTPException(400, "دورة الفوترة غير صحيح ة")
        if payment_method not in ("bank_transfer", "cash"):
            raise HTTPException(400, "طريقة الدفع غير صحيحة")

        stored_name = None
        if payment_method == "bank_transfer":
            if receipt is None or not receipt.filename:
                raise HTTPException(400, "الرجاء رفع صورة إيصال التحويل")
            suffix = Path(receipt.filename).suffix.lower()
            if suffix not in _ALLOWED_RECEIPT_SUFFIXES:
                raise HTTPException(400, "صيغة الملف غير مدعومة")
            content = await receipt.read()
            if not content:
                raise HTTPException(400, "ملف الإيصال فارغ")
            if len(content) > MAX_RECEIPT_MB * 1024 * 1024:
                raise HTTPException(400, f"حجم الملف أكبر من {MAX_RECEIPT_MB} ميجابايت")
            stored_name = f"{uuid.uuid4().hex}{suffix}"
            with open(RECEIPTS_DIR / stored_name, "wb") as f:
                f.write(content)

        req = SubscriptionRequest(
            id=uuid.uuid4().hex,
            user_id=user.id,
            full_name=full_name.strip()[:200],
            company_name=(company_name or "").strip()[:200] or None,
            email=email.strip()[:200],
            phone=phone.strip()[:50],
            plan_id=plan.id,
            billing_cycle=billing_cycle,
            payment_method=payment_method,
            receipt_stored_filename=stored_name,
            status="pending",
        )
        db.add(req)
        db.commit()
        log_event(db, "billing.subscribe_request", user.id, {"request_id": req.id, "plan": plan.key, "method": payment_method})
        return {"id": req.id, "status": req.status}
    finally:
        db.close()


@router.get("/api/subscribe/mine")
def my_subscription_requests(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        plans = {p.id: p for p in db.query(Plan).all()}
        items = (
            db.query(SubscriptionRequest)
            .filter(SubscriptionRequest.user_id == user.id)
            .order_by(SubscriptionRequest.created_at.desc())
            .limit(50)
            .all()
        )
        return {"items": [_request_public(r, plans) for r in items]}
    finally:
        db.close()


# ---------------------------------------------------------------- admin: bank accounts

@router.get("/admin/bank-accounts")
def admin_list_bank_accounts(request: Request):
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        items = db.query(BankAccount).order_by(BankAccount.created_at.asc()).all()
        return {"items": [{**_bank_account_public(b), "is_active": bool(b.is_active)} for b in items]}
    finally:
        db.close()


@router.post("/admin/bank-accounts")
def admin_create_bank_account(request: Request, payload: dict = Body(...)):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        bank_name = (payload.get("bank_name") or "").strip()
        beneficiary_name = (payload.get("beneficiary_name") or "").strip()
        account_number = (payload.get("account_number") or "").strip()
        iban = (payload.get("iban") or "").strip()
        if not (bank_name and beneficiary_name and account_number and iban):
            raise HTTPException(400, "جميع الحقول مطلوبة")
        acc = BankAccount(
            id=uuid.uuid4().hex, bank_name=bank_name[:200], beneficiary_name=beneficiary_name[:200],
            account_number=account_number[:100], iban=iban[:100], is_active=1,
        )
        db.add(acc)
        db.commit()
        log_event(db, "billing.bank_account_create", admin.id, {"id": acc.id})
        return {**_bank_account_public(acc), "is_active": True}
    finally:
        db.close()


@router.patch("/admin/bank-accounts/{account_id}")
def admin_update_bank_account(account_id: str, request: Request, payload: dict = Body(...)):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        acc = db.query(BankAccount).filter(BankAccount.id == account_id).first()
        if not acc:
            raise HTTPException(404, "الحساب غير موجود")
        for field in ("bank_name", "beneficiary_name", "account_number", "iban"):
            if field in payload and payload[field] is not None:
                setattr(acc, field, str(payload[field]).strip()[:200])
        if "is_active" in payload:
            acc.is_active = 1 if payload["is_active"] else 0
        db.commit()
        log_event(db, "billing.bank_account_update", admin.id, {"id": acc.id})
        return {**_bank_account_public(acc), "is_active": bool(acc.is_active)}
    finally:
        db.close()


@router.delete("/admin/bank-accounts/{account_id}")
def admin_delete_bank_account(account_id: str, request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        acc = db.query(BankAccount).filter(BankAccount.id == account_id).first()
        if not acc:
            raise HTTPException(404, "الحساب غير موجود")
        db.delete(acc)
        db.commit()
        log_event(db, "billing.bank_account_delete", admin.id, {"id": account_id})
        return {"ok": True}
    finally:
        db.close()


# ---------------------------------------------------------------- admin: subscription requests

@router.get("/admin/subscription-requests")
def admin_list_subscription_requests(request: Request, status: str = Query("")):
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        q = db.query(SubscriptionRequest)
        if status:
            q = q.filter(SubscriptionRequest.status == status)
        items = q.order_by(SubscriptionRequest.created_at.desc()).limit(500).all()
        plans = {p.id: p for p in db.query(Plan).all()}
        return {"items": [_request_public(r, plans) for r in items]}
    finally:
        db.close()


@router.get("/admin/subscription-requests/{request_id}/receipt")
def admin_get_receipt(request_id: str, request: Request):
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        r = db.query(SubscriptionRequest).filter(SubscriptionRequest.id == request_id).first()
        if not r or not r.receipt_stored_filename:
            raise HTTPException(404, "لا يوجد إيصال")
        path = RECEIPTS_DIR / r.receipt_stored_filename
        if not path.exists():
            raise HTTPException(404, "الملف غير موجود")
        return FileResponse(str(path))
    finally:
        db.close()


def _activate_subscription(db, req: SubscriptionRequest) -> User:
    user = db.query(User).filter(User.id == req.user_id).first()
    if not user:
        raise HTTPException(404, "المستخدم غير موجود")
    plan = db.query(Plan).filter(Plan.id == req.plan_id).first()
    now = dt.datetime.utcnow()
    base = user.subscription_expires_at if (user.subscription_expires_at and user.subscription_expires_at > now) else now
    days = 365 if req.billing_cycle == "annual" else 30
    user.plan_name = plan.key if plan else user.plan_name
    user.subscription_expires_at = base + dt.timedelta(days=days)
    db.add(user)
    return user


@router.post("/admin/subscription-requests/{request_id}/approve")
def admin_approve_request(request_id: str, request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        r = db.query(SubscriptionRequest).filter(SubscriptionRequest.id == request_id).first()
        if not r:
            raise HTTPException(404, "الطلب غير موجود")
        if r.status == "approved":
            raise HTTPException(400, "تمت الموافقة على هذا الطلب مسبقاً")
        user = _activate_subscription(db, r)
        r.status = "approved"
        r.reviewed_at = dt.datetime.utcnow()
        r.reviewed_by_id = admin.id
        db.add(r)
        db.commit()
        log_event(db, "billing.request_approve", admin.id, {"request_id": r.id, "user_id": user.id})
        try:
            send_plain_email(
                user.email or r.email,
                "تم تفعيل اشتراكك | OptimalSuite AI",
                f"مرحباً {r.full_name},\n\nتم قبول طلب اشتراكك وتفعيل الخطة بنجاح.\n"
                f"تاريخ انتهاء الاشتراك: {user.subscription_expires_at.strftime('%Y-%m-%d')}\n\n"
                "شكراً لاستخدامك OptimalSuite AI.",
            )
        except Exception:
            pass
        return _request_public(r, {p.id: p for p in db.query(Plan).all()})
    finally:
        db.close()


@router.post("/admin/subscription-requests/{request_id}/reject")
def admin_reject_request(request_id: str, request: Request, payload: dict = Body(default_factory=dict)):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        r = db.query(SubscriptionRequest).filter(SubscriptionRequest.id == request_id).first()
        if not r:
            raise HTTPException(404, "الطلب غير موجود")
        note = ((payload or {}).get("note") or "").strip()[:2000]
        r.status = "rejected"
        r.admin_notes = note or r.admin_notes
        r.reviewed_at = dt.datetime.utcnow()
        r.reviewed_by_id = admin.id
        db.add(r)
        db.commit()
        log_event(db, "billing.request_reject", admin.id, {"request_id": r.id})
        try:
            send_plain_email(
                r.email,
                "بخصوص طلب اشتراكك | OptimalSuite AI",
                f"مرحباً {r.full_name},\n\nنأسف لإبلاغك أنه تم رفض طلب اشتراكك.\n"
                + (f"السبب: {note}\n" if note else "")
                + "\nيمكنك التواصل معنا لمزيد من التفاصيل أو تقديم طلب جديد.",
            )
        except Exception:
            pass
        return _request_public(r, {p.id: p for p in db.query(Plan).all()})
    finally:
        db.close()


@router.post("/admin/subscription-requests/{request_id}/request-info")
def admin_request_info(request_id: str, request: Request, payload: dict = Body(default_factory=dict)):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        r = db.query(SubscriptionRequest).filter(SubscriptionRequest.id == request_id).first()
        if not r:
            raise HTTPException(404, "الطلب غير موجود")
        note = ((payload or {}).get("note") or "").strip()[:2000]
        r.status = "info_requested"
        r.admin_notes = note or r.admin_notes
        r.reviewed_at = dt.datetime.utcnow()
        r.reviewed_by_id = admin.id
        db.add(r)
        db.commit()
        log_event(db, "billing.request_info", admin.id, {"request_id": r.id})
        try:
            send_plain_email(
                r.email,
                "مطلوب معلومات إضافية لطلب اشتراكك | OptimalSuite AI",
                f"مرحباً {r.full_name},\n\nنحتاج معلومات إضافية لاستكمال طلب اشتراكك:\n"
                f"{note}\n\nالرجاء التواصل معنا أو تحديث طلبك.",
            )
        except Exception:
            pass
        return _request_public(r, {p.id: p for p in db.query(Plan).all()})
    finally:
        db.close()
