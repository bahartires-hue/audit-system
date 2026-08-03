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
from ..mailer import send_plain_email
from ..models import BankAccount, Plan, SubscriptionRequest, User

router = APIRouter(tags=["billing"])

_ALLOWED_RECEIPT_SUFFIXES = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp")
MAX_RECEIPT_MB = int(os.environ.get("AUDITFLOW_MAX_RECEIPT_MB", "10"))

BASE_DIR = Path(__file__).resolve().parents[3]
_data_root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
UPLOAD_DIR = (Path(_data_root) / "uploads") if _data_root else (BASE_DIR / "uploads")
RECEIPTS_DIR = UPLOAD_DIR / "receipts"
RECEIPTS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_PLANS = [
    {
        "key": "free",
        "name_ar": "مجانية",
        "price_monthly": 0,
        "price_annual": 0,
        "max_users": 1,
        "features_json": ["مستخدم واحد", "تقارير محدودة", "مساحة تخزين محدودة"],
        "sort_order": 1,
    },
    {
        "key": "pro",
        "name_ar": "احترافية",
        "price_monthly": 299,
        "price_annual": 2990,
        "max_users": 10,
        "features_json": ["عدد مستخدمون أكبر", "جميع التقارير", "AI كامل", "نسخ احتياطي تلقائي", "مساحة أكبر", "دعم فني"],
        "sort_order": 2,
    },
    {
        "key": "enterprise",
        "name_ar": "الشركات",
        "price_monthly": 899,
        "price_annual": 8990,
        "max_users": None,
        "features_json": ["مستخدمون غير محدودين", "جميع المميزات", "صلاحيات متقدمة", "دعم أولوية", "تخصيصات خاصة"],
        "sort_order": 3,
    },
]

STATUS_LABELS_AR = {
    "pending": "قيد المراجعة",
    "approved": "مقبول",
    "rejected": "مرفوض",
    "info_requested": "بانتظار معلومات إضافية",
}

METHOD_LABELS_AR = {"bank_transfer": "حوالة بنكية", "cash": "نقداً"}


def _ensure_default_plans(db) -> None:
    if db.query(Plan).count() > 0:
        return
    for p in DEFAULT_PLANS:
        db.add(Plan(id=uuid.uuid4().hex, **p, is_active=1))
    db.commit()


def _plan_public(p: Plan) -> dict:
    return {
        "id": p.id,
        "key": p.key,
        "name_ar": p.name_ar,
        "price_monthly": p.price_monthly,
        "price_annual": p.price_annual,
        "max_users": p.max_users,
        "features": p.features_json or [],
        "is_active": bool(p.is_active),
    }


def _bank_account_public(b: BankAccount) -> dict:
    return {
        "id": b.id,
        "bank_name": b.bank_name,
        "beneficiary_name": b.beneficiary_name,
        "account_number": b.account_number,
        "iban": b.iban,
        "is_active": bool(b.is_active),
        "created_at": b.created_at.isoformat() + "Z" if b.created_at else None,
    }


def _request_public(r: SubscriptionRequest, plan_by_id: dict, users_by_id: dict) -> dict:
    plan = plan_by_id.get(r.plan_id)
    reviewer = users_by_id.get(r.reviewed_by_id)
    return {
        "id": r.id,
        "full_name": r.full_name,
        "company_name": r.company_name or "",
        "email": r.email,
        "phone": r.phone,
        "plan_id": r.plan_id,
        "plan_key": plan.key if plan else None,
        "plan_name": plan.name_ar if plan else "-",
        "billing_cycle": r.billing_cycle,
        "billing_cycle_label": "سنوي" if r.billing_cycle == "annual" else "شهري",
        "payment_method": r.payment_method,
        "payment_method_label": METHOD_LABELS_AR.get(r.payment_method, r.payment_method),
        "has_receipt": bool(r.receipt_stored_filename),
        "status": r.status,
        "status_label": STATUS_LABELS_AR.get(r.status, r.status),
        "admin_notes": r.admin_notes or "",
        "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
        "reviewed_at": r.reviewed_at.isoformat() + "Z" if r.reviewed_at else None,
        "reviewed_by": reviewer.username if reviewer else None,
    }


@router.get("/api/plans")
def list_plans(request: Request):
    db = SessionLocal()
    try:
        require_user(db, request)
        _ensure_default_plans(db)
        rows = db.query(Plan).filter(Plan.is_active == 1).order_by(Plan.sort_order.asc()).all()
        return {"items": [_plan_public(p) for p in rows]}
    finally:
        db.close()


@router.get("/api/bank-accounts")
def list_bank_accounts_public(request: Request):
    db = SessionLocal()
    try:
        require_user(db, request)
        rows = db.query(BankAccount).filter(BankAccount.is_active == 1).order_by(BankAccount.created_at.asc()).all()
        return {"items": [_bank_account_public(b) for b in rows]}
    finally:
        db.close()


@router.post("/api/subscribe")
async def create_subscription_request(
    request: Request,
    plan_id: str = Form(...),
    billing_cycle: str = Form("monthly"),
    full_name: str = Form(...),
    company_name: str = Form(""),
    phone: str = Form(...),
    payment_method: str = Form(...),
    receipt: UploadFile | None = File(None),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        require_csrf(request)

        cycle = (billing_cycle or "monthly").strip().lower()
        if cycle not in ("monthly", "annual"):
            raise HTTPException(400, "دورة الفوترة غير صحيحة")

        method = (payment_method or "").strip().lower()
        if method not in ("bank_transfer", "cash"):
            raise HTTPException(400, "طريقة الدفع غير صحيحة")

        plan = db.query(Plan).filter(Plan.id == plan_id, Plan.is_active == 1).first()
        if not plan:
            raise HTTPException(404, "الخطة غير موجودة")

        stored_name = None
        if method == "bank_transfer":
            if receipt is None or not receipt.filename:
                raise HTTPException(400, "يرجى رفع صورة إيصال التحويل")
            original = receipt.filename or "receipt"
            suffix = Path(original).suffix.lower()
            if suffix not in _ALLOWED_RECEIPT_SUFFIXES:
                raise HTTPException(400, "صيغة صورة الإيصال غير مدعومة")
            content = await receipt.read()
            if not content:
                raise HTTPException(400, "ملف الإيصال فارغ")
            max_bytes = MAX_RECEIPT_MB * 1024 * 1024
            if len(content) > max_bytes:
                raise HTTPException(400, f"حجم الملف أكبر من {MAX_RECEIPT_MB} ميجابايت")
            stored_name = f"{uuid.uuid4().hex}{suffix}"
            with open(RECEIPTS_DIR / stored_name, "wb") as f:
                f.write(content)

        req = SubscriptionRequest(
            id=uuid.uuid4().hex,
            user_id=user.id,
            full_name=(full_name or "").strip()[:200] or (user.username or ""),
            company_name=(company_name or "").strip()[:200] or None,
            email=(user.email or "").strip(),
            phone=(phone or "").strip()[:50],
            plan_id=plan.id,
            billing_cycle=cycle,
            payment_method=method,
            receipt_stored_filename=stored_name,
            status="pending",
        )
        db.add(req)
        db.commit()
        log_event(db, "billing.subscribe_request", user.id, {"request_id": req.id, "plan_key": plan.key, "method": method})
        return {"ok": True, "id": req.id, "status": req.status}
    finally:
        db.close()


@router.get("/api/subscribe/mine")
def list_my_subscription_requests(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = (
            db.query(SubscriptionRequest)
            .filter(SubscriptionRequest.user_id == user.id)
            .order_by(SubscriptionRequest.created_at.desc())
            .limit(20)
            .all()
        )
        plans = {p.id: p for p in db.query(Plan).all()}
        return {"items": [_request_public(r, plans, {}) for r in rows]}
    finally:
        db.close()


def _require_admin_user(db, request: Request) -> User:
    user = require_user(db, request)
    if int(getattr(user, "is_admin", 0) or 0) != 1:
        raise HTTPException(403, "هذا الإجراء متاح للمدير فقط")
    return user


@router.get("/admin/bank-accounts")
def admin_list_bank_accounts(request: Request):
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        rows = db.query(BankAccount).order_by(BankAccount.created_at.desc()).all()
        return {"items": [_bank_account_public(b) for b in rows]}
    finally:
        db.close()


@router.post("/admin/bank-accounts")
async def admin_create_bank_account(request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        payload = await request.json()
        bank_name = str((payload or {}).get("bank_name", "")).strip()
        beneficiary_name = str((payload or {}).get("beneficiary_name", "")).strip()
        account_number = str((payload or {}).get("account_number", "")).strip()
        iban = str((payload or {}).get("iban", "")).strip()
        if not (bank_name and beneficiary_name and account_number and iban):
            raise HTTPException(400, "يرجى تعبئة جميع الحقول")
        acc = BankAccount(
            id=uuid.uuid4().hex,
            bank_name=bank_name,
            beneficiary_name=beneficiary_name,
            account_number=account_number,
            iban=iban,
            is_active=1,
        )
        db.add(acc)
        db.commit()
        log_event(db, "billing.bank_account.create", admin.id, {"bank_account_id": acc.id})
        return _bank_account_public(acc)
    finally:
        db.close()


@router.patch("/admin/bank-accounts/{account_id}")
async def admin_update_bank_account(account_id: str, request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        acc = db.query(BankAccount).filter(BankAccount.id == account_id).first()
        if not acc:
            raise HTTPException(404, "الحساب البنكي غير موجود")
        payload = await request.json()
        for field in ("bank_name", "beneficiary_name", "account_number", "iban"):
            if field in (payload or {}):
                setattr(acc, field, str(payload[field]).strip())
        if "is_active" in (payload or {}):
            acc.is_active = 1 if payload["is_active"] else 0
        db.commit()
        log_event(db, "billing.bank_account.update", admin.id, {"bank_account_id": acc.id})
        return _bank_account_public(acc)
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
            raise HTTPException(404, "الحساب البنكي غير موجود")
        db.delete(acc)
        db.commit()
        log_event(db, "billing.bank_account.delete", admin.id, {"bank_account_id": account_id})
        return {"ok": True}
    finally:
        db.close()


@router.get("/admin/subscription-requests")
def admin_list_subscription_requests(request: Request, status: str = Query("")):
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        query = db.query(SubscriptionRequest)
        st = (status or "").strip().lower()
        if st:
            if st not in STATUS_LABELS_AR:
                raise HTTPException(400, "حالة غير صحيحة")
            query = query.filter(SubscriptionRequest.status == st)
        rows = query.order_by(SubscriptionRequest.created_at.desc()).limit(500).all()
        plans = {p.id: p for p in db.query(Plan).all()}
        reviewer_ids = {r.reviewed_by_id for r in rows if r.reviewed_by_id}
        reviewers = {}
        if reviewer_ids:
            for u in db.query(User).filter(User.id.in_(reviewer_ids)).all():
                reviewers[u.id] = u
        return {"items": [_request_public(r, plans, reviewers) for r in rows]}
    finally:
        db.close()


@router.get("/admin/subscription-requests/{request_id}/receipt")
def admin_get_subscription_receipt(request_id: str, request: Request):
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        req = db.query(SubscriptionRequest).filter(SubscriptionRequest.id == request_id).first()
        if not req or not req.receipt_stored_filename:
            raise HTTPException(404, "لا توجد صورة إيصال لهذا الطلب")
        path = RECEIPTS_DIR / req.receipt_stored_filename
        if not path.exists():
            raise HTTPException(404, "الملف غير موجود على الخادم")
        return FileResponse(str(path))
    finally:
        db.close()


def _months_for_cycle(cycle: str) -> int:
    return 12 if cycle == "annual" else 1


@router.post("/admin/subscription-requests/{request_id}/approve")
async def admin_approve_subscription_request(request_id: str, request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        req = db.query(SubscriptionRequest).filter(SubscriptionRequest.id == request_id).first()
        if not req:
            raise HTTPException(404, "الطلب غير موجود")
        if req.status == "approved":
            raise HTTPException(400, "تم قبول هذا الطلب مسبقًا")
        plan = db.query(Plan).filter(Plan.id == req.plan_id).first()
        if not plan:
            raise HTTPException(400, "خطة الطلب غير موجودة")
        payload = {}
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        months = int((payload or {}).get("months") or _months_for_cycle(req.billing_cycle))

        user = db.query(User).filter(User.id == req.user_id).first()
        if not user:
            raise HTTPException(404, "مستخدم الطلب غير موجود")

        now = dt.datetime.utcnow()
        base = user.subscription_expires_at if (user.subscription_expires_at and user.subscription_expires_at > now) else now
        user.plan_name = plan.key
        user.subscription_expires_at = base + dt.timedelta(days=30 * months)

        req.status = "approved"
        req.reviewed_at = now
        req.reviewed_by_id = admin.id
        db.commit()

        log_event(
            db,
            "billing.subscription_request.approve",
            admin.id,
            {"request_id": req.id, "plan_key": plan.key, "months": months, "user_id": user.id},
        )

        try:
            send_plain_email(
                to_email=user.email,
                subject="تم قبول طلب اشتراكك",
                body=(
                    f"مرحباً {user.username}،\n\n"
                    f"تم قبول طلب اشتراكك في خطة \"{plan.name_ar}\" وتفعيلها على حسابك.\n"
                    f"تاريخ انتهاء الاشتراك الجديد: {user.subscription_expires_at.strftime('%Y-%m-%d')}\n\n"
                    f"شكرًا لاستخدامك OptimalSuite AI."
                ),
            )
        except Exception:
            pass

        return {"ok": True, "status": req.status, "plan_name": plan.name_ar, "subscription_expires_at": user.subscription_expires_at.isoformat() + "Z"}
    finally:
        db.close()


@router.post("/admin/subscription-requests/{request_id}/reject")
async def admin_reject_subscription_request(request_id: str, request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        req = db.query(SubscriptionRequest).filter(SubscriptionRequest.id == request_id).first()
        if not req:
            raise HTTPException(404, "الطلب غير موجود")
        payload = await request.json()
        notes = str((payload or {}).get("notes", "")).strip()[:2000]
        req.status = "rejected"
        req.admin_notes = notes or req.admin_notes
        req.reviewed_at = dt.datetime.utcnow()
        req.reviewed_by_id = admin.id
        db.commit()
        log_event(db, "billing.subscription_request.reject", admin.id, {"request_id": req.id})

        user = db.query(User).filter(User.id == req.user_id).first()
        if user and user.email:
            try:
                send_plain_email(
                    to_email=user.email,
                    subject="بخصوص طلب اشتراكك",
                    body=(
                        f"مرحباً {user.username}،\n\n"
                        f"نأسف، تم رفض طلب اشتراكك.\n"
                        + (f"ملاحظات: {notes}\n\n" if notes else "\n")
                        + "لأي استفسار يرجى التواصل مع فريق الدعم."
                    ),
                )
            except Exception:
                pass
        return {"ok": True, "status": req.status}
    finally:
        db.close()


@router.post("/admin/subscription-requests/{request_id}/request-info")
async def admin_request_info_subscription_request(request_id: str, request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        req = db.query(SubscriptionRequest).filter(SubscriptionRequest.id == request_id).first()
        if not req:
            raise HTTPException(404, "الطلب غير موجود")
        payload = await request.json()
        notes = str((payload or {}).get("notes", "")).strip()[:2000]
        if not notes:
            raise HTTPException(400, "يرجى كتابة توضيح للمعلومات المطلوبة")
        req.status = "info_requested"
        req.admin_notes = notes
        req.reviewed_at = dt.datetime.utcnow()
        req.reviewed_by_id = admin.id
        db.commit()
        log_event(db, "billing.subscription_request.request_info", admin.id, {"request_id": req.id})

        user = db.query(User).filter(User.id == req.user_id).first()
        if user and user.email:
            try:
                send_plain_email(
                    to_email=user.email,
                    subject="مطلوب معلومات إضافية لطلب اشتراكك",
                    body=(
                        f"مرحباً {user.username}،\n\n"
                        f"نحتاج معلومات إضافية لإتمام مراجعة طلب اشتراكك:\n{notes}\n\n"
                        f"يرجى التواصل مع فريق الدعم أو تحديث طلبك."
                    ),
                )
            except Exception:
                pass
        return {"ok": True, "status": req.status}
    finally:
        db.close()
