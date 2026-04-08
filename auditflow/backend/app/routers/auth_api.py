from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import os
import secrets
import uuid

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import func

from ..auth_core import (
    COOKIE_PATH,
    CSRF_COOKIE,
    LOCK_MINUTES,
    SESSION_COOKIE,
    cookie_secure,
    create_session,
    current_user_from_request,
    hash_password,
    issue_csrf_token,
    log_event,
    admin_contact_text,
    require_csrf,
    require_user,
    session_max_age_seconds,
    subscription_expired_text,
    verify_password,
)
from ..db import SessionLocal
from ..mailer import send_password_reset_email, send_smtp_test_email, smtp_status
from ..models import AnalysisReport, AppSetting, AuditLog, InviteCode, PasswordResetToken, User, UserSession
from ..rate_limit import limiter

router = APIRouter(tags=["auth"])


def _default_admin_config() -> dict:
    return {
        "admin_contact": (os.getenv("AUDITFLOW_ADMIN_CONTACT") or "").strip(),
        "company_name": "OptimalMatch",
        "social_links": {
            "whatsapp": "https://wa.me/966558815838",
            "email": "mailto:auditsystem2030@gmail.com",
        },
    }


def _get_admin_config(db) -> dict:
    row = db.query(AppSetting).filter(AppSetting.key == "admin_config").first()
    if not row or not isinstance(row.value_json, dict):
        return _default_admin_config()
    base = _default_admin_config()
    base.update(row.value_json or {})
    if not isinstance(base.get("social_links"), dict):
        base["social_links"] = _default_admin_config()["social_links"]
    return base


def _invite_env_codes() -> set[str]:
    raw = (os.getenv("AUDITFLOW_INVITE_CODES") or "").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _is_invite_valid(db, code: str) -> bool:
    c = (code or "").strip()
    if not c:
        return False
    if c in _invite_env_codes():
        return True
    row = db.query(InviteCode).filter(InviteCode.code == c).first()
    if not row:
        return False
    if int(row.disabled or 0) == 1:
        return False
    if row.expires_at and row.expires_at < dt.datetime.utcnow():
        return False
    return int(row.used_count or 0) < int(row.max_uses or 1)


def _consume_invite_if_db(db, code: str, user_id: str) -> None:
    c = (code or "").strip()
    if not c or c in _invite_env_codes():
        return
    row = db.query(InviteCode).filter(InviteCode.code == c).first()
    if not row:
        return
    row.used_count = int(row.used_count or 0) + 1
    log_event(db, "auth.invite.consume", user_id, {"code": c})


def _is_bootstrap_admin_registration(db, username: str, email: str) -> bool:
    # First account becomes admin automatically.
    any_user = db.query(User.id).first()
    if not any_user:
        return True
    admin_email = (os.getenv("AUDITFLOW_ADMIN_EMAIL") or "").strip().lower()
    admin_username = (os.getenv("AUDITFLOW_ADMIN_USERNAME") or "").strip()
    if admin_email and email == admin_email:
        return True
    if admin_username and username == admin_username:
        return True
    return False


def _require_admin_user(db, request: Request) -> User:
    user = require_user(db, request)
    if int(user.is_admin or 0) != 1:
        raise HTTPException(403, "هذه العملية للمدير فقط")
    return user


def _months_for_plan(plan_name: str) -> int:
    p = (plan_name or "").strip().lower()
    if p in ("month", "1m", "m1"):
        return 1
    if p in ("3months", "3m", "m3"):
        return 3
    if p in ("6months", "6m", "m6"):
        return 6
    if p in ("year", "12m", "m12"):
        return 12
    if p in ("5years", "60m", "m60"):
        return 60
    return 0


@router.get("/auth/me")
def auth_me(request: Request):
    db = SessionLocal()
    try:
        u = current_user_from_request(db, request)
        csrf = request.cookies.get(CSRF_COOKIE) or issue_csrf_token()
        username = u.username if u else None
        email = u.email if u else None
        is_admin = bool(int(u.is_admin or 0)) if u else False
        is_active = bool(int((u.is_active if u else 1) or 0)) if u else False
        plan_name = (u.plan_name or "free") if u else None
        exp = u.subscription_expires_at if u else None
        res = Response(
            content=json.dumps(
                {
                    "username": username,
                    "email": email,
                    "is_admin": is_admin,
                    "is_active": is_active,
                    "plan_name": plan_name,
                    "subscription_expires_at": exp.isoformat() + "Z" if exp else None,
                    "csrf_token": csrf,
                }
            ),
            media_type="application/json",
        )
        res.set_cookie(
            key=CSRF_COOKIE,
            value=csrf,
            path=COOKIE_PATH,
            httponly=False,
            samesite="lax",
            secure=cookie_secure(),
            max_age=session_max_age_seconds(),
        )
        return res
    finally:
        db.close()


@router.post("/auth/register")
async def auth_register(request: Request):
    payload = await request.json()
    username = str((payload or {}).get("username", "")).strip()
    email = str((payload or {}).get("email", "")).strip().lower()
    invite_code = str((payload or {}).get("invite_code", "")).strip()
    selected_plan = str((payload or {}).get("plan", "free")).strip().lower()
    accepted_terms = bool((payload or {}).get("accepted_terms"))
    accepted_privacy = bool((payload or {}).get("accepted_privacy"))
    accepted_agreement = bool((payload or {}).get("accepted_agreement"))
    password = str((payload or {}).get("password", "")).strip()
    if len(username) < 3:
        raise HTTPException(400, "اسم المستخدم قصير")
    if "@" not in email or "." not in email:
        raise HTTPException(400, "البريد الإلكتروني غير صالح")
    if len(password) < 4:
        raise HTTPException(400, "كلمة المرور قصيرة")
    if not (accepted_terms and accepted_privacy and accepted_agreement):
        raise HTTPException(400, "يجب الموافقة على شروط الاستخدام وسياسة الخصوصية واتفاقية المستخدم")

    db = SessionLocal()
    try:
        require_csrf(request)
        bootstrap_admin = _is_bootstrap_admin_registration(db, username, email)
        if not bootstrap_admin and not _is_invite_valid(db, invite_code):
            raise HTTPException(400, "كود الدعوة غير صالح أو منتهي")
        exists = db.query(User).filter(func.lower(User.username) == username.lower()).first()
        if exists:
            raise HTTPException(400, "اسم المستخدم موجود بالفعل، سجّل دخولك مباشرة")
        exists_email = db.query(User).filter(User.email == email).first()
        if exists_email:
            raise HTTPException(400, "البريد الإلكتروني مستخدم بالفعل")
        plan_name = "free"
        is_active = 1
        sub_exp = None
        if not bootstrap_admin:
            months = _months_for_plan(selected_plan)
            if months <= 0:
                plan_name = "free"
                is_active = 1
            else:
                # Customer picked a paid term: wait for admin approval/activation.
                plan_name = f"pending_{selected_plan}"
                is_active = 0

        user = User(
            id=uuid.uuid4().hex,
            username=username,
            email=email,
            is_admin=1 if bootstrap_admin else 0,
            is_active=is_active,
            plan_name=plan_name,
            subscription_expires_at=sub_exp,
            password_hash=hash_password(password),
            preferences_json={
                "legal_acceptance": {
                    "terms": True,
                    "privacy": True,
                    "user_agreement": True,
                    "accepted_at": dt.datetime.utcnow().isoformat() + "Z",
                }
            },
        )
        db.add(user)
        db.commit()
        if not bootstrap_admin:
            _consume_invite_if_db(db, invite_code, user.id)
        db.commit()

        token = create_session(db, user.id)
        csrf = issue_csrf_token()
        log_event(db, "auth.register", user.id, {"username": username, "email": email, "is_admin": bool(user.is_admin)})
        res = Response(content='{"ok":true}', media_type="application/json")
        res.set_cookie(
            key=SESSION_COOKIE,
            value=token,
            path=COOKIE_PATH,
            httponly=True,
            samesite="lax",
            secure=cookie_secure(),
            max_age=session_max_age_seconds(),
        )
        res.set_cookie(
            key=CSRF_COOKIE,
            value=csrf,
            path=COOKIE_PATH,
            httponly=False,
            samesite="lax",
            secure=cookie_secure(),
            max_age=session_max_age_seconds(),
        )
        return res
    finally:
        db.close()


@router.get("/auth/activity")
def auth_activity(request: Request, limit: int = 100):
    lim = max(1, min(int(limit or 100), 500))
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = (
            db.query(AuditLog)
            .filter(AuditLog.user_id == user.id)
            .order_by(AuditLog.created_at.desc())
            .limit(lim)
            .all()
        )
        return {
            "items": [
                {
                    "id": x.id,
                    "action": x.action,
                    "meta": x.meta_json or {},
                    "created_at": x.created_at.isoformat() + "Z" if x.created_at else None,
                }
                for x in rows
            ]
        }
    finally:
        db.close()


@router.patch("/auth/preferences")
async def auth_preferences(request: Request):
    payload = await request.json()
    patch = (payload or {}).get("preferences") or payload or {}
    if not isinstance(patch, dict):
        raise HTTPException(400, "preferences يجب أن يكون كائناً")
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        cur = user.preferences_json if isinstance(user.preferences_json, dict) else {}
        merged = {**cur, **patch}
        user.preferences_json = merged
        db.commit()
        log_event(db, "auth.preferences_update", user.id, {"keys": list(patch.keys())})
        return {"preferences": merged}
    finally:
        db.close()


@router.post("/auth/request-password-reset")
@limiter.limit("10/minute")
async def auth_request_password_reset(request: Request):
    payload = await request.json()
    email = str((payload or {}).get("email", "")).strip().lower()
    if "@" not in email:
        raise HTTPException(400, "أدخل بريدًا صحيحًا")

    db = SessionLocal()
    try:
        require_csrf(request)
        user = db.query(User).filter(User.email == email).first()
        # Always return ok to avoid user enumeration
        if not user:
            return {"ok": True}

        token = secrets.token_urlsafe(36)
        exp_min = int((os.getenv("AUDITFLOW_RESET_TOKEN_MINUTES") or "30").strip())
        row = PasswordResetToken(
            token=token,
            user_id=user.id,
            expires_at=dt.datetime.utcnow() + dt.timedelta(minutes=max(5, exp_min)),
            used=0,
        )
        db.add(row)
        db.commit()

        base_url = (os.getenv("AUDITFLOW_PUBLIC_BASE_URL") or "").strip().rstrip("/")
        if not base_url:
            # Fallback for local usage
            base_url = str(request.base_url).rstrip("/")
        reset_link = f"{base_url}/login?reset_token={token}"
        try:
            send_password_reset_email(email, reset_link)
            log_event(db, "auth.password_reset.requested", user.id, {"email": email, "delivery": "smtp"})
            return {"ok": True}
        except Exception:
            # Fallback for environments without SMTP, keeps reset flow operational.
            log_event(db, "auth.password_reset.requested", user.id, {"email": email, "delivery": "link_fallback"})
            return {"ok": True, "delivery": "link_fallback", "reset_link": reset_link}
    finally:
        db.close()


@router.post("/auth/reset-password")
@limiter.limit("10/minute")
async def auth_reset_password(request: Request):
    payload = await request.json()
    token = str((payload or {}).get("token", "")).strip()
    new_password = str((payload or {}).get("new_password", "")).strip()
    if len(new_password) < 8:
        raise HTTPException(400, "كلمة المرور الجديدة يجب أن تكون 8 أحرف على الأقل")
    if not token:
        raise HTTPException(400, "رمز الاستعادة مطلوب")

    db = SessionLocal()
    try:
        require_csrf(request)
        row = db.query(PasswordResetToken).filter(PasswordResetToken.token == token).first()
        if not row:
            raise HTTPException(400, "رمز الاستعادة غير صالح")
        if int(row.used or 0) == 1 or row.expires_at < dt.datetime.utcnow():
            raise HTTPException(400, "رمز الاستعادة منتهي أو مستخدم")
        user = db.query(User).filter(User.id == row.user_id).first()
        if not user:
            raise HTTPException(400, "المستخدم غير موجود")

        user.password_hash = hash_password(new_password)
        user.failed_attempts = 0
        user.locked_until = None
        row.used = 1
        db.commit()
        log_event(db, "auth.password_reset.done", user.id, {})
        return {"ok": True}
    finally:
        db.close()


@router.post("/auth/invites")
async def auth_create_invite(request: Request):
    payload = await request.json()
    # Security by default: one-time, short-lived invites
    max_uses = 1
    hours = int((payload or {}).get("expires_in_hours", 168) or 168)
    code = str((payload or {}).get("code", "")).strip() or secrets.token_urlsafe(8).replace("-", "").replace("_", "")
    code = code[:32]
    if hours < 1:
        hours = 1
    if hours > 168:
        hours = 168

    db = SessionLocal()
    try:
        require_csrf(request)
        user = _require_admin_user(db, request)

        exists = db.query(InviteCode).filter(InviteCode.code == code).first()
        if exists:
            raise HTTPException(400, "الكود موجود بالفعل")
        row = InviteCode(
            code=code,
            created_by=user.id,
            max_uses=max_uses,
            used_count=0,
            expires_at=dt.datetime.utcnow() + dt.timedelta(hours=hours),
            disabled=0,
        )
        db.add(row)
        db.commit()
        log_event(db, "auth.invite.created", user.id, {"code": code, "expires_in_hours": hours})
        return {
            "code": code,
            "max_uses": max_uses,
            "expires_at": row.expires_at.isoformat() + "Z",
        }
    finally:
        db.close()


@router.get("/auth/invites")
def auth_list_invites(request: Request, limit: int = 100):
    lim = max(1, min(int(limit or 100), 500))
    db = SessionLocal()
    try:
        user = _require_admin_user(db, request)
        rows = (
            db.query(InviteCode)
            .order_by(InviteCode.created_at.desc())
            .limit(lim)
            .all()
        )
        now = dt.datetime.utcnow()
        items = []
        for r in rows:
            expired = bool(r.expires_at and r.expires_at < now)
            items.append(
                {
                    "code": r.code,
                    "max_uses": int(r.max_uses or 1),
                    "used_count": int(r.used_count or 0),
                    "disabled": bool(int(r.disabled or 0)),
                    "expires_at": r.expires_at.isoformat() + "Z" if r.expires_at else None,
                    "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
                    "status": "expired" if expired else ("disabled" if int(r.disabled or 0) == 1 else "active"),
                }
            )
        return {"items": items}
    finally:
        db.close()


@router.get("/admin/summary")
def admin_summary(request: Request):
    db = SessionLocal()
    try:
        _ = _require_admin_user(db, request)
        now = dt.datetime.utcnow()
        total_users = db.query(User).count()
        active_users = db.query(User).filter(User.is_active == 1).count()
        paid_active = (
            db.query(User)
            .filter(User.is_active == 1, User.plan_name != "free", User.subscription_expires_at.isnot(None), User.subscription_expires_at > now)
            .count()
        )
        invite_total = db.query(InviteCode).count()
        invite_used = db.query(InviteCode).filter(InviteCode.used_count > 0).count()
        invite_active = (
            db.query(InviteCode)
            .filter(InviteCode.disabled == 0)
            .all()
        )
        invite_active_count = sum(1 for x in invite_active if not x.expires_at or x.expires_at > now)
        expiring_7d = (
            db.query(User)
            .filter(
                User.is_active == 1,
                User.subscription_expires_at.isnot(None),
                User.subscription_expires_at > now,
                User.subscription_expires_at <= now + dt.timedelta(days=7),
            )
            .count()
        )
        return {
            "total_users": total_users,
            "active_users": active_users,
            "paid_active": paid_active,
            "invite_total": invite_total,
            "invite_used": invite_used,
            "invite_active": invite_active_count,
            "expiring_7d": expiring_7d,
        }
    finally:
        db.close()


@router.get("/admin/config")
def admin_config_get(request: Request):
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        return _get_admin_config(db)
    finally:
        db.close()


@router.patch("/admin/config")
async def admin_config_patch(request: Request):
    payload = await request.json()
    patch = (payload or {}).get("config") or payload or {}
    if not isinstance(patch, dict):
        raise HTTPException(400, "config يجب أن يكون كائناً")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        row = db.query(AppSetting).filter(AppSetting.key == "admin_config").first()
        current = _get_admin_config(db)
        merged = {**current, **patch}
        if isinstance(current.get("social_links"), dict):
            social_patch = patch.get("social_links")
            if isinstance(social_patch, dict):
                merged["social_links"] = {**current["social_links"], **social_patch}
        if not row:
            row = AppSetting(key="admin_config", value_json=merged, updated_at=dt.datetime.utcnow())
            db.add(row)
        else:
            row.value_json = merged
            row.updated_at = dt.datetime.utcnow()
        db.commit()
        log_event(db, "admin.config.updated", admin.id, {"keys": list(patch.keys())})
        merged["smtp"] = smtp_status()
        return merged
    finally:
        db.close()


@router.post("/admin/smtp-test")
async def admin_smtp_test(request: Request):
    payload = await request.json()
    to_email = str((payload or {}).get("to_email", "")).strip().lower()
    if "@" not in to_email:
        raise HTTPException(400, "أدخل بريدًا صحيحًا")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        try:
            send_smtp_test_email(to_email)
        except Exception as e:
            raise HTTPException(400, f"فشل اختبار SMTP: {str(e) or 'تحقق من إعدادات البريد'}")
        log_event(db, "admin.smtp_test.sent", admin.id, {"to_email": to_email})
        return {"ok": True, "to_email": to_email}
    finally:
        db.close()


@router.get("/admin/backup")
def admin_backup(request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        users = db.query(User).order_by(User.created_at.desc()).all()
        reports = db.query(AnalysisReport).order_by(AnalysisReport.created_at.desc()).limit(1000).all()
        payload = {
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
            "by_admin": admin.username,
            "users": [
                {
                    "id": u.id,
                    "username": u.username,
                    "email": u.email,
                    "is_admin": int(u.is_admin or 0),
                    "is_active": int(u.is_active or 0),
                    "plan_name": u.plan_name,
                    "subscription_expires_at": u.subscription_expires_at.isoformat() + "Z" if u.subscription_expires_at else None,
                    "created_at": u.created_at.isoformat() + "Z" if u.created_at else None,
                }
                for u in users
            ],
            "reports": [
                {
                    "id": r.id,
                    "user_id": r.user_id,
                    "title": r.title,
                    "branch1_name": r.branch1_name,
                    "branch2_name": r.branch2_name,
                    "status": r.status,
                    "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
                    "total_ops": r.total_ops,
                    "matched_ops": r.matched_ops,
                    "mismatch_ops": r.mismatch_ops,
                    "errors_count": r.errors_count,
                    "warnings_count": r.warnings_count,
                    "archived": int(r.archived or 0),
                }
                for r in reports
            ],
        }
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        buff = io.BytesIO()
        with gzip.GzipFile(fileobj=buff, mode="wb") as gz:
            gz.write(raw)
        buff.seek(0)
        ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        headers = {"Content-Disposition": f'attachment; filename="auditflow-backup-{ts}.json.gz"'}
        return StreamingResponse(buff, media_type="application/gzip", headers=headers)
    finally:
        db.close()


@router.get("/admin/users")
def admin_users(request: Request, limit: int = 200):
    lim = max(1, min(int(limit or 200), 1000))
    db = SessionLocal()
    try:
        _ = _require_admin_user(db, request)
        rows = db.query(User).order_by(User.created_at.desc()).limit(lim).all()
        return {
            "items": [
                {
                    "id": u.id,
                    "username": u.username,
                    "email": u.email,
                    "is_admin": bool(int(u.is_admin or 0)),
                    "is_active": bool(int(u.is_active or 0)),
                    "plan_name": u.plan_name or "free",
                    "subscription_expires_at": u.subscription_expires_at.isoformat() + "Z" if u.subscription_expires_at else None,
                    "created_at": u.created_at.isoformat() + "Z" if u.created_at else None,
                }
                for u in rows
            ]
        }
    finally:
        db.close()


@router.patch("/admin/users")
async def admin_update_user(request: Request):
    payload = await request.json()
    user_id = str((payload or {}).get("user_id", "")).strip()
    if not user_id:
        raise HTTPException(400, "user_id مطلوب")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        target = db.query(User).filter(User.id == user_id).first()
        if not target:
            raise HTTPException(404, "المستخدم غير موجود")
        if "is_active" in payload:
            target.is_active = 1 if bool(payload.get("is_active")) else 0
            if int(target.is_active or 0) == 1:
                target.failed_attempts = 0
                target.locked_until = None
        if "plan_name" in payload:
            p = str(payload.get("plan_name") or "free").strip().lower()
            target.plan_name = p or "free"
        if "subscription_months" in payload:
            m = int(payload.get("subscription_months") or 0)
            if m > 0:
                base = target.subscription_expires_at if target.subscription_expires_at and target.subscription_expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
                target.subscription_expires_at = base + dt.timedelta(days=30 * m)
                target.is_active = 1
                target.failed_attempts = 0
                target.locked_until = None
                if target.plan_name.startswith("pending_"):
                    target.plan_name = target.plan_name.replace("pending_", "", 1) or "paid"
            else:
                target.subscription_expires_at = None
        db.commit()
        log_event(
            db,
            "admin.user.updated",
            admin.id,
            {
                "target_user_id": target.id,
                "is_active": bool(int(target.is_active or 0)),
                "plan_name": target.plan_name,
                "subscription_expires_at": target.subscription_expires_at.isoformat() if target.subscription_expires_at else None,
            },
        )
        return {"ok": True}
    finally:
        db.close()


@router.get("/auth/subscription-status")
def auth_subscription_status(request: Request):
    db = SessionLocal()
    try:
        user = current_user_from_request(db, request)
        if not user:
            raise HTTPException(401, "يرجى تسجيل الدخول أولاً")
        now = dt.datetime.utcnow()
        exp = user.subscription_expires_at
        days_left = None
        if exp:
            days_left = int((exp - now).total_seconds() // 86400)
        pending = str(user.plan_name or "").startswith("pending_")
        return {
            "is_admin": bool(int(user.is_admin or 0)),
            "is_active": bool(int(user.is_active or 0)),
            "plan_name": user.plan_name or "free",
            "subscription_expires_at": exp.isoformat() + "Z" if exp else None,
            "days_left": days_left,
            "pending_approval": pending,
        }
    finally:
        db.close()


@router.post("/auth/login")
@limiter.limit("25/minute")
async def auth_login(request: Request):
    payload = await request.json()
    username = str((payload or {}).get("username", "")).strip()
    password = str((payload or {}).get("password", "")).strip()

    db = SessionLocal()
    try:
        require_csrf(request)
        user = db.query(User).filter(func.lower(User.username) == username.lower()).first()
        if not user:
            raise HTTPException(401, "بيانات الدخول غير صحيحة")
        if int(user.is_active or 0) != 1:
            if str(user.plan_name or "").startswith("pending_"):
                raise HTTPException(403, "تم إنشاء الحساب وبانتظار اعتماد الاشتراك من المدير")
            raise HTTPException(403, admin_contact_text())
        if user.subscription_expires_at and user.subscription_expires_at < dt.datetime.utcnow():
            raise HTTPException(403, subscription_expired_text())
        if user.locked_until and user.locked_until > dt.datetime.utcnow():
            raise HTTPException(429, "الحساب مقفل مؤقتاً. حاول لاحقاً")
        if not verify_password(password, user.password_hash):
            user.failed_attempts = int(user.failed_attempts or 0) + 1
            if user.failed_attempts >= 5:
                user.locked_until = dt.datetime.utcnow() + dt.timedelta(minutes=LOCK_MINUTES)
                user.failed_attempts = 0
            db.commit()
            raise HTTPException(401, "بيانات الدخول غير صحيحة")

        if "$" not in user.password_hash:
            user.password_hash = hash_password(password)
        user.failed_attempts = 0
        user.locked_until = None
        db.commit()

        token = create_session(db, user.id)
        csrf = issue_csrf_token()
        log_event(db, "auth.login", user.id, {"username": username})
        res = Response(content='{"ok":true}', media_type="application/json")
        res.set_cookie(
            key=SESSION_COOKIE,
            value=token,
            path=COOKIE_PATH,
            httponly=True,
            samesite="lax",
            secure=cookie_secure(),
            max_age=session_max_age_seconds(),
        )
        res.set_cookie(
            key=CSRF_COOKIE,
            value=csrf,
            path=COOKIE_PATH,
            httponly=False,
            samesite="lax",
            secure=cookie_secure(),
            max_age=session_max_age_seconds(),
        )
        return res
    finally:
        db.close()


@router.post("/auth/logout")
def auth_logout(request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = current_user_from_request(db, request)
        token = request.cookies.get(SESSION_COOKIE)
        if token:
            s = db.query(UserSession).filter(UserSession.token == token).first()
            if s:
                db.delete(s)
                db.commit()
        if user:
            log_event(db, "auth.logout", user.id)
        res = Response(content='{"ok":true}', media_type="application/json")
        res.delete_cookie(SESSION_COOKIE, path=COOKIE_PATH)
        res.delete_cookie(CSRF_COOKIE, path=COOKIE_PATH)
        return res
    finally:
        db.close()


@router.post("/auth/change-password")
async def auth_change_password(request: Request):
    require_csrf(request)
    payload = await request.json()
    old_password = str((payload or {}).get("old_password", ""))
    new_password = str((payload or {}).get("new_password", ""))
    if len(new_password) < 8:
        raise HTTPException(400, "كلمة المرور الجديدة يجب أن تكون 8 أحرف على الأقل")
    db = SessionLocal()
    try:
        user = require_user(db, request)
        if not verify_password(old_password, user.password_hash):
            raise HTTPException(400, "كلمة المرور الحالية غير صحيحة")
        user.password_hash = hash_password(new_password)
        db.commit()
        log_event(db, "auth.change_password", user.id)
        return {"ok": True}
    finally:
        db.close()
