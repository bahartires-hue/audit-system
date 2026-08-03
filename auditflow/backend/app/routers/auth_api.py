from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import os
import secrets
import uuid

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import DateTime, func, select, text

from ..auth_core import (
    COOKIE_PATH,
    CSRF_COOKIE,
    IMPERSONATOR_COOKIE,
    LOCK_MINUTES,
    PAGE_KEYS,
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
from ..db import Base, SessionLocal
from ..mailer import send_password_reset_email, send_plain_email, send_smtp_test_email, smtp_status
from ..models import AnalysisReport, AppSetting, AuditLog, Document, InviteCode, PasswordResetToken, User, UserSession
from ..rate_limit import limiter

router = APIRouter(tags=["auth"])


def _default_admin_config() -> dict:
    return {
        "admin_contact": (os.getenv("AUDITFLOW_ADMIN_CONTACT") or "").strip(),
        "company_name": "OptimalSuite AI",
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
    # Bootstrap rule:
    # - If there are no users, first account is admin.
    # - If users exist but no admin account exists, allow bootstrap admin registration too.
    any_user = db.query(User.id).first()
    if not any_user:
        return True
    any_admin = db.query(User.id).filter(User.is_admin == 1).first()
    if not any_admin:
        return True
    admin_email = (os.getenv("AUDITFLOW_ADMIN_EMAIL") or "").strip().lower()
    admin_username = (os.getenv("AUDITFLOW_ADMIN_USERNAME") or "").strip()
    if admin_email and email == admin_email:
        return True
    if admin_username and username == admin_username:
        return True
    return False


def _ensure_first_user_admin(db) -> None:
    """
    ضمان الطوارئ: أول مستخدم في النظام يجب أن يكون مديرًا.
    يفيد في الحالات القديمة التي سُجل فيها أول حساب بدون صلاحيات الإدارة.
    """
    first_user = db.query(User).order_by(User.created_at.asc(), User.id.asc()).first()
    if not first_user:
        return
    if int(first_user.is_admin or 0) == 1:
        return
    first_user.is_admin = 1
    first_user.role_name = "admin"
    first_user.is_active = 1
    db.commit()


def _require_admin_user(db, request: Request) -> User:
    user = require_user(db, request)
    if int(user.is_admin or 0) != 1:
        raise HTTPException(403, "هذه العملية للمدير فقط")
    return user


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _has_role(user: User, required: str) -> bool:
    req = (required or "").strip().lower()
    role = (getattr(user, "role_name", "") or "user").strip().lower()
    if int(getattr(user, "is_admin", 0) or 0) == 1:
        return True
    if req in ("", "user"):
        return True
    return role == req


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
                    "role_name": (u.role_name if u else "user"),
                    "is_active": is_active,
                    "plan_name": plan_name,
                    "subscription_expires_at": exp.isoformat() + "Z" if exp else None,
                    "allowed_pages": (u.allowed_pages or []) if u else [],
                    "impersonated": bool(request.cookies.get(IMPERSONATOR_COOKIE)),
                    "roles": {
                        "user": bool(u),
                        "support": _has_role(u, "support") if u else False,
                        "auditor": _has_role(u, "auditor") if u else False,
                        "manager": _has_role(u, "manager") if u else False,
                        "admin": _has_role(u, "admin") if u else False,
                    },
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
        _ensure_first_user_admin(db)
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
            role_name="admin" if bootstrap_admin else "user",
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
        deactivated_count = db.query(User).filter(User.is_active == 0).count()
        documents_total = db.query(Document).count()
        reports_total = db.query(AnalysisReport).count()
        today_start = dt.datetime(now.year, now.month, now.day)
        ops_today = db.query(AuditLog).filter(AuditLog.created_at >= today_start).count()

        last_backup = (
            db.query(AuditLog)
            .filter(AuditLog.action == "admin.backup.downloaded")
            .order_by(AuditLog.created_at.desc())
            .first()
        )
        days_since_backup = None
        if last_backup and last_backup.created_at:
            days_since_backup = max(0, (now - last_backup.created_at).days)

        # سلسلة آخر 30 يومًا: مستخدمون جدد واشتراكات مدفوعة جديدة لكل يوم.
        chart_days = 30
        start_day = today_start - dt.timedelta(days=chart_days - 1)
        new_users_rows = (
            db.query(func.date(User.created_at), func.count(User.id))
            .filter(User.created_at >= start_day)
            .group_by(func.date(User.created_at))
            .all()
        )
        new_paid_rows = (
            db.query(func.date(User.created_at), func.count(User.id))
            .filter(User.created_at >= start_day, User.plan_name != "free")
            .group_by(func.date(User.created_at))
            .all()
        )
        new_users_map = {str(d): int(c) for d, c in new_users_rows}
        new_paid_map = {str(d): int(c) for d, c in new_paid_rows}
        labels = []
        new_users_series = []
        new_paid_series = []
        for i in range(chart_days):
            day = start_day + dt.timedelta(days=i)
            key = day.strftime("%Y-%m-%d")
            labels.append(day.strftime("%m-%d"))
            new_users_series.append(new_users_map.get(key, 0))
            new_paid_series.append(new_paid_map.get(key, 0))

        return {
            "total_users": total_users,
            "active_users": active_users,
            "paid_active": paid_active,
            "invite_total": invite_total,
            "invite_used": invite_used,
            "invite_active": invite_active_count,
            "expiring_7d": expiring_7d,
            "deactivated_count": deactivated_count,
            "documents_total": documents_total,
            "reports_total": reports_total,
            "ops_today": ops_today,
            "days_since_backup": days_since_backup,
            "chart_30d": {
                "labels": labels,
                "new_users": new_users_series,
                "new_paid": new_paid_series,
            },
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


# ---------------------------------------------------------------------------
# Full-system backup / restore
#
# Backs up EVERY table registered on the SQLAlchemy metadata (Base.metadata),
# not just users + AnalysisReport, so that "استعادة" truly restores the
# system as it was. Two tables are intentionally excluded because they hold
# short-lived security material rather than business data (see
# _BACKUP_EXCLUDED_TABLES below); everything else -- items, purchases,
# sales, returns, expenses, stock movements, branches, customers,
# suppliers, HR/payroll data, documents, settings, analysis reports, etc.
# -- is included automatically, in FK-safe (parent-before-child) order.
# ---------------------------------------------------------------------------

# Tables intentionally left out of backup/restore: these hold live login
# sessions and single-use password-reset tokens, not business data, and
# reviving stale ones on restore would be a security concern rather than a
# recovery benefit.
_BACKUP_EXCLUDED_TABLES = {"user_sessions", "password_reset_tokens"}

# On restore, if a `users` row already exists we never overwrite these
# security-sensitive fields from the backup file -- a restore should bring
# back business data, not silently swap out the live admin's password hash,
# admin flag, or lockout state.
_USERS_PROTECTED_FIELDS_ON_UPDATE = {"password_hash", "is_admin", "failed_attempts", "locked_until"}


def _backup_tables_ordered() -> list:
    return [t for t in Base.metadata.sorted_tables if t.name not in _BACKUP_EXCLUDED_TABLES]


def _serialize_backup_value(v):
    if isinstance(v, dt.datetime):
        return v.isoformat() + "Z"
    if isinstance(v, dt.date):
        return v.isoformat()
    return v


def _deserialize_backup_value(col, v):
    if v is None:
        return None
    if isinstance(col.type, DateTime):
        try:
            return dt.datetime.fromisoformat(str(v).replace("Z", ""))
        except Exception:
            return None
    return v


def _live_table_counts(db) -> dict:
    counts = {}
    for table in _backup_tables_ordered():
        counts[table.name] = int(db.execute(select(func.count()).select_from(table)).scalar_one())
    return counts


def _bump_serial_sequence(db, table) -> None:
    """After restoring explicit ids into an autoincrement PK column (Postgres),
    advance the column's sequence past the highest restored id so future
    inserts never collide with a restored row. No-op on SQLite (no sequences)."""
    if db.bind is None or db.bind.dialect.name != "postgresql":
        return
    for col in table.primary_key.columns:
        if not col.autoincrement:
            continue
        db.execute(
            text(
                f'SELECT setval(pg_get_serial_sequence(:tbl, :col), '
                f'COALESCE((SELECT MAX("{col.name}") FROM "{table.name}"), 0) + 1, false)'
            ),
            {"tbl": table.name, "col": col.name},
        )


def _restore_table_rows(db, table, rows: list) -> dict:
    """Restores one table's rows with per-row isolation (each row runs in its
    own SAVEPOINT): a single malformed/legacy row is skipped and reported,
    it never aborts the rest of the table or the rest of the restore."""
    pk_cols = list(table.primary_key.columns)
    if not pk_cols:
        return {"inserted": 0, "updated": 0, "skipped": len(rows), "row_errors": ["لا يوجد مفتاح أساسي لهذا الجدول"]}
    pk_names = [c.name for c in pk_cols]
    inserted = 0
    updated = 0
    skipped = 0
    any_inserted = False
    row_errors = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict) or any(row.get(pk) in (None, "") for pk in pk_names):
            skipped += 1
            continue
        values = {}
        for col in table.columns:
            if col.name in row:
                values[col.name] = _deserialize_backup_value(col, row[col.name])
        pk_filter = [table.c[pk] == values.get(pk, row.get(pk)) for pk in pk_names]
        try:
            with db.begin_nested():
                exists = db.execute(select(*[table.c[pk] for pk in pk_names]).where(*pk_filter)).first()
                if exists:
                    update_values = dict(values)
                    if table.name == "users":
                        for f in _USERS_PROTECTED_FIELDS_ON_UPDATE:
                            update_values.pop(f, None)
                    if update_values:
                        db.execute(table.update().where(*pk_filter).values(**update_values))
                    updated += 1
                else:
                    db.execute(table.insert().values(**values))
                    inserted += 1
                    any_inserted = True
        except Exception as e:
            skipped += 1
            pk_desc = ",".join(str(row.get(pk)) for pk in pk_names)
            row_errors.append(f"صف #{idx} (المفتاح={pk_desc}): {str(e).splitlines()[0][:180]}")
    if any_inserted:
        try:
            with db.begin_nested():
                _bump_serial_sequence(db, table)
        except Exception:
            pass
    result = {"inserted": inserted, "updated": updated, "skipped": skipped}
    if row_errors:
        result["row_errors"] = row_errors[:20] + ([f"... و {len(row_errors) - 20} أخطاء إضافية"] if len(row_errors) > 20 else [])
    return result


@router.get("/admin/backup/summary")
def admin_backup_summary(request: Request):
    """Live per-table record counts, so the admin can see exactly what a
    backup would contain BEFORE downloading it."""
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        counts = _live_table_counts(db)
        return {
            "ok": True,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
            "counts": counts,
            "total_tables": len(counts),
            "total_records": sum(counts.values()),
            "excluded_tables": sorted(_BACKUP_EXCLUDED_TABLES),
        }
    finally:
        db.close()


@router.get("/admin/backup")
def admin_backup(request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        tables = {}
        counts = {}
        for table in _backup_tables_ordered():
            rows = db.execute(table.select()).mappings().all()
            tables[table.name] = [
                {col_name: _serialize_backup_value(val) for col_name, val in row.items()} for row in rows
            ]
            counts[table.name] = len(tables[table.name])
        payload = {
            "format": "auditflow-full-backup",
            "schema_version": 2,
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
            "by_admin": admin.username,
            "table_order": [t.name for t in _backup_tables_ordered()],
            "excluded_tables": sorted(_BACKUP_EXCLUDED_TABLES),
            "counts": counts,
            "tables": tables,
        }
        raw = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        buff = io.BytesIO()
        with gzip.GzipFile(fileobj=buff, mode="wb") as gz:
            gz.write(raw)
        buff.seek(0)
        ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        headers = {"Content-Disposition": f'attachment; filename="auditflow-backup-{ts}.json.gz"'}
        log_event(db, "admin.backup.downloaded", admin.id, {"counts": counts})
        return StreamingResponse(buff, media_type="application/gzip", headers=headers)
    finally:
        db.close()


@router.post("/admin/backup/validate")
async def admin_backup_validate(request: Request, backup_file: UploadFile = File(...)):
    """Upload-only inspection: reports per-table record counts found in the
    file. Does NOT write anything to the database."""
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        raw = await backup_file.read()
        try:
            if backup_file.filename and backup_file.filename.lower().endswith(".gz"):
                raw = gzip.decompress(raw)
            parsed = json.loads(raw.decode("utf-8"))
        except Exception:
            raise HTTPException(400, "ملف النسخة الاحتياطية غير صالح")

        tables = parsed.get("tables")
        if isinstance(tables, dict):
            counts = {name: (len(rows) if isinstance(rows, list) else 0) for name, rows in tables.items()}
            log_event(
                db,
                "admin.backup.validated",
                admin.id,
                {"counts": counts, "format": parsed.get("format", "auditflow-full-backup"), "filename": backup_file.filename or "backup"},
            )
            return {
                "ok": True,
                "format": parsed.get("format", "auditflow-full-backup"),
                "counts": counts,
                "total_tables": len(counts),
                "total_records": sum(counts.values()),
            }

        # Backward compatibility with the old users-only backup format.
        legacy_users = parsed.get("users")
        legacy_reports = parsed.get("reports")
        if isinstance(legacy_users, list) or isinstance(legacy_reports, list):
            counts = {"users": len(legacy_users or []), "reports (analysis_reports فقط)": len(legacy_reports or [])}
            log_event(
                db,
                "admin.backup.validated",
                admin.id,
                {"counts": counts, "format": "legacy", "filename": backup_file.filename or "backup"},
            )
            return {"ok": True, "format": "legacy", "counts": counts, "total_tables": len(counts), "total_records": sum(counts.values())}

        raise HTTPException(400, "بنية النسخة الاحتياطية غير صحيحة")
    finally:
        db.close()


@router.get("/admin/audit")
def admin_audit(
    request: Request,
    limit: int = 200,
    action: str = "",
    user_id: str = "",
    q: str = "",
):
    lim = max(1, min(int(limit or 200), 1000))
    db = SessionLocal()
    try:
        _ = _require_admin_user(db, request)
        query = db.query(AuditLog).order_by(AuditLog.created_at.desc())
        if action.strip():
            query = query.filter(AuditLog.action == action.strip())
        if user_id.strip():
            query = query.filter(AuditLog.user_id == user_id.strip())
        rows = query.limit(lim).all()
        qn = q.strip().lower()
        items = []
        for x in rows:
            meta = x.meta_json if isinstance(x.meta_json, dict) else {}
            if qn:
                raw = f"{x.action} {x.user_id or ''} {json.dumps(meta, ensure_ascii=False)}".lower()
                if qn not in raw:
                    continue
            items.append(
                {
                    "id": x.id,
                    "user_id": x.user_id,
                    "action": x.action,
                    "meta": meta,
                    "created_at": x.created_at.isoformat() + "Z" if x.created_at else None,
                }
            )
        return {"items": items}
    finally:
        db.close()


# ============================================================ الأمان (لوحة المدير)
@router.get("/admin/security/summary")
def admin_security_summary(request: Request):
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        now = dt.datetime.utcnow()
        locked_accounts = db.query(User).filter(User.locked_until.isnot(None), User.locked_until > now).count()
        active_sessions = db.query(UserSession).filter(UserSession.expires_at > now).count()
        since_today = dt.datetime(now.year, now.month, now.day)
        failed_today = (
            db.query(AuditLog)
            .filter(AuditLog.action == "auth.login.failed", AuditLog.created_at >= since_today)
            .count()
        )
        since_7d = now - dt.timedelta(days=7)
        failed_7d = (
            db.query(AuditLog)
            .filter(AuditLog.action == "auth.login.failed", AuditLog.created_at >= since_7d)
            .count()
        )
        return {
            "locked_accounts": locked_accounts,
            "active_sessions": active_sessions,
            "failed_logins_today": failed_today,
            "failed_logins_7d": failed_7d,
        }
    finally:
        db.close()


@router.get("/admin/security/failed-logins")
def admin_security_failed_logins(request: Request, limit: int = 50):
    lim = max(1, min(int(limit or 50), 500))
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        rows = (
            db.query(AuditLog)
            .filter(AuditLog.action == "auth.login.failed")
            .order_by(AuditLog.created_at.desc())
            .limit(lim)
            .all()
        )
        items = []
        for x in rows:
            meta = x.meta_json if isinstance(x.meta_json, dict) else {}
            items.append(
                {
                    "username": meta.get("username", ""),
                    "ip": meta.get("ip", ""),
                    "locked": bool(meta.get("locked", False)),
                    "created_at": x.created_at.isoformat() + "Z" if x.created_at else None,
                }
            )
        return {"items": items}
    finally:
        db.close()


@router.get("/admin/security/sessions")
def admin_security_sessions(request: Request, limit: int = 100):
    lim = max(1, min(int(limit or 100), 500))
    db = SessionLocal()
    try:
        _require_admin_user(db, request)
        now = dt.datetime.utcnow()
        rows = (
            db.query(UserSession)
            .filter(UserSession.expires_at > now)
            .order_by(UserSession.created_at.desc())
            .limit(lim)
            .all()
        )
        user_ids = list({r.user_id for r in rows})
        users = {u.id: u for u in db.query(User).filter(User.id.in_(user_ids)).all()} if user_ids else {}
        items = []
        for r in rows:
            u = users.get(r.user_id)
            items.append(
                {
                    "token_id": r.token[:8] + "…" if r.token else "",
                    "token_full": r.token,
                    "user_id": r.user_id,
                    "username": u.username if u else "(محذوف)",
                    "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
                    "expires_at": r.expires_at.isoformat() + "Z" if r.expires_at else None,
                }
            )
        return {"items": items}
    finally:
        db.close()


@router.post("/admin/security/sessions/revoke")
async def admin_security_revoke_session(request: Request):
    payload = await request.json()
    token = str((payload or {}).get("token", "")).strip()
    if not token:
        raise HTTPException(400, "token مطلوب")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        sess = db.query(UserSession).filter(UserSession.token == token).first()
        if not sess:
            raise HTTPException(404, "الجلسة غير موجودة أو منتهية بالفعل")
        target_user_id = sess.user_id
        db.delete(sess)
        db.commit()
        log_event(db, "admin.security.session_revoked", admin.id, {"target_user_id": target_user_id})
        return {"ok": True}
    finally:
        db.close()


@router.post("/admin/security/unlock")
async def admin_security_unlock(request: Request):
    payload = await request.json()
    user_id = str((payload or {}).get("user_id", "")).strip()
    if not user_id:
        raise HTTPException(400, "user_id مطلوب")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        target = db.query(User).filter(User.id == user_id).first()
        if not target:
            raise HTTPException(404, "المستخدم غير موجود")
        target.failed_attempts = 0
        target.locked_until = None
        db.commit()
        log_event(db, "admin.security.unlock", admin.id, {"target_user_id": user_id, "username": target.username})
        return {"ok": True, "username": target.username}
    finally:
        db.close()


@router.post("/admin/notify-expiring")
async def admin_notify_expiring(request: Request):
    payload = await request.json()
    days = max(1, min(int((payload or {}).get("days", 7) or 7), 30))
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        now = dt.datetime.utcnow()
        rows = (
            db.query(User)
            .filter(
                User.is_active == 1,
                User.subscription_expires_at.isnot(None),
                User.subscription_expires_at > now,
                User.subscription_expires_at <= now + dt.timedelta(days=days),
                User.email.isnot(None),
            )
            .all()
        )
        sent = 0
        failed = 0
        for u in rows:
            try:
                send_plain_email(
                    to_email=str(u.email),
                    subject="تنبيه قرب انتهاء الاشتراك | OptimalSuite AI",
                    body=(
                        f"مرحباً {u.username},\n\n"
                        f"اشتراكك ({u.plan_name}) سينتهي قريبًا في {u.subscription_expires_at}.\n"
                        "يرجى التواصل مع الإدارة للتجديد.\n"
                    ),
                )
                sent += 1
            except Exception:
                failed += 1
        log_event(
            db,
            "admin.notify_expiring.sent",
            admin.id,
            {"days": days, "targets": len(rows), "sent": sent, "failed": failed},
        )
        return {"ok": True, "targets": len(rows), "sent": sent, "failed": failed}
    finally:
        db.close()


@router.get("/admin/users")
def admin_users(request: Request, limit: int = 200):
    lim = max(1, min(int(limit or 200), 1000))
    db = SessionLocal()
    try:
        _ = _require_admin_user(db, request)
        rows = db.query(User).order_by(User.created_at.desc()).limit(lim).all()
        user_ids = [u.id for u in rows]
        last_login_map = {}
        last_logout_map = {}
        reports_count_map = {}
        if user_ids:
            last_login_map = dict(
                db.query(AuditLog.user_id, func.max(AuditLog.created_at))
                .filter(AuditLog.action == "auth.login", AuditLog.user_id.in_(user_ids))
                .group_by(AuditLog.user_id)
                .all()
            )
            last_logout_map = dict(
                db.query(AuditLog.user_id, func.max(AuditLog.created_at))
                .filter(AuditLog.action == "auth.logout", AuditLog.user_id.in_(user_ids))
                .group_by(AuditLog.user_id)
                .all()
            )
            reports_count_map = dict(
                db.query(AnalysisReport.user_id, func.count(AnalysisReport.id))
                .filter(AnalysisReport.user_id.in_(user_ids))
                .group_by(AnalysisReport.user_id)
                .all()
            )
        return {
            "items": [
                {
                    "id": u.id,
                    "username": u.username,
                    "email": u.email,
                    "is_admin": bool(int(u.is_admin or 0)),
                    "role_name": (u.role_name or ("admin" if int(u.is_admin or 0) == 1 else "user")),
                    "is_active": bool(int(u.is_active or 0)),
                    "plan_name": u.plan_name or "free",
                    "subscription_expires_at": u.subscription_expires_at.isoformat() + "Z" if u.subscription_expires_at else None,
                    "created_at": u.created_at.isoformat() + "Z" if u.created_at else None,
                    "allowed_pages": u.allowed_pages or [],
                    "locked_until": u.locked_until.isoformat() + "Z" if u.locked_until else None,
                    "last_login_at": (last_login_map.get(u.id).isoformat() + "Z") if last_login_map.get(u.id) else None,
                    "last_logout_at": (last_logout_map.get(u.id).isoformat() + "Z") if last_logout_map.get(u.id) else None,
                    "reports_count": int(reports_count_map.get(u.id, 0) or 0),
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
        if "role_name" in payload:
            role = str(payload.get("role_name") or "user").strip().lower()
            allowed = {"user", "auditor", "support", "manager", "admin"}
            if role not in allowed:
                raise HTTPException(400, "role_name غير مدعوم")
            target.role_name = role
            if role == "admin":
                target.is_admin = 1
        if "allowed_pages" in payload:
            raw_pages = payload.get("allowed_pages") or []
            if not isinstance(raw_pages, list):
                raise HTTPException(400, "allowed_pages يجب أن تكون قائمة")
            cleaned = []
            for p in raw_pages:
                p = str(p or "").strip()
                if p in PAGE_KEYS and p not in cleaned:
                    cleaned.append(p)
            target.allowed_pages = cleaned
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
                "role_name": target.role_name,
                "subscription_expires_at": target.subscription_expires_at.isoformat() if target.subscription_expires_at else None,
            },
        )
        return {"ok": True}
    finally:
        db.close()


@router.post("/admin/users/create")
async def admin_create_user(request: Request):
    payload = await request.json()
    username = str((payload or {}).get("username", "")).strip()
    email = str((payload or {}).get("email", "")).strip().lower()
    password = str((payload or {}).get("password", "")).strip()
    plan_name = str((payload or {}).get("plan_name", "free")).strip().lower() or "free"
    is_active = 1 if bool((payload or {}).get("is_active", True)) else 0
    if len(username) < 3:
        raise HTTPException(400, "اسم المستخدم قصير")
    if "@" not in email or "." not in email:
        raise HTTPException(400, "البريد الإلكتروني غير صالح")
    if len(password) < 4:
        raise HTTPException(400, "كلمة المرور قصيرة")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        exists = db.query(User).filter(func.lower(User.username) == username.lower()).first()
        if exists:
            raise HTTPException(400, "اسم المستخدم موجود بالفعل")
        exists_email = db.query(User).filter(User.email == email).first()
        if exists_email:
            raise HTTPException(400, "البريد الإلكتروني مستخدم بالفعل")
        user = User(
            id=uuid.uuid4().hex,
            username=username,
            email=email,
            is_admin=0,
            role_name="user",
            is_active=is_active,
            plan_name=plan_name,
            subscription_expires_at=None,
            password_hash=hash_password(password),
            preferences_json={},
        )
        db.add(user)
        db.commit()
        log_event(db, "admin.user.created", admin.id, {"target_user_id": user.id, "username": username, "email": email})
        return {"ok": True, "id": user.id, "username": user.username}
    finally:
        db.close()


@router.delete("/admin/users/{user_id}")
async def admin_delete_user(user_id: str, request: Request):
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        target = db.query(User).filter(User.id == user_id).first()
        if not target:
            raise HTTPException(404, "المستخدم غير موجود")
        if target.id == admin.id:
            raise HTTPException(400, "لا يمكنك حذف حسابك الخاص")
        if int(target.is_admin or 0) == 1:
            raise HTTPException(400, "لا يمكن حذف حساب مدير آخر")
        reports_count = db.query(AnalysisReport).filter(AnalysisReport.user_id == target.id).count()
        docs_count = db.query(Document).filter(Document.user_id == target.id).count()
        if reports_count or docs_count:
            raise HTTPException(
                400,
                f"لا يمكن حذف هذا المستخدم لوجود {reports_count} تقرير و{docs_count} مستند مرتبطين به — عطّليه بدلاً من ذلك",
            )
        db.query(UserSession).filter(UserSession.user_id == target.id).delete()
        username_snapshot = target.username
        db.delete(target)
        db.commit()
        log_event(db, "admin.user.deleted", admin.id, {"target_user_id": user_id, "username": username_snapshot})
        return {"ok": True}
    except HTTPException:
        raise
    except Exception:
        db.rollback()
        raise HTTPException(400, "تعذر حذف المستخدم (قد تكون له بيانات مرتبطة أخرى)")
    finally:
        db.close()


@router.post("/admin/users/reset-password")
async def admin_reset_password(request: Request):
    payload = await request.json()
    user_id = str((payload or {}).get("user_id", "")).strip()
    if not user_id:
        raise HTTPException(400, "user_id مطلوب")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        target = db.query(User).filter(User.id == user_id).first()
        if not target:
            raise HTTPException(404, "المستخدم غير موجود")
        new_password = secrets.token_urlsafe(9)
        target.password_hash = hash_password(new_password)
        target.failed_attempts = 0
        target.locked_until = None
        db.commit()
        log_event(db, "admin.user.password_reset", admin.id, {"target_user_id": user_id, "username": target.username})
        return {"ok": True, "new_password": new_password, "username": target.username}
    finally:
        db.close()


@router.post("/admin/notify-all")
async def admin_notify_all(request: Request):
    payload = await request.json()
    subject = str((payload or {}).get("subject", "")).strip()
    body = str((payload or {}).get("body", "")).strip()
    only_active = bool((payload or {}).get("only_active", True))
    if not subject or not body:
        raise HTTPException(400, "العنوان والنص مطلوبان")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        query = db.query(User).filter(User.email.isnot(None))
        if only_active:
            query = query.filter(User.is_active == 1)
        rows = query.all()
        sent = 0
        failed = 0
        for u in rows:
            try:
                send_plain_email(to_email=str(u.email), subject=subject, body=body)
                sent += 1
            except Exception:
                failed += 1
        log_event(
            db,
            "admin.notify.broadcast",
            admin.id,
            {"subject": subject, "targets": len(rows), "sent": sent, "failed": failed, "only_active": only_active},
        )
        return {"ok": True, "targets": len(rows), "sent": sent, "failed": failed}
    finally:
        db.close()


@router.post("/admin/impersonate")
async def admin_impersonate(request: Request):
    payload = await request.json()
    user_id = str((payload or {}).get("user_id", "")).strip()
    if not user_id:
        raise HTTPException(400, "user_id مطلوب")
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        target = db.query(User).filter(User.id == user_id).first()
        if not target:
            raise HTTPException(404, "المستخدم غير موجود")
        if target.id == admin.id:
            raise HTTPException(400, "لا يمكنك انتحال حسابك الخاص")
        if int(target.is_admin or 0) == 1:
            raise HTTPException(400, "لا يمكن تسجيل الدخول كمدير آخر")
        original_token = request.cookies.get(SESSION_COOKIE, "")
        if not original_token:
            raise HTTPException(400, "تعذر تحديد جلسة المدير الحالية")
        new_token = create_session(db, target.id)
        csrf = issue_csrf_token()
        log_event(
            db, "admin.impersonate.start", admin.id, {"target_user_id": target.id, "target_username": target.username}
        )
        res = Response(content=json.dumps({"ok": True, "target_username": target.username}), media_type="application/json")
        res.set_cookie(
            key=SESSION_COOKIE, value=new_token, path=COOKIE_PATH, httponly=True,
            samesite="lax", secure=cookie_secure(), max_age=session_max_age_seconds(),
        )
        res.set_cookie(
            key=CSRF_COOKIE, value=csrf, path=COOKIE_PATH, httponly=False,
            samesite="lax", secure=cookie_secure(), max_age=session_max_age_seconds(),
        )
        res.set_cookie(
            key=IMPERSONATOR_COOKIE, value=original_token, path=COOKIE_PATH, httponly=True,
            samesite="lax", secure=cookie_secure(), max_age=session_max_age_seconds(),
        )
        return res
    finally:
        db.close()


@router.post("/admin/impersonate/stop")
async def admin_impersonate_stop(request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        original_token = request.cookies.get(IMPERSONATOR_COOKIE, "")
        if not original_token:
            raise HTTPException(400, "لا توجد جلسة إدارية للعودة إليها")
        sess = db.query(UserSession).filter(UserSession.token == original_token).first()
        if not sess or sess.expires_at < dt.datetime.utcnow():
            raise HTTPException(400, "انتهت صلاحية جلسة المدير الأصلية، يرجى تسجيل الدخول من جديد")
        admin_user = db.query(User).filter(User.id == sess.user_id).first()
        if not admin_user or int(admin_user.is_admin or 0) != 1:
            raise HTTPException(400, "الجلسة الأصلية لم تعد جلسة مدير صالحة")
        current_user = current_user_from_request(db, request)
        csrf = issue_csrf_token()
        log_event(
            db,
            "admin.impersonate.stop",
            admin_user.id,
            {"was_impersonating_user_id": current_user.id if current_user else None},
        )
        res = Response(content='{"ok":true}', media_type="application/json")
        res.set_cookie(
            key=SESSION_COOKIE, value=original_token, path=COOKIE_PATH, httponly=True,
            samesite="lax", secure=cookie_secure(), max_age=session_max_age_seconds(),
        )
        res.set_cookie(
            key=CSRF_COOKIE, value=csrf, path=COOKIE_PATH, httponly=False,
            samesite="lax", secure=cookie_secure(), max_age=session_max_age_seconds(),
        )
        res.delete_cookie(IMPERSONATOR_COOKIE, path=COOKIE_PATH)
        return res
    finally:
        db.close()


@router.post("/admin/backup/restore")
async def admin_backup_restore(request: Request, backup_file: UploadFile = File(...)):
    """Restores every table present in the uploaded backup file. Semantics
    are additive/merge, never destructive: existing rows are matched by
    primary key and updated with the backup's values, rows missing from the
    live database are inserted, and rows that exist live but are NOT present
    in the backup file are left untouched (no deletes, ever). The whole
    operation is one transaction -- if anything fails, nothing is written."""
    db = SessionLocal()
    try:
        admin = _require_admin_user(db, request)
        require_csrf(request)
        raw = await backup_file.read()
        try:
            if backup_file.filename and backup_file.filename.lower().endswith(".gz"):
                raw = gzip.decompress(raw)
            parsed = json.loads(raw.decode("utf-8"))
        except Exception:
            raise HTTPException(400, "ملف النسخة الاحتياطية غير صالح")

        tables_payload = parsed.get("tables")
        if not isinstance(tables_payload, dict):
            # Backward compatibility: restore an old users-only backup file.
            legacy_users = parsed.get("users")
            if not isinstance(legacy_users, list):
                raise HTTPException(400, "بنية النسخة الاحتياطية غير صحيحة")
            tables_payload = {"users": legacy_users}

        table_map = {t.name: t for t in _backup_tables_ordered()}
        unknown_tables = [n for n in tables_payload.keys() if n not in table_map]
        by_table = {}
        errors = {}
        # Each row is restored in its own SAVEPOINT (see _restore_table_rows),
        # so one malformed row/table never blocks the rest of the restore --
        # everything that CAN be safely restored, IS restored. We only abort
        # the whole operation (and commit nothing) if something catastrophic
        # happens outside that per-row isolation, e.g. the commit itself fails.
        try:
            for table in _backup_tables_ordered():
                name = table.name
                if name not in tables_payload:
                    continue
                rows = tables_payload.get(name)
                if not isinstance(rows, list):
                    errors[name] = "بيانات الجدول ليست قائمة"
                    continue
                try:
                    by_table[name] = _restore_table_rows(db, table, rows)
                except Exception as e:
                    errors[name] = str(e).splitlines()[0][:220]
            db.commit()
        except Exception as e:
            db.rollback()
            raise HTTPException(400, f"فشلت عملية الاستعادة بالكامل ولم يتم تعديل أي بيانات: {str(e)}")

        total_inserted = sum(v.get("inserted", 0) for v in by_table.values())
        total_updated = sum(v.get("updated", 0) for v in by_table.values())
        total_skipped = sum(v.get("skipped", 0) for v in by_table.values())
        log_event(
            db,
            "admin.backup.restored",
            admin.id,
            {
                "tables": by_table,
                "unknown_tables_in_file": unknown_tables,
                "errors": errors,
                "filename": backup_file.filename or "backup",
            },
        )
        return {
            "ok": True,
            "tables_restored": by_table,
            "unknown_tables_in_file": unknown_tables,
            "errors": errors,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "total_skipped": total_skipped,
        }
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
            "role_name": user.role_name or ("admin" if int(user.is_admin or 0) == 1 else "user"),
            "is_active": bool(int(user.is_active or 0)),
            "plan_name": user.plan_name or "free",
            "subscription_expires_at": exp.isoformat() + "Z" if exp else None,
            "days_left": days_left,
            "pending_approval": pending,
        }
    finally:
        db.close()


_DUMMY_PASSWORD_HASH = hash_password(secrets.token_urlsafe(32))


@router.post("/auth/login")
@limiter.limit("25/minute")
async def auth_login(request: Request):
    payload = await request.json()
    username = str((payload or {}).get("username", "")).strip()
    password = str((payload or {}).get("password", "")).strip()

    db = SessionLocal()
    try:
        require_csrf(request)
        _ensure_first_user_admin(db)
        user = db.query(User).filter(func.lower(User.username) == username.lower()).first()

        # نتحقق من كلمة المرور أولاً، قبل أي فحص لحالة الحساب (نشط/منتهي/مقفول).
        # هذا يمنع استغلال الرد كـ "oracle" لمعرفة وجود اسم مستخدم أو حالته بدون
        # معرفة كلمة المرور الصحيحة. لو المستخدم غير موجود، نقارن بهاش وهمي بنفس
        # التكلفة الحسابية حتى لا يكشف زمن الاستجابة عن وجود الحساب من عدمه.
        password_ok = verify_password(password, user.password_hash if user else _DUMMY_PASSWORD_HASH)

        if not user or not password_ok:
            ip = _client_ip(request)
            if user:
                user.failed_attempts = int(user.failed_attempts or 0) + 1
                locked_now = False
                if user.failed_attempts >= 5:
                    user.locked_until = dt.datetime.utcnow() + dt.timedelta(minutes=LOCK_MINUTES)
                    user.failed_attempts = 0
                    locked_now = True
                db.commit()
                log_event(db, "auth.login.failed", user.id, {"username": username, "ip": ip, "locked": locked_now})
            else:
                log_event(db, "auth.login.failed", None, {"username": username, "ip": ip, "locked": False})
            raise HTTPException(401, "بيانات الدخول غير صحيحة")

        # كلمة المرور صحيحة — الآن فقط نكشف حالة الحساب (هذا مقبول لأن الطالب
        # أثبت معرفته بكلمة المرور الصحيحة).
        if int(user.is_active or 0) != 1:
            if str(user.plan_name or "").startswith("pending_"):
                raise HTTPException(403, "تم إنشاء الحساب وبانتظار اعتماد الاشتراك من المدير")
            raise HTTPException(403, admin_contact_text())
        if user.subscription_expires_at and user.subscription_expires_at < dt.datetime.utcnow():
            raise HTTPException(403, subscription_expired_text())
        if user.locked_until and user.locked_until > dt.datetime.utcnow():
            raise HTTPException(429, "الحساب مقفول مؤقتًا. حاول لاحقًا")

        if "$" not in user.password_hash:
            user.password_hash = hash_password(password)
        user.failed_attempts = 0
        user.locked_until = None
        db.commit()

        token = create_session(db, user.id)
        csrf = issue_csrf_token()
        log_event(db, "auth.login", user.id, {"username": username, "ip": _client_ip(request)})
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
