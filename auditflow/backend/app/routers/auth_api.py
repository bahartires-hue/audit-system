from __future__ import annotations

import datetime as dt
import json
import os
import secrets
import uuid

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

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
    require_csrf,
    require_user,
    session_max_age_seconds,
    verify_password,
)
from ..db import SessionLocal
from ..mailer import send_password_reset_email
from ..models import AuditLog, InviteCode, PasswordResetToken, User, UserSession
from ..rate_limit import limiter

router = APIRouter(tags=["auth"])


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
    password = str((payload or {}).get("password", "")).strip()
    if len(username) < 3:
        raise HTTPException(400, "اسم المستخدم قصير")
    if "@" not in email or "." not in email:
        raise HTTPException(400, "البريد الإلكتروني غير صالح")
    if len(password) < 4:
        raise HTTPException(400, "كلمة المرور قصيرة")

    db = SessionLocal()
    try:
        require_csrf(request)
        bootstrap_admin = _is_bootstrap_admin_registration(db, username, email)
        if not bootstrap_admin and not _is_invite_valid(db, invite_code):
            raise HTTPException(400, "كود الدعوة غير صالح أو منتهي")
        exists = db.query(User).filter(User.username == username).first()
        if exists:
            raise HTTPException(400, "اسم المستخدم موجود بالفعل")
        exists_email = db.query(User).filter(User.email == email).first()
        if exists_email:
            raise HTTPException(400, "البريد الإلكتروني مستخدم بالفعل")
        user = User(
            id=uuid.uuid4().hex,
            username=username,
            email=email,
            is_admin=1 if bootstrap_admin else 0,
            is_active=1,
            plan_name="free",
            password_hash=hash_password(password),
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
        send_password_reset_email(email, reset_link)
        log_event(db, "auth.password_reset.requested", user.id, {"email": email})
        return {"ok": True}
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
    max_uses = int((payload or {}).get("max_uses", 1) or 1)
    if max_uses < 1:
        max_uses = 1
    if max_uses > 10:
        max_uses = 10
    hours = int((payload or {}).get("expires_in_hours", 6) or 6)
    code = str((payload or {}).get("code", "")).strip() or secrets.token_urlsafe(8).replace("-", "").replace("_", "")
    code = code[:32]
    if hours < 1:
        hours = 1

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
        return {
            "total_users": total_users,
            "active_users": active_users,
            "paid_active": paid_active,
            "invite_total": invite_total,
            "invite_used": invite_used,
            "invite_active": invite_active_count,
        }
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
        if "plan_name" in payload:
            p = str(payload.get("plan_name") or "free").strip().lower()
            target.plan_name = p or "free"
        if "subscription_months" in payload:
            m = int(payload.get("subscription_months") or 0)
            if m > 0:
                base = target.subscription_expires_at if target.subscription_expires_at and target.subscription_expires_at > dt.datetime.utcnow() else dt.datetime.utcnow()
                target.subscription_expires_at = base + dt.timedelta(days=30 * m)
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


@router.post("/auth/login")
@limiter.limit("25/minute")
async def auth_login(request: Request):
    payload = await request.json()
    username = str((payload or {}).get("username", "")).strip()
    password = str((payload or {}).get("password", "")).strip()

    db = SessionLocal()
    try:
        require_csrf(request)
        user = db.query(User).filter(User.username == username).first()
        if not user:
            raise HTTPException(401, "بيانات الدخول غير صحيحة")
        if int(user.is_active or 0) != 1:
            raise HTTPException(403, "الحساب موقوف. تواصل مع الإدارة")
        if user.subscription_expires_at and user.subscription_expires_at < dt.datetime.utcnow():
            raise HTTPException(403, "انتهت صلاحية الاشتراك. تواصل مع الإدارة")
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
