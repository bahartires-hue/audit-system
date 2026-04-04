from __future__ import annotations

import datetime as dt
import json
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
from ..models import User, UserSession

router = APIRouter(tags=["auth"])


@router.get("/auth/me")
def auth_me(request: Request):
    db = SessionLocal()
    try:
        u = current_user_from_request(db, request)
        csrf = request.cookies.get(CSRF_COOKIE) or issue_csrf_token()
        username = u.username if u else None
        res = Response(
            content=json.dumps({"username": username, "csrf_token": csrf}),
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
    password = str((payload or {}).get("password", "")).strip()
    if len(username) < 3:
        raise HTTPException(400, "اسم المستخدم قصير")
    if len(password) < 4:
        raise HTTPException(400, "كلمة المرور قصيرة")

    db = SessionLocal()
    try:
        require_csrf(request)
        exists = db.query(User).filter(User.username == username).first()
        if exists:
            raise HTTPException(400, "اسم المستخدم موجود بالفعل")
        user = User(id=uuid.uuid4().hex, username=username, password_hash=hash_password(password))
        db.add(user)
        db.commit()

        token = create_session(db, user.id)
        csrf = issue_csrf_token()
        log_event(db, "auth.register", user.id, {"username": username})
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


@router.post("/auth/login")
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
