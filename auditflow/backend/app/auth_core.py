from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import secrets
import uuid
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from .models import AuditLog, User, UserSession

SESSION_COOKIE = "auditflow_session"
CSRF_COOKIE = "auditflow_csrf"
SESSION_DAYS = 14
LOCK_MINUTES = 15


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200000)
    return f"pbkdf2_sha256${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    if "$" not in stored:
        return hashlib.sha256(password.encode("utf-8")).hexdigest() == stored
    try:
        algo, salt_hex, hash_hex = stored.split("$", 2)
        if algo != "pbkdf2_sha256":
            return False
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200000)
        return hmac.compare_digest(dk, expected)
    except Exception:
        return False


def create_session(db: Session, user_id: str) -> str:
    token = secrets.token_urlsafe(40)
    now = dt.datetime.utcnow()
    session = UserSession(
        token=token,
        user_id=user_id,
        created_at=now,
        expires_at=now + dt.timedelta(days=SESSION_DAYS),
    )
    db.add(session)
    db.commit()
    return token


def issue_csrf_token() -> str:
    return secrets.token_urlsafe(24)


def require_csrf(request: Request) -> None:
    cookie_token = request.cookies.get(CSRF_COOKIE, "")
    header_token = request.headers.get("x-csrf-token", "")
    if not cookie_token or not header_token or not hmac.compare_digest(cookie_token, header_token):
        raise HTTPException(403, "CSRF token غير صالح")


def current_user_from_request(db: Session, request: Request) -> Optional[User]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    s = db.query(UserSession).filter(UserSession.token == token).first()
    if not s:
        return None
    if s.expires_at < dt.datetime.utcnow():
        db.delete(s)
        db.commit()
        return None
    return db.query(User).filter(User.id == s.user_id).first()


def require_user(db: Session, request: Request) -> User:
    u = current_user_from_request(db, request)
    if not u:
        raise HTTPException(401, "يرجى تسجيل الدخول أولاً")
    return u


def log_event(db: Session, action: str, user_id: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> None:
    db.add(
        AuditLog(
            id=uuid.uuid4().hex,
            user_id=user_id,
            action=action,
            meta_json=meta or {},
        )
    )
    db.commit()
