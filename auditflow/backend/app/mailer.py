from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage


def _smtp_enabled() -> bool:
    return bool((os.getenv("SMTP_HOST") or "").strip())


def send_password_reset_email(to_email: str, reset_link: str) -> None:
    """
    Sends password reset email via SMTP.
    Required env:
    - SMTP_HOST
    - SMTP_PORT (default 587)
    - SMTP_USER (optional)
    - SMTP_PASS (optional)
    - SMTP_FROM (default SMTP_USER)
    """
    if not _smtp_enabled():
        raise RuntimeError("SMTP غير مفعّل: يرجى ضبط SMTP_HOST")

    host = (os.getenv("SMTP_HOST") or "").strip()
    port = int((os.getenv("SMTP_PORT") or "587").strip())
    user = (os.getenv("SMTP_USER") or "").strip()
    password = os.getenv("SMTP_PASS") or ""
    sender = (os.getenv("SMTP_FROM") or user).strip()
    if not sender:
        raise RuntimeError("SMTP_FROM غير مضبوط")

    msg = EmailMessage()
    msg["Subject"] = "استعادة كلمة المرور | التطابق الأمثل"
    msg["From"] = sender
    msg["To"] = to_email
    msg.set_content(
        "طلبت استعادة كلمة المرور.\n\n"
        f"الرابط: {reset_link}\n\n"
        "إذا لم تكن أنت، تجاهل هذه الرسالة."
    )

    with smtplib.SMTP(host, port, timeout=20) as smtp:
        smtp.ehlo()
        try:
            smtp.starttls()
            smtp.ehlo()
        except Exception:
            # Allow non-TLS SMTP setups if explicitly configured that way
            pass
        if user:
            smtp.login(user, password)
        smtp.send_message(msg)
