from __future__ import annotations

import datetime as dt
import os

from app.auth_core import log_event
from app.db import SessionLocal
from app.mailer import send_plain_email
from app.models import User


def main() -> None:
    days = max(1, min(int((os.getenv("AUDITFLOW_NOTIFY_DAYS") or "7").strip()), 30))
    db = SessionLocal()
    try:
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
                    subject="تنبيه قرب انتهاء الاشتراك | OptimalMatch",
                    body=(
                        f"مرحباً {u.username},\n\n"
                        f"اشتراكك ({u.plan_name}) سينتهي قريباً في {u.subscription_expires_at}.\n"
                        "يرجى التواصل مع الإدارة للتجديد.\n"
                    ),
                )
                sent += 1
            except Exception:
                failed += 1
        log_event(db, "cron.notify_expiring.ran", None, {"days": days, "targets": len(rows), "sent": sent, "failed": failed})
        print(f"notify_expiring: targets={len(rows)} sent={sent} failed={failed}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
