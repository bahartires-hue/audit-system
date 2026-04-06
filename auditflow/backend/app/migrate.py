from __future__ import annotations

from sqlalchemy import text

from .db import engine


def run_migrations() -> None:
    """SQLite: أعمدة ناقصة على قواعد قديمة."""
    if engine.dialect.name != "sqlite":
        # PostgreSQL: rely on metadata/create_all for new tables.
        return
    with engine.begin() as conn:
        r = conn.execute(text("PRAGMA table_info(analysis_reports)"))
        cols = [row[1] for row in r.fetchall()]
        if cols and "user_id" not in cols:
            conn.execute(text("ALTER TABLE analysis_reports ADD COLUMN user_id VARCHAR"))

        r2 = conn.execute(text("PRAGMA table_info(users)"))
        ucols = [row[1] for row in r2.fetchall()]
        if ucols:
            if "failed_attempts" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN failed_attempts INTEGER DEFAULT 0"))
            if "locked_until" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN locked_until DATETIME"))
            if "email" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN email VARCHAR"))
            if "is_admin" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0"))
            if "is_active" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1"))
            if "plan_name" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN plan_name VARCHAR DEFAULT 'free'"))
            if "subscription_expires_at" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN subscription_expires_at DATETIME"))
            if "preferences_json" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN preferences_json JSON"))

        r3 = conn.execute(text("PRAGMA table_info(analysis_reports)"))
        rcols = [row[1] for row in r3.fetchall()]
        if rcols:
            if "tags_json" not in rcols:
                conn.execute(text("ALTER TABLE analysis_reports ADD COLUMN tags_json JSON"))
            if "notes" not in rcols:
                conn.execute(text("ALTER TABLE analysis_reports ADD COLUMN notes TEXT"))
            if "archived" not in rcols:
                conn.execute(text("ALTER TABLE analysis_reports ADD COLUMN archived INTEGER DEFAULT 0"))

        conn.execute(text("UPDATE analysis_reports SET archived = 0 WHERE archived IS NULL"))
        conn.execute(text("UPDATE analysis_reports SET tags_json = '[]' WHERE tags_json IS NULL"))
        conn.execute(text("UPDATE users SET preferences_json = '{}' WHERE preferences_json IS NULL"))
        conn.execute(text("UPDATE users SET is_active = 1 WHERE is_active IS NULL"))
        conn.execute(text("UPDATE users SET plan_name = 'free' WHERE plan_name IS NULL"))
