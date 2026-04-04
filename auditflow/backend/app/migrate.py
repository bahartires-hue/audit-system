from __future__ import annotations

from sqlalchemy import text

from .db import engine


def run_migrations() -> None:
    """SQLite: أعمدة ناقصة على قواعد قديمة."""
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
