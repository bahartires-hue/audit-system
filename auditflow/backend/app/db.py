from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

BASE_DIR = Path(__file__).resolve().parents[1]  # auditflow/backend


def _default_sqlite_path() -> Path:
    root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
    if root:
        return Path(root) / "auditflow.db"
    return BASE_DIR / "data.db"


DB_PATH = Path(os.getenv("DATABASE_PATH", str(_default_sqlite_path())))

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if DATABASE_URL:
    # Render/PostgreSQL path
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://") :]
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        connect_args={"check_same_thread": False},
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

