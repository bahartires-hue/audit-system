from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Tuple

from fastapi import UploadFile

MAX_UPLOAD_MB = int(os.environ.get("AUDITFLOW_MAX_UPLOAD_MB", "15"))

_ALLOWED_SUFFIXES = (".pdf", ".xlsx", ".xls", ".csv", ".xlsm", ".xlsb")


def _validate_upload_bytes(original: str, content: bytes) -> None:
    if not content:
        raise ValueError("ملف فارغ")
    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    if len(content) > max_bytes:
        raise ValueError(f"حجم الملف أكبر من {MAX_UPLOAD_MB} ميجابايت")
    lower = (original or "upload").lower()
    if not any(lower.endswith(s) for s in _ALLOWED_SUFFIXES):
        raise ValueError("نوع الملف غير مدعوم. المسموح: Excel أو PDF أو CSV")
    if lower.endswith(".pdf") and not content.startswith(b"%PDF"):
        raise ValueError("ملف PDF غير صالح")
    if lower.endswith(".xlsx") and not content.startswith(b"PK"):
        raise ValueError("ملف Excel (.xlsx) غير صالح")
    if lower.endswith(".xlsm") and not content.startswith(b"PK"):
        raise ValueError("ملف Excel (.xlsm) غير صالح")
    if lower.endswith(".xls") and not content.startswith(b"\xD0\xCF\x11\xE0"):
        raise ValueError("ملف Excel (.xls) غير صالح")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_upload_file(upload: UploadFile, dest_dir: Path) -> Tuple[str, str]:
    """
    Returns: (saved_path, original_filename)
    """
    ensure_dir(dest_dir)
    original = upload.filename or "upload"
    suffix = Path(original).suffix
    saved_name = f"{uuid.uuid4().hex}{suffix}"
    saved_path = dest_dir / saved_name

    content = upload.file.read()
    _validate_upload_bytes(original, content)

    with open(saved_path, "wb") as f:
        f.write(content)

    return str(saved_path), original
