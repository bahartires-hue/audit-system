from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Tuple

from fastapi import UploadFile


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

    # UploadFile.file is a SpooledTemporaryFile; we read bytes once.
    content = upload.file.read()
    with open(saved_path, "wb") as f:
        f.write(content)

    return str(saved_path), original

