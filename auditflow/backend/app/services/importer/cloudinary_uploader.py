from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Tuple

log = logging.getLogger("importer.cloudinary")


def _cloudinary_ready() -> bool:
    return bool(
        (os.getenv("CLOUDINARY_CLOUD_NAME") or "").strip()
        and (os.getenv("CLOUDINARY_API_KEY") or "").strip()
        and (os.getenv("CLOUDINARY_API_SECRET") or "").strip()
    )


def upload_to_cloudinary(local_image_path: str, public_id: str) -> Tuple[str, str]:
    if not local_image_path:
        return "", "no_local_image"
    if not _cloudinary_ready():
        return "", "cloudinary_not_configured"
    try:
        import cloudinary  # type: ignore
        import cloudinary.uploader  # type: ignore

        cloudinary.config(
            cloud_name=(os.getenv("CLOUDINARY_CLOUD_NAME") or "").strip(),
            api_key=(os.getenv("CLOUDINARY_API_KEY") or "").strip(),
            api_secret=(os.getenv("CLOUDINARY_API_SECRET") or "").strip(),
            secure=True,
        )

        pid = (public_id or "").strip().lower().replace("_", "-")
        pid = "".join(ch if (ch.isalnum() or ch in "-/") else "-" for ch in pid)
        pid = "-".join(x for x in pid.split("-") if x)
        if not pid:
            pid = Path(local_image_path).stem

        res = cloudinary.uploader.upload(
            local_image_path,
            public_id=pid,
            folder="products",
            resource_type="image",
            overwrite=True,
            unique_filename=False,
            invalidate=True,
        )
        url = (res.get("secure_url") or "").strip()
        if not url.startswith("https://res.cloudinary.com/"):
            return "", "cloudinary_invalid_url"
        return url, "uploaded"
    except Exception as e:
        log.warning("cloudinary upload failed path=%s err=%s", local_image_path, e)
        return "", "upload_failed"

