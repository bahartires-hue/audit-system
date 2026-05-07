from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlparse

import requests
import logging
from PIL import Image

log = logging.getLogger("importer.images")
_BANNED_TOKENS = {"tireex", "competitor", "img", "image", "photo", "cdn", "site"}


def _guess_ext(url: str) -> str:
    p = urlparse(url).path.lower()
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        if p.endswith(ext):
            return ".jpg" if ext == ".jpeg" else ext
    return ".jpg"


def sanitize_filename(name: str) -> str:
    t = (name or "").strip().lower()
    t = re.sub(r"[^a-z0-9\-]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-")
    parts = [p for p in t.split("-") if p and p not in _BANNED_TOKENS]
    out = "-".join(parts).strip("-")
    return out or "tire-product"


def _watermark_suspected(image_url: str, file_name: str) -> bool:
    blob = f"{image_url} {file_name}".lower()
    return any(x in blob for x in ["watermark", "logo-overlay", "copyright", "wm-"])


def _is_valid_image_file(path: Path) -> bool:
    try:
        with Image.open(path) as img:
            img.verify()
        return path.stat().st_size > 0
    except Exception:
        return False


def download_image(image_url: str, target_dir: Path, seo_slug: str) -> tuple[str, str]:
    target_dir.mkdir(parents=True, exist_ok=True)
    ext = _guess_ext(image_url)
    safe_slug = sanitize_filename(seo_slug)
    fname = f"{safe_slug}{ext}"
    fpath = target_dir / fname

    if not image_url:
        return "", "no_image_url"
    if fpath.exists():
        status = "needs_review" if _watermark_suspected(image_url, fname) else "exists"
        return str(fpath), status
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Referer": f"{urlparse(image_url).scheme}://{urlparse(image_url).netloc}/",
    }
    last_err = None
    for timeout_s in (12, 20, 30):
        try:
            res = requests.get(image_url, timeout=timeout_s, headers=headers)
            res.raise_for_status()
            ctype = (res.headers.get("Content-Type") or "").lower()
            if ctype and not ctype.startswith("image/"):
                raise ValueError(f"non-image content-type: {ctype}")
            if not res.content:
                raise ValueError("empty image content")
            with open(fpath, "wb") as f:
                f.write(res.content)
            if not _is_valid_image_file(fpath):
                try:
                    fpath.unlink(missing_ok=True)
                except Exception:
                    pass
                raise ValueError("invalid image file")
            status = "needs_review" if _watermark_suspected(image_url, fname) else "downloaded"
            return str(fpath), status
        except Exception as e:
            last_err = e
            continue
    log.warning("image download failed url=%s err=%s", image_url, last_err)
    return "", "failed"

