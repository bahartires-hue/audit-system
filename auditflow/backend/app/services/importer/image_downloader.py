from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import requests
import logging

log = logging.getLogger("importer.images")


def _guess_ext(url: str) -> str:
    p = urlparse(url).path.lower()
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        if p.endswith(ext):
            return ".jpg" if ext == ".jpeg" else ext
    return ".jpg"


def download_image(image_url: str, target_dir: Path, seo_slug: str) -> tuple[str, str]:
    target_dir.mkdir(parents=True, exist_ok=True)
    ext = _guess_ext(image_url)
    fname = f"{seo_slug}{ext}"
    fpath = target_dir / fname

    if not image_url:
        return "", "no_image_url"
    if fpath.exists():
        return str(fpath), "exists"
    try:
        res = requests.get(image_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        res.raise_for_status()
        with open(fpath, "wb") as f:
            f.write(res.content)
        return str(fpath), "downloaded"
    except Exception as e:
        log.warning("image download failed url=%s err=%s", image_url, e)
        return "", "failed"

