from __future__ import annotations

import logging
import re
from typing import Any, Dict

log = logging.getLogger("importer.parser")

_SIZE_RE = re.compile(r"(\d{3})\s*/\s*(\d{2,3})\s*Z?R\s*(\d{2})", re.IGNORECASE)
_LOAD_SPEED_RE = re.compile(r"\b(\d{2,3}[A-Z])\b")


def slugify(s: str) -> str:
    t = (s or "").strip().lower()
    t = re.sub(r"[^a-z0-9\u0600-\u06ff]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-")
    return t or "product"


def parse_tire_name(raw_name: str) -> Dict[str, Any]:
    name = re.sub(r"\s+", " ", (raw_name or "").strip())
    if not name:
        return {
            "original_name": "",
            "brand": "",
            "model": "",
            "size": "",
            "width": "",
            "profile": "",
            "rim": "",
            "load_speed": "",
            "xl": False,
            "product_title": "",
            "parse_status": "failed",
        }

    toks = name.split()
    brand = toks[0].title() if toks else ""
    m_size = _SIZE_RE.search(name)
    width = m_size.group(1) if m_size else ""
    profile = m_size.group(2) if m_size else ""
    rim = m_size.group(3) if m_size else ""
    size = f"{width}/{profile}R{rim}" if width and profile and rim else ""
    m_ls = _LOAD_SPEED_RE.search(name)
    load_speed = m_ls.group(1).upper() if m_ls else ""
    xl = bool(re.search(r"\bXL\b", name, re.IGNORECASE))

    model = name
    if brand:
        model = re.sub(rf"^{re.escape(brand)}\s*", "", model, flags=re.IGNORECASE)
    if m_size:
        model = model.replace(m_size.group(0), " ").strip()
    if load_speed:
        model = re.sub(rf"\b{re.escape(load_speed)}\b", "", model, flags=re.IGNORECASE).strip()
    if xl:
        model = re.sub(r"\bXL\b", "", model, flags=re.IGNORECASE).strip()
    model = re.sub(r"\s+", " ", model)

    improved = " ".join(x for x in [brand, model, size, load_speed, "XL" if xl else ""] if x).strip()
    return {
        "original_name": name,
        "brand": brand,
        "model": model,
        "size": size,
        "width": width,
        "profile": profile,
        "rim": rim,
        "load_speed": load_speed,
        "xl": xl,
        "product_title": improved or name,
        "parse_status": "ok" if size else "size_missing",
    }

