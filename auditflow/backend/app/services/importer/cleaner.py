from __future__ import annotations

import re
from typing import Any, Dict


_SIZE_RE = re.compile(r"(\d{3}/\d{2,3}Z?R\d{2})", re.IGNORECASE)
_LOAD_SPEED_RE = re.compile(r"\b(\d{2,3}[A-Z])\b")


def slugify(s: str) -> str:
    t = (s or "").strip().lower()
    t = re.sub(r"[^a-z0-9\u0600-\u06ff]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-")
    return t or "product"


def normalize_size(size: str) -> str:
    s = (size or "").upper().replace("ZR", "R").replace(" ", "")
    return s


def clean_tire_name(raw_name: str) -> Dict[str, Any]:
    name = re.sub(r"\s+", " ", (raw_name or "").strip())
    toks = name.split()
    brand = toks[0].title() if toks else ""

    m_size = _SIZE_RE.search(name)
    size = normalize_size(m_size.group(1)) if m_size else ""
    m_ls = _LOAD_SPEED_RE.search(name)
    load_speed = m_ls.group(1).upper() if m_ls else ""
    xl = bool(re.search(r"\bXL\b", name, re.IGNORECASE))

    model = name
    if brand:
        model = re.sub(rf"^{re.escape(brand)}\s*", "", model, flags=re.IGNORECASE)
    if size:
        model = model.replace(m_size.group(1), "").strip()
    if load_speed:
        model = re.sub(rf"\b{re.escape(load_speed)}\b", "", model, flags=re.IGNORECASE).strip()
    if xl:
        model = re.sub(r"\bXL\b", "", model, flags=re.IGNORECASE).strip()
    model = re.sub(r"\s+", " ", model)

    return {
        "raw_name": name,
        "brand": brand,
        "model": model,
        "size": size,
        "load_speed": load_speed,
        "xl": xl,
    }

