from __future__ import annotations

import logging
import re
from typing import Any, Dict

log = logging.getLogger("importer.parser")

_SIZE_RE = re.compile(r"(\d{3})\s*/\s*(\d{2,3})\s*Z?R\s*(\d{2})", re.IGNORECASE)
_LOAD_SPEED_RE = re.compile(r"\b(\d{2,3}[A-Z])\b")
_RF_RE = re.compile(r"\b(RF|RUNFLAT|RUN FLAT)\b", re.IGNORECASE)
_PR_RE = re.compile(r"\b(\d{1,2}\s*PR)\b", re.IGNORECASE)
_AR_RE = re.compile(r"[\u0600-\u06FF]")

BRAND_TRANSLATIONS = {
    "ميشلان": "Michelin",
    "بريدجستون": "Bridgestone",
    "هانكوك": "Hankook",
    "جوديير": "Goodyear",
    "كونتيننتال": "Continental",
    "بيريللي": "Pirelli",
    "يوكوهاما": "Yokohama",
    "كمهو": "Kumho",
    "دنلوب": "Dunlop",
    "نيكسن": "Nexen",
    "نكسان": "Nexen",
    "الفا": "Alpha",
}

MODEL_TRANSLATIONS = {
    "بايلوت سبورت": "Pilot Sport",
    "بايلوت": "Pilot",
    "روديان": "Roadian",
    "تورانزا": "Turanza",
    "بوتينزا": "Potenza",
}


def slugify(s: str) -> str:
    t = (s or "").strip().lower()
    t = re.sub(r"[^a-z0-9\u0600-\u06ff]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-")
    return t or "product"


def normalize_brand_name(raw: str) -> str:
    s = re.sub(r"\s+", " ", (raw or "").strip())
    if not s:
        return ""
    for ar, en in BRAND_TRANSLATIONS.items():
        if ar in s:
            s = s.replace(ar, en)
    return s.title()


def normalize_model_name(raw: str) -> str:
    s = re.sub(r"\s+", " ", (raw or "").strip())
    if not s:
        return ""
    for ar, en in MODEL_TRANSLATIONS.items():
        s = s.replace(ar, en)
    s = re.sub(r"\bRUN\s*FLAT\b", "RF", s, flags=re.IGNORECASE)
    return s.title()


def _contains_arabic(s: str) -> bool:
    return bool(_AR_RE.search(s or ""))


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
    brand = normalize_brand_name(toks[0] if toks else "")
    detected_ar_brand = ""
    for ar, en in BRAND_TRANSLATIONS.items():
        if ar in name:
            detected_ar_brand = ar
            if not brand:
                brand = en
            break
    m_size = _SIZE_RE.search(name)
    width = m_size.group(1) if m_size else ""
    profile = m_size.group(2) if m_size else ""
    rim = m_size.group(3) if m_size else ""
    size = f"{width}/{profile}R{rim}" if width and profile and rim else ""
    m_ls = _LOAD_SPEED_RE.search(name)
    load_speed = m_ls.group(1).upper() if m_ls else ""
    xl = bool(re.search(r"\bXL\b", name, re.IGNORECASE))
    rf = bool(_RF_RE.search(name))
    pr = (_PR_RE.search(name).group(1).upper().replace(" ", "") if _PR_RE.search(name) else "")

    model = name
    if brand:
        model = re.sub(rf"^{re.escape(brand)}\s*", "", model, flags=re.IGNORECASE)
    if detected_ar_brand:
        model = re.sub(rf"\b{re.escape(detected_ar_brand)}\b", "", model, flags=re.IGNORECASE).strip()
    if m_size:
        model = model.replace(m_size.group(0), " ").strip()
    if load_speed:
        model = re.sub(rf"\b{re.escape(load_speed)}\b", "", model, flags=re.IGNORECASE).strip()
    if xl:
        model = re.sub(r"\bXL\b", "", model, flags=re.IGNORECASE).strip()
    if rf:
        model = re.sub(r"\b(RF|RUNFLAT|RUN FLAT)\b", "", model, flags=re.IGNORECASE).strip()
    if pr:
        model = re.sub(rf"\b{re.escape(pr)}\b", "", model, flags=re.IGNORECASE).strip()
    model = re.sub(r"\s+", " ", model)
    model = normalize_model_name(model)
    # fallback ترجمة الماركة إذا كانت بالعربي داخل الاسم الكامل
    if not brand:
        for ar, en in BRAND_TRANSLATIONS.items():
            if ar in name:
                brand = en
                model = normalize_model_name(name.replace(ar, "").strip())
                break

    # enforce English-only brand/model
    if _contains_arabic(brand) or _contains_arabic(model):
        parse_status = "non_english_name"
    else:
        parse_status = "ok" if size else "size_missing"

    improved = " ".join(
        x for x in [brand, model, size, load_speed, "XL" if xl else "", "RF" if rf else "", pr] if x
    ).strip()
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
        "rf": rf,
        "pr": pr,
        "product_title": improved or name,
        "parse_status": parse_status,
    }

