from __future__ import annotations

import re
from typing import Any, Dict


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _load_speed_parts(load_speed: str) -> tuple[str, str]:
    ls = _clean(load_speed).upper()
    m = re.match(r"^(\d{2,3})([A-Z])$", ls)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


def rewrite_description_fallback(prod: Dict[str, Any], source_description: str = "") -> str:
    brand = _clean(prod.get("brand", ""))
    model = _clean(prod.get("model", ""))
    size = _clean(prod.get("size", ""))
    load_speed = _clean(prod.get("load_speed", ""))
    year = _clean(prod.get("year", ""))
    country = _clean(prod.get("country", ""))
    warranty = _clean(prod.get("warranty", ""))
    pattern = _clean(prod.get("pattern", ""))
    xl = bool(prod.get("xl"))
    ptype = "إطار سيارات"
    if pattern:
        p = pattern.lower()
        if "at" in p or "all terrain" in p:
            ptype = "إطار All Terrain"
        elif "mt" in p or "mud" in p:
            ptype = "إطار Mud Terrain"

    load_idx, speed_symbol = _load_speed_parts(load_speed)
    title = _clean(f"كفر {brand} {model} مقاس {size} {load_speed} {'XL' if xl else ''}")

    intro = (
        f"{title}\n\n"
        f"{ptype} مصمم لتقديم توازن ممتاز بين الثبات والراحة وعمر الاستخدام، "
        f"ومناسب للقيادة اليومية والرحلات الطويلة مع أداء موثوق على مختلف أنواع الطرق."
    )
    if pattern:
        intro += f" يتميز بنقشة {pattern} التي تساعد على تحسين التماسك وتقليل الانزلاق."

    durability = (
        "تم تطوير هيكل الإطار بمواد متينة تساعد على توزيع الضغط بشكل متوازن، "
        "ودعم ثبات المركبة في المنعطفات والسرعات المتوسطة والعالية."
    )
    if xl:
        durability += " كما أن نسخة XL توفر دعماً إضافياً لتحمل الأحمال."

    specs = [
        f"الماركة: {brand}" if brand else "",
        f"الموديل: {model}" if model else "",
        f"المقاس: {size}" if size else "",
        f"مؤشر الحمولة والسرعة: {load_speed}" if load_speed else "",
        f"مؤشر الحمولة: {load_idx}" if load_idx else "",
        f"رمز السرعة: {speed_symbol}" if speed_symbol else "",
        f"سنة الصنع: {year}" if year else "",
        f"بلد المنشأ: {country}" if country else "",
        f"النقشة: {pattern}" if pattern else "",
        f"الضمان: {warranty}" if warranty else "",
    ]
    specs_text = "\n".join(x for x in specs if x)

    keywords = ", ".join(
        x
        for x in [
            f"كفر {brand}".strip(),
            f"{brand} {model}".strip(),
            f"مقاس {size}".strip(),
            "إطارات سيارات",
            "أفضل كفرات",
            "سعر كفرات",
            "شراء كفرات اونلاين",
        ]
        if _clean(x)
    )

    extra = _clean(source_description)
    if extra:
        extra = f"\n\nمعلومات إضافية:\n{extra[:700]}"

    return (
        f"{intro}\n\n"
        f"{durability}\n\n"
        "المواصفات\n\n"
        f"{specs_text}\n\n"
        "كلمات مفتاحية SEO\n"
        f"{keywords}"
        f"{extra}"
    ).strip()

