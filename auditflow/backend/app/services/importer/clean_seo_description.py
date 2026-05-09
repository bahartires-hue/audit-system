"""
وصف منتج لسلة «بحر الإطارات» من حقول منظمة فقط — لا يستخدم نص الصفحة المسحوب.
"""
from __future__ import annotations

import re
from typing import Any, Dict

BAD_WORDS = [
    "السعر الأصلي",
    "السعر الحالي",
    "شامل الضريبة",
    "إضافة إلى السلة",
    "اشتري الان",
    "اشتري الآن",
    "زوار يستعرضون",
    "طلبات لهذا المنتج",
    "تمت منذ قليل",
    "متوفر في المخزون",
    "غير متوفر في المخزون",
    "أربع إطارات بسعر",
    "كمية",
    "تقييم",
    "سياسة الخصوصية",
    "الشروط والأحكام",
    "تسجيل الدخول",
    "السلة",
    "الدعم عبر البريد",
    "رقم ضريبي",
    "منتجات ذات صلة",
]

BAD_AI_PHRASES = [
    "نربط بين",
    "ضمن خانة",
    "مع ذكر",
    "ختامًا",
    "من جهة أخرى",
    "المعلومات أعلاه",
    "تصنيف تقريبي",
    "فئة مركبة مرجعية",
    "أسلوب طريق",
    "النقشة: النقشة",
]


def clean_text(value: str) -> str:
    if not value:
        return ""

    value = str(value)
    value = re.sub(r"\s+", " ", value).strip()

    for word in BAD_WORDS + BAD_AI_PHRASES:
        value = value.replace(word, "")

    value = value.replace("النقشة: النقشة:", "النقشة:")
    value = value.replace("النقشة: النقشة", "النقشة")
    value = re.sub(r"\s+", " ", value).strip()

    return value


def guess_car_type(size: str) -> str:
    if not size:
        return "السيارات"

    m = re.search(r"R(\d{2})", size.upper())
    rim = int(m.group(1)) if m else 0

    width_match = re.search(r"^(\d{3})", size)
    width = int(width_match.group(1)) if width_match else 0

    if rim >= 17 and width >= 245:
        return "سيارات الدفع الرباعي والـ SUV"
    if rim <= 16 and width <= 225:
        return "السيارات السيدان والاستخدام اليومي"
    return "السيارات العائلية والاستخدام اليومي"


def generate_clean_seo_description(product: Dict[str, Any]) -> str:
    brand = clean_text(str(product.get("brand", "") or ""))
    size = clean_text(str(product.get("size", "") or ""))
    load_speed = clean_text(str(product.get("load_speed", "") or ""))
    pattern = clean_text(str(product.get("pattern", "") or ""))
    country = clean_text(str(product.get("country", "") or ""))
    year = clean_text(str(product.get("year", "") or ""))
    warranty = clean_text(str(product.get("warranty", "") or ""))

    car_type = guess_car_type(size)

    parts: list[str] = []

    parts.append(
        f"كفر {brand} مقاس {size} مناسب لـ {car_type}، ويقدم أداءً عمليًا للقيادة اليومية داخل المدينة وعلى الطرق السريعة."
    )

    if pattern:
        parts.append(
            f"تساعد نقشة {pattern} على تحسين التماسك والثبات على الطرق المعبدة، مع تقليل الضوضاء أثناء الاستخدام المستمر."
        )

    if load_speed:
        parts.append(
            f"رمز الحمولة والسرعة {load_speed} يجعله خيارًا مناسبًا لمن يبحث عن إطار متوازن يجمع بين الراحة والتحمل."
        )

    extra: list[str] = []
    if country:
        extra.append(f"بلد المنشأ: {country}")
    if year:
        extra.append(f"سنة الصنع: {year}")
    if warranty:
        extra.append(f"الضمان: {warranty}")

    if extra:
        parts.append("، ".join(extra) + ".")

    parts.append(
        "في بحر الإطارات نوفر كفرات في الدمام بمقاسات متعددة، مع إمكانية شراء كفرات أونلاين وخيارات دفع مرنة."
    )

    description = " ".join(parts)
    description = clean_text(description)

    if len(description) > 750:
        description = description[:750].rsplit(" ", 1)[0] + "."

    return description


def validate_description(description: str) -> bool:
    if not description:
        return False

    if len(description) < 180:
        return False

    if len(description) > 800:
        return False

    for word in BAD_WORDS + BAD_AI_PHRASES:
        if word in description:
            return False

    if re.search(r"هناك\s*\d+\s*زوار", description):
        return False

    if description.count("كفرات في الدمام") > 1:
        return False

    if description.count("بحر الإطارات") > 1:
        return False

    return True
