"""
وصف منتج لسلة «بحر الإطارات» من حقول منظمة فقط — لا يستخدم نص الصفحة المسحوب.
"""
from __future__ import annotations

import hashlib
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

# أي ظهور لهذه السلاسل في الوصف النهائي = رفض النص بالكامل (لا يُعتمد وصف الصفحة أو حشو المتجر).
DESCRIPTION_REJECT_SUBSTRINGS = (
    "السعر",
    "الضريبة",
    "المخزون",
    "زوار",
    "طلبات",
    "تقييم",
    "إضافة للسلة",
    "إضافة إلى السلة",
    "أضف إلى السلة",
    "أضف للسلة",
    "اشتري الآن",
    "اشتري الان",
    "متوفر في المخزون",
    "شامل الضريبة",
)


def description_has_rejected_commerce_markers(text: str) -> bool:
    """True إذا كان النص يحتوي كلمات متجر/صفحة ممنوعة (يُرفض بالكامل)."""
    if not str(text or "").strip():
        return True
    t = str(text)
    for sub in DESCRIPTION_REJECT_SUBSTRINGS:
        if sub in t:
            return True
    if re.search(r"هناك\s*\d+\s*زوار", t):
        return True
    return False


def clean_text(value: str) -> str:
    if not value:
        return ""

    value = str(value)
    value = re.sub(r"\s+", " ", value).strip()

    for word in BAD_WORDS + BAD_AI_PHRASES:
        value = value.replace(word, "")

    value = value.replace("النقشة: النقشة:", "النقشة:")
    value = value.replace("النقشة: النقشة", "النقشة")
    value = re.sub(r"نقشة\s+النقشة", "النقشة", value)
    value = re.sub(r"\s+", " ", value).strip()

    return value


def normalize_pattern_display(raw: str) -> str:
    """
    إزالة بادئات «نقشة / النقشة» من قيمة الحقل القادمة من المتجر
    حتى لا تتكرر عبارة مثل «تساعد نقشة النقشة: …» في الجملة.
    """
    p = clean_text(str(raw or ""))
    if not p:
        return ""
    p = re.sub(r"^(ال)?نقشة\s*[:：]\s*", "", p, flags=re.IGNORECASE).strip()
    p = re.sub(r"^(ال)?نقشة\s+", "", p, flags=re.IGNORECASE).strip()
    p = p.strip(":-： ").strip()
    return p


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


def _variant_seed(product: Dict[str, Any]) -> int:
    key = "|".join(
        [
            str(product.get("brand", "") or ""),
            str(product.get("size", "") or ""),
            str(product.get("load_speed", "") or ""),
            str(product.get("pattern", "") or ""),
        ]
    )
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:8], 16)


def generate_clean_seo_description(product: Dict[str, Any]) -> str:
    brand = clean_text(str(product.get("brand", "") or ""))
    size = clean_text(str(product.get("size", "") or ""))
    load_speed = clean_text(str(product.get("load_speed", "") or ""))
    pattern_raw = clean_text(str(product.get("pattern", "") or ""))
    pattern = normalize_pattern_display(pattern_raw)
    country = clean_text(str(product.get("country", "") or ""))
    year = clean_text(str(product.get("year", "") or ""))
    warranty = clean_text(str(product.get("warranty", "") or ""))

    car_type = guess_car_type(size)
    h = _variant_seed(product)

    openers = [
        f"كفر {brand} مقاس {size} يناسب {car_type}، مع أداء عملي للتنقل اليومي داخل المدينة وعلى الطرق السريعة.",
        f"يقدّم {brand} في مقاس {size} خيارًا متوازنًا لمالكي {car_type}، مع تركيز على ثبات معقول وراحة أثناء القيادة المتكررة.",
        f"مقاس {size} من {brand} موجّه لاستخدام {car_type} بشكل عام، مع سلوك عملي على الأسفلت في الزحام والمسافات القصيرة.",
        f"إطار {brand} {size} يستهدف {car_type}، ويُفضّل مطابقته مع جدول الإطارات قبل الاعتماد النهائي للتركيب.",
    ]
    pattern_lines = [
        f"تصميم {pattern} يدعم تماسكًا جيدًا على الطرق المعبدة مع ضوضاء مقبولة نسبيًا أثناء الاستخدام الطويل.",
        f"نقشة {pattern} تساهم في ثبات أفضل عند المنعطفات الخفيفة مع أسلوب قيادة هادئ على الطرق السريعة.",
        f"أسلوب {pattern} يميل إلى التوازن بين الكبح والتسارع على الأسفلت دون مبالغة في وعود الأداء.",
    ]
    load_lines = [
        f"رمز الحمولة والسرعة {load_speed} يحدّد نطاق التحميل والسرعة الآمنة؛ راجع توصية مصنع سيارتك.",
        f"مؤشر {load_speed} يوضح حدود التحميل والسرعة؛ التزم بجدول الإطارات لضمان استخدام آمن.",
    ]
    closers = [
        "في بحر الإطارات نوفر كفرات في الدمام بمقاسات متعددة، مع إمكانية شراء كفرات أونلاين وخيارات دفع مرنة.",
        "بحر الإطارات يخدم طلب كفرات في الدمام وكفرات سيارات بمقاسات مختلفة، مع خيار طلب أونلاين عند التوفر.",
    ]

    parts: list[str] = [openers[h % len(openers)]]

    if pattern:
        parts.append(pattern_lines[h % len(pattern_lines)])

    if load_speed:
        parts.append(load_lines[(h >> 4) % len(load_lines)])

    extra: list[str] = []
    if country:
        extra.append(f"بلد المنشأ: {country}")
    if year:
        extra.append(f"سنة الصنع: {year}")
    if warranty:
        extra.append(f"الضمان: {warranty}")

    if extra:
        parts.append("، ".join(extra) + ".")

    parts.append(closers[(h >> 8) % len(closers)])

    description = " ".join(parts)
    description = clean_text(description)

    if len(description) > 750:
        description = description[:750].rsplit(" ", 1)[0] + "."

    return description


def validate_description(description: str) -> bool:
    if not description:
        return False

    if description_has_rejected_commerce_markers(description):
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
