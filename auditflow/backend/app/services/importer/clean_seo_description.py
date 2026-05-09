"""
وصف منتج لسلة «بحر الإطارات» من حقول منظمة فقط — لا يستخدم نص الصفحة المسحوب.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional

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


def strip_html_tags(text: str) -> str:
    """إزالة أي وسم HTML — لا يُسمح بدمج نص خام من الصفحة داخل الوصف."""
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", str(text))
    return re.sub(r"\s+", " ", t).strip()


def _sanitize_spec_value(value: str) -> str:
    v = strip_html_tags(value or "")
    v = re.sub(r"[\x00-\x1f\x7f]", "", v)
    return re.sub(r"\s+", " ", v).strip()


def build_specs_lines_from_fields(specs: Dict[str, Any]) -> str:
    """سطر مواصفات من الحقول السبعة فقط (بدون أسعار أو نص صفحة)."""
    pairs = [
        ("الماركة", "brand"),
        ("المقاس", "size"),
        ("رمز الحمولة والسرعة", "load_speed"),
        ("النقشة", "pattern"),
        ("بلد المنشأ", "country"),
        ("سنة الصنع", "year"),
        ("الضمان", "warranty"),
    ]
    lines: list[str] = []
    for label, key in pairs:
        v = _sanitize_spec_value(str(specs.get(key, "") or ""))
        if v:
            lines.append(f"{label}: {v}")
    return "\n".join(lines)


def combine_ai_narrative_with_specs(narrative: str, specs: Dict[str, Any]) -> str:
    """وصف نهائي = نص AI (بعد تنظيف HTML) + كتلة مواصفات منظمة فقط — لا دمج مع raw_text أو page_text."""
    n = strip_html_tags(narrative or "").strip()
    b = build_specs_lines_from_fields(specs).strip()
    if n and b:
        return f"{n}\n\n{b}"
    return n or b


def must_reject_description(text: str) -> bool:
    """
    True = يجب رفض الوصف بالكامل (لا يُحفظ كما هو؛ يُعاد التوليد أو الاستبدال).
    فلاتر إجبارية: أنماط المتجر، أرقام طويلة، HTML، إلخ.
    """
    if not str(text or "").strip():
        return True
    t = re.sub(r"\s+", " ", str(text).strip())
    if re.search(r"(هو:|هناك\s+\d+|4\.8|""|\d+,\d+)", t):
        return True
    if "هناك" in t:
        return True
    if "هو:" in t:
        return True
    if '""' in t:
        return True
    if re.search(r"\d{6,}", t):
        return True
    if re.search(r"</?[a-zA-Z][^>]{0,200}?>", t):
        return True
    if description_has_rejected_commerce_markers(t):
        return True
    return False


def validate_export_body(text: str) -> bool:
    """تحقق من الوصف النهائي المحفوظ (AI + مواصفات) قبل التصدير."""
    if not str(text or "").strip():
        return False
    if len(text.strip()) < 40 or len(text) > 2500:
        return False
    if must_reject_description(text):
        return False
    for word in BAD_WORDS + BAD_AI_PHRASES:
        if word in text:
            return False
    if re.search(r"هناك\s*\d+\s*زوار", text):
        return False
    return True


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


def _is_blank_field(value: Any) -> bool:
    if value is None:
        return True
    s = str(value).strip()
    if not s:
        return True
    low = s.lower()
    return low in ("none", "nan", "null", "undefined")


def add_line(label: str, value: Any) -> Optional[str]:
    if _is_blank_field(value):
        return None
    s = str(value).strip()
    return f"{label} {s}".strip()


def normalize_tire_size_display(raw: str) -> str:
    """
    توحيد شكل المقاس: 275/50R22XL، دون فراغات زائدة، مع دعم 275-50-22 و 275 / 50 R 22 XL.
    """
    if _is_blank_field(raw):
        return ""
    t = str(raw).strip()
    t = re.sub(r"\s+", " ", t)
    u = t.upper().replace("×", "/").replace("Ｒ", "R")
    u = re.sub(r"\s+", "", u)
    u = u.replace("ZR", "R")
    u = re.sub(r"(\d{3})-(\d{2,3})-(\d{2})(?![0-9])", r"\1/\2R\3", u)
    u = re.sub(r"(\d{3})/(\d{2,3})/(\d{2})(?![0-9])", r"\1/\2R\3", u)
    if re.match(r"^\d{3}/\d{2,3}R\d{2}[A-Z]*$", u):
        return u
    m = re.search(r"(\d{3})\s*/\s*(\d{2,3})\s*Z?R\s*(\d{2})", t, re.IGNORECASE)
    if m:
        base = f"{m.group(1)}/{m.group(2)}R{m.group(3)}"
        rest = t[m.end():].strip()
        tail = re.sub(r"[^A-Za-z]+", "", rest).upper() if rest else ""
        return base + tail
    return re.sub(r"\s+", "", t.strip())


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


def make_simple_description(p: Dict[str, Any]) -> str:
    """
    وصف منتج بصيغة ثابتة — أسطر فقط إذا للحقل قيمة حقيقية (لا None ولا nan ولا فراغ).
    """
    brand = "" if _is_blank_field(p.get("brand")) else str(p.get("brand")).strip()
    pattern = normalize_pattern_display("" if _is_blank_field(p.get("pattern")) else str(p.get("pattern")))
    size_raw = "" if _is_blank_field(p.get("size")) else str(p.get("size"))
    size = normalize_tire_size_display(size_raw)
    load_speed = "" if _is_blank_field(p.get("load_speed")) else strip_html_tags(str(p.get("load_speed")).strip())
    traction = "" if _is_blank_field(p.get("traction")) else strip_html_tags(str(p.get("traction")).strip())
    temperature = "" if _is_blank_field(p.get("temperature")) else strip_html_tags(str(p.get("temperature")).strip())
    treadwear = "" if _is_blank_field(p.get("treadwear")) else strip_html_tags(str(p.get("treadwear")).strip())
    country = "" if _is_blank_field(p.get("country")) else strip_html_tags(str(p.get("country")).strip())
    year = "" if _is_blank_field(p.get("year")) else str(p.get("year")).strip()
    warranty = "" if _is_blank_field(p.get("warranty")) else strip_html_tags(str(p.get("warranty")).strip())

    head = f"إطارات {brand}".strip() if brand else "إطارات"
    lines: List[Optional[str]] = [
        head,
        add_line("نقشة", pattern),
        add_line("مقاس", size),
        add_line("مؤشر السرعة والحمولة", load_speed),
        add_line("تراكشن", traction),
        add_line("تمبريتشر", temperature),
        add_line("تريدواير", treadwear),
        add_line("الصناعة", country),
        "بجودة عالية حسب مواصفات ومقاييس المملكة العربية السعودية",
        add_line("إنتاج", year),
        add_line("ضمان", warranty),
    ]
    return "\n".join(x for x in lines if x)


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

    if must_reject_description(description):
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
