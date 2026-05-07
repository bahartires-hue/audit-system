from __future__ import annotations

import re
import hashlib
from typing import Any, Dict


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _load_speed_parts(load_speed: str) -> tuple[str, str]:
    ls = _clean(load_speed).upper()
    m = re.match(r"^(\d{2,3})([A-Z])$", ls)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


def _pick(options: list[str], seed: str, offset: int = 0) -> str:
    if not options:
        return ""
    h = hashlib.sha1(f"{seed}:{offset}".encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(options)
    return options[idx]


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
    rf = bool(prod.get("rf"))
    pr = _clean(prod.get("pr", ""))
    ptype = "إطار سيارات"
    if pattern:
        p = pattern.lower()
        if "at" in p or "all terrain" in p:
            ptype = "إطار All Terrain"
        elif "mt" in p or "mud" in p:
            ptype = "إطار Mud Terrain"

    load_idx, speed_symbol = _load_speed_parts(load_speed)
    title = _clean(f"كفر {brand} {model} مقاس {size} {load_speed} {'XL' if xl else ''} {'RF' if rf else ''} {pr}")
    seed = "|".join([brand, model, size, load_speed, year, country, pattern, warranty, "xl" if xl else "", "rf" if rf else "", pr])

    intro_line = _pick(
        [
            f"{ptype} مصمم لتقديم توازن ممتاز بين الثبات والراحة وعمر الاستخدام.",
            f"{ptype} يوفر أداءً موثوقًا مع ثبات ممتاز على الطرق السريعة والداخلية.",
            f"{ptype} يقدم تجربة قيادة مستقرة مع تماسك جيد في الظروف اليومية المتنوعة.",
            f"{ptype} مناسب للاستخدام اليومي والرحلات الطويلة بفضل تصميمه المتين وتقنيته الحديثة.",
        ],
        seed,
        1,
    )
    usage_line = _pick(
        [
            "مناسب للسيارات التي تحتاج كفاءة عالية في التحكم وتقليل الضوضاء أثناء القيادة.",
            "خيار عملي لمن يبحث عن كفر متوازن بين الراحة، الثبات، وعمر تشغيلي أطول.",
            "يساعد على تحسين استجابة المركبة في المنعطفات مع توزيع متوازن للضغط على سطح الطريق.",
            "يوفر ثقة أكبر أثناء القيادة بسرعات مختلفة مع أداء متزن على الطرق المعبدة.",
        ],
        seed,
        2,
    )
    intro = f"{title}\n\n{intro_line} {usage_line}"
    if pattern:
        intro += f" يتميز بنقشة {pattern} التي تدعم التماسك وتقلل احتمالية الانزلاق."

    durability = _pick(
        [
            "تم تطوير هيكل الإطار بمواد متينة تساعد على توزيع الضغط بشكل متوازن ودعم الثبات في المنعطفات.",
            "تصميم الدعسة والجدار الجانبي يعزز الثبات ويمنح الإطار قدرة أفضل على تحمل الاستخدام المستمر.",
            "البنية الداخلية للإطار مصممة لتقليل التآكل غير المنتظم وتحسين عمر الإطار على المدى الطويل.",
            "القنوات والنقشة المتقدمة تساعد على تحسين الثبات وتقليل فقدان التماسك في الظروف المتغيرة.",
        ],
        seed,
        3,
    )
    if xl:
        durability += " كما أن نسخة XL توفر دعماً إضافياً لتحمل الأحمال."
    if rf:
        durability += " كما أن تقنية RunFlat (RF) تمنح قدرة أفضل على الاستمرار المؤقت بعد فقدان الضغط."

    specs = [
        f"الماركة: {brand}" if brand else "",
        f"الموديل: {model}" if model else "",
        f"المقاس: {size}" if size else "",
        f"مؤشر الحمولة والسرعة: {load_speed}" if load_speed else "",
        f"مؤشر الحمولة: {load_idx}" if load_idx else "",
        f"رمز السرعة: {speed_symbol}" if speed_symbol else "",
        f"عدد الطبقات: {pr}" if pr else "",
        f"سنة الصنع: {year}" if year else "",
        f"بلد المنشأ: {country}" if country else "",
        f"النقشة: {pattern}" if pattern else "",
        f"الضمان: {warranty}" if warranty else "",
    ]
    specs_text = "\n".join(x for x in specs if x)

    keywords_list = [
        f"كفر {brand}".strip(),
        f"{brand} {model}".strip(),
        f"مقاس {size}".strip(),
        f"إطار {size}".strip() if size else "",
        "إطارات سيارات",
        "أفضل كفرات",
        "سعر كفرات",
        "شراء كفرات اونلاين",
        "كفرات أصلية",
        "توصيل كفرات",
    ]
    if load_speed:
        keywords_list.append(f"تحميل {load_speed}")
    if pattern:
        keywords_list.append(f"نقشة {pattern}")
    keywords = ", ".join(
        x
        for x in keywords_list
        if _clean(x)
    )

    extra = _clean(source_description)
    if extra:
        extra = f"\n\nمعلومات إضافية:\n{extra[:700]}"

    cta = _pick(
        [
            "اطلب الآن للحصول على إطار بجودة عالية وسعر منافس مع خدمة موثوقة.",
            "اختر هذا المقاس الآن واستفد من أداء عملي يناسب الاستخدام اليومي والرحلات.",
            "خيار مثالي لمن يريد توازنًا بين الأمان، الراحة، والقيمة مقابل السعر.",
            "احجزه الآن لضمان توفر المقاس المناسب لسيارتك بأفضل أداء ممكن.",
        ],
        seed,
        4,
    )

    return (
        f"{intro}\n\n"
        f"{durability}\n\n"
        "المواصفات\n\n"
        f"{specs_text}\n\n"
        "كلمات مفتاحية SEO\n"
        f"{keywords}\n\n"
        f"{cta}"
        f"{extra}"
    ).strip()

