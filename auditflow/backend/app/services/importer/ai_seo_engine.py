"""
محرك SEO لوصف منتجات متجر «بحر الإطارات» (الدمام): توليد من حقول منظمة فقط، بدون نسخ وصف أي متجر آخر.

متغيرات البيئة:
- OPENAI_API_KEY + اختياري OPENAI_MODEL (افتراضي gpt-4o-mini)
- GEMINI_API_KEY + اختياري GEMINI_MODEL (افتراضي gemini-2.0-flash)
- AUDITFLOW_SEO_PROVIDER: openai | gemini | none | (فارغ = تلقائي حسب المفتاح المتاح)
- AUDITFLOW_STORE_PUBLIC_BASE أو PUBLIC_BASE_URL: أساس الروابط لـ canonical و JSON-LD
"""
from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

log = logging.getLogger("importer.ai_seo")

STORE_AR = "بحر الإطارات"

# يجب أن تظهر في كل وصف (صياغة طبيعية — لا تكرار نفس الجملة بين المنتجات)
_MANDATORY_PHRASES = [
    "بحر الإطارات",
    "كفرات في الدمام",
    "كفرات الدمام",
    "شراء كفرات اونلاين",
    "كفرات سيارات",
]

# اختر 2–3 من هذه في كل وصف مع تنويع بين المنتجات
_LOCAL_SEO_POOL = [
    "بحر الإطارات",
    "كفرات في الدمام",
    "كفرات الدمام",
    "محل كفرات في الدمام",
    "شراء كفرات اونلاين",
    "تركيب كفرات في الدمام",
]

# عبارات ممنوعة (حشو عام / تسويق فارغ)
_BANNED_PHRASES = [
    "أفضل كفرات",
    "كفرات أصلية",
    "يوفر ثبات ممتاز",
    "يقلل الانزلاق",
    "مناسب للاستخدام اليومي",
    "زوار يستعرضون",
    "إضافة إلى السلة",
    "اشتري الان",
    "سعر كفرات",
    "توصيل كفرات",
    "mailto:",
]

_DESC_MIN = 700
_DESC_MAX = 1000

_MIN = {
    "seo_title": 12,
    "meta_description": 80,
    "description_short": 40,
    "description_long": _DESC_MIN,
    "image_alt_text": 20,
}
_MAX = {
    "seo_title": 120,
    "meta_description": 300,
    "description_short": 280,
    "description_long": _DESC_MAX,
    "image_alt_text": 200,
}


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _clip_desc(s: str, mx: int = _DESC_MAX) -> str:
    t = _norm(s)
    if len(t) <= mx:
        return t
    cut = t[:mx]
    if " " in cut:
        return cut.rsplit(" ", 1)[0].rstrip() + "…"
    return cut + "…"


def _forbidden_merchant_patterns(text: str) -> List[str]:
    """أنماط ممنوعة: أسعار، ضريبة، مخزون، روابط، بريد، شروط طويلة."""
    hits: List[str] = []
    low = text.lower()
    if re.search(r"https?://", text, re.I):
        hits.append("url")
    if "@" in text or re.search(r"\b[\w.+-]+@[\w.-]+\.\w+\b", text):
        hits.append("email")
    if re.search(r"\b\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*(?:ر\.س|ريال|sar|ريالات)\b", low, re.I):
        hits.append("price")
    if re.search(r"\b(?:ضريبة|القيمة المضافة|vat|مخزون|رصيد)\b", low, re.I):
        hits.append("commerce_meta")
    return hits


def _mandatory_ok(text: str, brand: str, size: str, pattern: str) -> Tuple[bool, List[str]]:
    miss: List[str] = []
    for ph in _MANDATORY_PHRASES:
        if ph not in text:
            miss.append(f"missing:{ph}")
    b = _norm(brand)
    sz = _norm(size)
    if b and b not in text and b.replace(" ", "") not in text.replace(" ", ""):
        miss.append("missing_brand")
    if sz and sz.upper().replace(" ", "") not in text.upper().replace(" ", ""):
        miss.append("missing_size")
    pat = _norm(pattern)
    if pat and pat.lower() not in text.lower():
        miss.append("missing_pattern")
    return (len(miss) == 0, miss)


def _load_speed_split(load_speed: str) -> Tuple[str, str]:
    ls = _norm(load_speed).upper()
    m = re.match(r"^(\d{2,3})([A-Z])$", ls)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


def classify_tire_product(prod: Dict[str, Any]) -> Dict[str, Any]:
    """تصنيف ذكي من المقاس والنقشة والموديل (بدون AI)."""
    brand = _norm(prod.get("brand", ""))
    model = _norm(prod.get("model", "")).lower()
    pattern = _norm(prod.get("pattern", "")).lower()
    size = _norm(prod.get("size", ""))
    rim = ""
    m_rim = re.search(r"R(\d{2})", size, re.I)
    if m_rim:
        rim = m_rim.group(1)
    try:
        rim_n = int(rim) if rim else 0
    except ValueError:
        rim_n = 0

    blob = f"{model} {pattern} {brand.lower()}"

    # مركبة
    vehicle = "عام"
    if rim_n >= 18 or any(x in blob for x in ("suv", "4x4", "4wd", "x5", "land", "باتrol", "برادو", "prado", "pajero")):
        vehicle = "SUV"
    elif any(x in blob for x in ("van", "فان", "commercial", "crafter", "sprinter", "transit", "hiace", "هايس")):
        vehicle = "تجاري"
    elif rim_n and rim_n <= 16 and any(x in blob for x in ("comfort", "eco", "touring", "سيدان", "sedan")):
        vehicle = "سيدان"

    # أسلوب طريق / أداء
    terrain = "طريق معبد"
    if any(x in pattern for x in ("at", "a/t", "all terrain", "all-terrain")) or "all terrain" in blob:
        terrain = "All Terrain"
    elif any(x in pattern for x in ("mt", "mud", "m/t")):
        terrain = "Mud Terrain"
    elif any(x in pattern for x in ("ht", "h/t", "highway")) or "highway" in blob:
        terrain = "Highway Terrain"
    elif any(x in blob for x in ("sport", "uhp", "pilot", "potenza", "سبورت")):
        terrain = "Sport / UHP"
    elif any(x in blob for x in ("winter", "snow", "ثلج", "شتوي")):
        terrain = "شتوي"

    tire_category_ar = "إطار سيارات"
    if vehicle == "SUV" and terrain == "Highway Terrain":
        tire_category_ar = "إطار SUV طرق معبدة (HT)"
    elif vehicle == "SUV" and terrain == "All Terrain":
        tire_category_ar = "إطار SUV متعدد الاستخدامات (AT)"
    elif terrain == "Sport / UHP":
        tire_category_ar = "إطار أداء رياضي / سرعات أعلى"
    elif vehicle == "تجاري":
        tire_category_ar = "إطار مركبات تجارية"

    return {
        "vehicle_segment": vehicle,
        "terrain_style": terrain,
        "tire_category_ar": tire_category_ar,
        "rim_inches": rim or "",
    }


def _bigrams(text: str) -> set:
    t = re.sub(r"\s+", " ", _norm(text).lower())
    words = [w for w in re.split(r"[^\w\u0600-\u06FF]+", t) if len(w) > 2]
    return {f"{a}|{b}" for a, b in zip(words, words[1:])} if len(words) > 1 else set()


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _sentence_fingerprint(text: str) -> List[str]:
    parts = re.split(r"[.!\n؟?]+", text)
    return [re.sub(r"\s+", " ", p).strip().lower() for p in parts if len(p.strip()) > 25]


def quality_check_bundle(
    bundle: Dict[str, Any],
    prior_long_texts: List[str],
    *,
    strict_internal: bool = True,
    prior_openings: Optional[List[str]] = None,
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    for key, mn in _MIN.items():
        v = _norm(str(bundle.get(key, "")))
        if len(v) < mn:
            reasons.append(f"{key}_too_short")
    for key, mx in _MAX.items():
        v = _norm(str(bundle.get(key, "")))
        if v and len(v) > mx:
            reasons.append(f"{key}_too_long")

    long_t = _norm(bundle.get("description_long", ""))
    low = long_t.lower()
    for bad in _BANNED_PHRASES:
        if bad.lower() in low:
            reasons.append(f"banned_phrase:{bad[:24]}")

    for hit in _forbidden_merchant_patterns(long_t):
        reasons.append(f"forbidden:{hit}")

    ok_m, miss_m = _mandatory_ok(
        long_t, bundle.get("_brand_ref", ""), bundle.get("_size_ref", ""), bundle.get("_pattern_ref", "")
    )
    if not ok_m:
        reasons.extend(miss_m)

    if "محل كفرات في الدمام" not in long_t and "تركيب كفرات في الدمام" not in long_t:
        reasons.append("missing_extra_local")

    if strict_internal:
        sents = _sentence_fingerprint(long_t)
        if len(sents) != len(set(sents)):
            reasons.append("duplicate_sentences_inside")

    brand = _norm(bundle.get("_brand_ref", "")).lower()
    if brand and long_t:
        wc = len(re.findall(r"[\w\u0600-\u06FF]+", long_t.lower()))
        bc = len(re.findall(re.escape(brand), long_t.lower()))
        if wc and bc / wc > 0.14:
            reasons.append("brand_keyword_stuffing")

    bg = _bigrams(long_t)
    for prev in prior_long_texts[-12:]:
        sim = _jaccard(bg, _bigrams(prev))
        if sim > 0.24:
            reasons.append(f"high_similarity:{sim:.2f}")
            break

    op = long_t[:100].strip()
    if prior_openings:
        for p in prior_openings[-20:]:
            if p and op and (op in p or p in op or _jaccard(_bigrams(op), _bigrams(p)) > 0.35):
                reasons.append("repeated_opening")
                break

    faq = bundle.get("faq")
    if not isinstance(faq, list) or len(faq) < 2:
        reasons.append("faq_too_small")
    else:
        for i, item in enumerate(faq[:6]):
            if not isinstance(item, dict):
                reasons.append("faq_bad_shape")
                break
            q, a = _norm(str(item.get("q", ""))), _norm(str(item.get("a", "")))
            if len(q) < 6 or len(a) < 12:
                reasons.append(f"faq_item_short:{i}")
                break

    kw = _norm(str(bundle.get("seo_keywords", "")))
    if kw:
        parts = [p.strip() for p in re.split(r"[,،;؛]", kw) if p.strip()]
        if len(parts) < 3:
            reasons.append("keywords_sparse")

    return (len(reasons) == 0, reasons)


def build_json_ld_product(row: Dict[str, Any], canonical_base: str) -> str:
    """Product schema.org JSON-LD (توليد تقني موثوق، ليس من نص حر للنموذج)."""
    name = _norm(row.get("product_title") or row.get("name", ""))
    desc = _norm(row.get("meta_description") or row.get("description_short", ""))
    img = _norm(row.get("image_cloudinary") or "")
    sku = _norm(row.get("size", "")) + "-" + _norm(row.get("brand", "")) + "-" + _norm(row.get("model", ""))
    sku = re.sub(r"[^\w\-]+", "-", sku).strip("-")[:80] or "sku"
    slug = hashlib.sha1(name.encode("utf-8")).hexdigest()[:12]
    base = (canonical_base or "").rstrip("/")
    url = f"{base}/products/{slug}" if base else ""
    obj: Dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": name,
        "description": desc[:5000],
        "sku": sku[:100],
        "brand": {"@type": "Brand", "name": _norm(row.get("brand", ""))},
    }
    if img:
        obj["image"] = [img]
    if url:
        obj["url"] = url
    return json.dumps(obj, ensure_ascii=False)


def _openai_generate_json(system: str, user: str, model: str) -> Dict[str, Any]:
    key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("missing_openai_key")
    url = "https://api.openai.com/v1/chat/completions"
    body = {
        "model": model,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "response_format": {"type": "json_object"},
        "temperature": 0.85,
        "max_tokens": 2200,
    }
    with httpx.Client(timeout=90.0) as client:
        r = client.post(url, headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, json=body)
        r.raise_for_status()
        data = r.json()
    content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or "{}"
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```[a-zA-Z]*\s*", "", content)
        content = re.sub(r"\s*```$", "", content).strip()
    return json.loads(content)


def _gemini_generate_json(system: str, user: str, model: str) -> Dict[str, Any]:
    key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("missing_gemini_key")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    prompt = system + "\n\n" + user
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.85,
            "maxOutputTokens": 4096,
            "responseMimeType": "application/json",
        },
    }
    with httpx.Client(timeout=90.0) as client:
        r = client.post(url, params={"key": key}, json=body)
        r.raise_for_status()
        data = r.json()
    cands = data.get("candidates") or []
    if not cands:
        raise RuntimeError("gemini_empty_candidates")
    parts = (cands[0].get("content") or {}).get("parts") or []
    if not parts:
        raise RuntimeError("gemini_empty_parts")
    text = parts[0].get("text") or "{}"
    return json.loads(text)


def _build_prompts(prod: Dict[str, Any], cls: Dict[str, Any], avoid_snippets: str) -> Tuple[str, str]:
    li, sp = _load_speed_split(str(prod.get("load_speed", "")))
    payload = {
        "store": STORE_AR,
        "city": "الدمام",
        "brand": prod.get("brand", ""),
        "model": prod.get("model", ""),
        "size": prod.get("size", ""),
        "load_index": li,
        "speed_rating": sp,
        "load_speed_combined": prod.get("load_speed", ""),
        "pattern": prod.get("pattern", ""),
        "country": prod.get("country", ""),
        "year": prod.get("year", ""),
        "warranty": prod.get("warranty", ""),
        "xl": bool(prod.get("xl")),
        "rf": bool(prod.get("rf")),
        "pr": prod.get("pr", ""),
        "classification": cls,
    }
    system = f"""You write unique Arabic SEO product copy for the tire shop «{STORE_AR}» in Dammam only.
Output ONE valid JSON object (no markdown). Human tone, no spam, no robotic repetition.

CRITICAL:
- Do NOT copy or paraphrase text from any other store or supplier page. Use ONLY the structured fields provided (brand, size, pattern, classification, country, year, warranty, load/speed). Never invent stock, links, email, tax, price, promotions, or competitor names.
- description_long MUST be between {_DESC_MIN} and {_DESC_MAX} Arabic characters (count characters, not tokens). One flowing Arabic text (no bullet URL lists, no raw links).
- You MUST naturally include ALL of these exact phrases somewhere in description_long (can be split across sentences): {json.dumps(_MANDATORY_PHRASES, ensure_ascii=False)}
- Also include the exact tire size string as given in JSON field "size", the brand name, and if "pattern" is non-empty include that pattern text naturally.
- Additionally include at least ONE of: «محل كفرات في الدمام» OR «تركيب كفرات في الدمام» (pick one; vary vs other SKUs).
- Do NOT start multiple products with the same opening clause; vary paragraph order and connectors. Avoid repeating one fixed template like «في بحر الإطارات نوفر كفرات في الدمام…» across products.
- Forbidden inside all text fields: http/https URLs, @ emails, explicit prices/currency words (ريال/ر.س/SAR), VAT words, inventory/stock claims.

JSON keys required:
seo_title, meta_description, description_short, description_long, seo_keywords, faq, image_alt_text
- seo_title ≤120 chars Arabic, includes brand+model+size.
- meta_description 80–260 chars, no URLs/prices.
- description_short: 2–3 sentences teaser (≤260 chars), may lightly echo local terms but main compliance is description_long.
- seo_keywords: 5–10 comma-separated Arabic keywords tightly tied to this SKU + Dammam local intent; no generic spam.
- faq: 3–5 objects {{"q","a"}} Arabic, practical, no prices/links; mention fitment/speed/load qualitatively only.
- image_alt_text: Arabic, includes brand+size, mentions {STORE_AR} once max, no stuffing."""

    user = f"""Write for ONE SKU at {STORE_AR}.

Previously used long-description snippets (do NOT imitate structure; change openings and sentence rhythm):
{avoid_snippets[:2200]}

Structured product fields (ONLY source of truth):
{json.dumps(payload, ensure_ascii=False)}

Return JSON with keys exactly:
seo_title, meta_description, description_short, description_long, seo_keywords, faq, image_alt_text"""

    return system, user


def _mandatory_pack_sentence(brand: str, size: str, pat: str) -> str:
    p = f" مع ذكر نقشة {pat} ضمن السياق." if pat else ""
    return (
        f"في {STORE_AR} نربط بين كفرات في الدمام وكفرات الدمام ضمن خانة كفرات سيارات، "
        f"مع تسهيل شراء كفرات اونلاين لمقاس {size} وماركة {brand}.{p}"
    )


def _minimal_fact_description(prod: Dict[str, Any], cls: Dict[str, Any]) -> Dict[str, Any]:
    """بدون AI: وصف يلتزم كلمات بحر الإطارات والدمام مع تنويع آلية حسب الهاش."""
    brand = _norm(prod.get("brand", ""))
    model = _norm(prod.get("model", ""))
    size = _norm(prod.get("size", ""))
    ls = _norm(prod.get("load_speed", ""))
    pat = _norm(prod.get("pattern", ""))
    country = _norm(prod.get("country", ""))
    year = _norm(prod.get("year", ""))
    warranty = _norm(prod.get("warranty", ""))
    seed = "|".join([brand, model, size, ls, cls.get("vehicle_segment", ""), cls.get("terrain_style", ""), pat])
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16)
    extra_local = "تركيب كفرات في الدمام" if h % 2 == 0 else "محل كفرات في الدمام"

    openers = [
        f"كفر {brand} مقاس {size} يقدّم أسلوب قيادة متزنًا على الطرق المعبدة، مع تركيز على راحة مناسبة للتنقل داخل المدينة والخروج للطرق السريعة.",
        f"عند اختيار كفرات سيارات بمقاس {size} من ماركة {brand}، غالبًا ما يبحث المستخدم عن توازن بين ثبات المنعطفات وهدوء المقصورة؛ هذا الخيار يستهدف ذلك التوازن بشكل عملي.",
        f"مقاس {size} لماركة {brand} يُعد من المقاسات الشائعة ضمن فئة {cls.get('tire_category_ar','إطار سيارات')}، مع ملاحظة أن أسلوب الاستخدام المرجّح هنا: {cls.get('terrain_style','طريق معبد')}.",
        f"لو كنت تفضّل التخطيط مسبقًا، فإن شراء كفرات اونلاين يساعدك على مقارنة الخيارات؛ وفي سياق كفرات الدمام يهمنا أن يكون الوصف واضحًا حول {brand} مقاس {size}.",
    ]
    op = openers[h % len(openers)]

    pat_line = f"النقشة الظاهرة في البيانات: {pat}." if pat else "لم تُذكر نقشة محددة في البيانات؛ راجع بطاقة المنتج عند الاستلام."

    spec = f"تصنيف تقريبي: {cls.get('tire_category_ar','')} — فئة مركبة مرجعية: {cls.get('vehicle_segment','')} — أسلوب طريق: {cls.get('terrain_style','')}."
    load_line = f"رمز التحميل والسرعة الظاهر: {ls}." if ls else "تحقق من بطاقة المنتج لرمز التحميل والسرعة المناسب لسيارتك."
    cy = f"بلد المنشأ: {country}." if country else ""
    yw = f"سنة الصنع المذكورة في البيانات: {year}." if year else ""
    wr = f"الضمان المذكور في البيانات: {warranty}." if warranty else ""

    mid_variants = [
        f"ضمن {STORE_AR} نعمل على تسهيل رحلة من يبحث عن كفرات في الدمام عبر تجربة طلب أوضح، مع اهتمام بأن يجد قارئ كفرات الدمام معلومة مفيدة عن المقاس والاستخدام دون مبالغة.",
        f"من جهة أخرى، يرتبط طلب كفرات سيارات بمقاس محدد مثل {size} بقراءة جدول الإطارات؛ لذلك نذكّر أن {extra_local} قد يكون خيارًا مناسبًا بعد التأكد من المقاس.",
        f"لمن يهتم بكفرات الدمام بشكل عام: يبقى {brand} مقاس {size} خيارًا عمليًا ضمن تشكيلة كفرات سيارات متنوعة، مع إبراز {extra_local} كجزء من منظومة خدمات محلية نعمل على تطويرها.",
    ]
    mid = mid_variants[h % len(mid_variants)]

    close_variants = [
        f"باختصار، {STORE_AR} يهدف إلى دعم شراء كفرات اونلاين بخطوات واضحة، مع الحفاظ على صياغة تشرح {size} و{brand} دون حشو مزعج.",
        f"ختامًا: نركّز في {STORE_AR} على أن تكون كلمات مثل كفرات في الدمام وكفرات الدمام مفيدة للقارئ، مع إبقاء النص قريبًا من واقع الاستخدام اليومي للإطار.",
        f"أخيرًا، إن كنت تبحث عن كفرات سيارات بمقاس {size}، فالمعلومات أعلاه تساعدك على فهم الصنف قبل الطلب، مع إبراز {STORE_AR} كوجهة محلية تدعم شراء كفرات اونلاين بشكل منظم.",
    ]
    close = close_variants[(h >> 3) % len(close_variants)]
    pack = _mandatory_pack_sentence(brand, size, pat)
    tail = [op, spec, load_line, pat_line, cy, yw, wr, mid, close]
    tail = [c for c in tail if c]
    if tail:
        r = h % len(tail)
        tail = tail[r:] + tail[:r]
    chunks = [pack] + tail
    long_t = _norm(" ".join(chunks))
    while len(long_t) < _DESC_MIN:
        long_t += f" ننصح بمطابقة {size} مع توصية مصنع المركبة قبل الاعتماد النهائي."
    long_t = _clip_desc(long_t, _DESC_MAX)

    title = f"{brand} {model} {size}".strip()[:118]
    meta = _clip_desc(
        f"{STORE_AR}: {brand} مقاس {size} — {cls.get('tire_category_ar','إطار')} — معلومات مختصرة للمقارنة قبل الطلب.",
        280,
    )
    kw = ", ".join(
        x
        for x in [
            f"{brand} {size}",
            "كفرات الدمام",
            "كفرات في الدمام",
            extra_local,
            cls.get("terrain_style", ""),
            "كفرات سيارات",
        ]
        if x
    )
    faq = [
        {"q": f"هل مقاس {size} يناسب سيارتي؟", "a": "اعتمد على جدول الإطارات في دليل المركبة أو استشر فني تركيب معتمد قبل الاعتماد النهائي."},
        {"q": "ما أهمية رمز التحميل والسرعة؟", "a": "يحددان حدود التحميل والسرعة الآمنة للإطار؛ التزم بما يوصي به مصنع السيارة."},
        {"q": f"لماذا يُذكر {STORE_AR} في الوصف؟", "a": "لأن النص مخصص لتجربة تسوق محلية في الدمام ولا يمثل نصًا منسوخًا من متجر آخر."},
    ]
    alt = _clip_desc(f"{STORE_AR} — {brand} {model} مقاس {size}", 180)
    short = _clip_desc(op + " " + spec, 250)
    return {
        "seo_title": title,
        "meta_description": meta,
        "description_short": short,
        "description_long": long_t,
        "seo_keywords": kw,
        "faq": faq,
        "image_alt_text": alt,
        "_brand_ref": brand,
        "_size_ref": size,
        "_pattern_ref": pat,
    }


def _normalize_ai_bundle(raw: Dict[str, Any], brand: str, size: str = "", pattern: str = "") -> Dict[str, Any]:
    faq = raw.get("faq")
    if isinstance(faq, str):
        try:
            faq = json.loads(faq)
        except Exception:
            faq = []
    if not isinstance(faq, list):
        faq = []
    clean_faq = []
    for it in faq:
        if isinstance(it, dict) and "q" in it and "a" in it:
            clean_faq.append({"q": _norm(str(it["q"])), "a": _norm(str(it["a"]))})
    return {
        "seo_title": _norm(str(raw.get("seo_title", ""))),
        "meta_description": _norm(str(raw.get("meta_description", ""))),
        "description_short": _clip_desc(str(raw.get("description_short", "")), 260),
        "description_long": _clip_desc(str(raw.get("description_long", "")), _DESC_MAX),
        "seo_keywords": _norm(str(raw.get("seo_keywords", ""))),
        "faq": clean_faq,
        "image_alt_text": _norm(str(raw.get("image_alt_text", ""))),
        "_brand_ref": brand,
        "_size_ref": _norm(size),
        "_pattern_ref": _norm(pattern),
    }


def _faq_to_text(faq: List[Dict[str, str]]) -> str:
    parts = ["### أسئلة شائعة"]
    for it in faq:
        parts.append(f"س: {it.get('q','')}")
        parts.append(f"ج: {it.get('a','')}")
        parts.append("")
    return "\n".join(parts).strip()


def merge_export_description(bundle: Dict[str, Any]) -> str:
    """حقل الوصف للتصدير: نص واحد ضمن الحد الأقصى (بدون لصق FAQ لتفادي تجاوز الحد)."""
    return _clip_desc(str(bundle.get("description_long", "")), _DESC_MAX)


def generate_seo_bundle(
    prod: Dict[str, Any],
    *,
    prior_long_samples: List[str],
    source_description: str = "",
) -> Dict[str, Any]:
    """
    يولد حزمة SEO كاملة. يحاول OpenAI ثم Gemini ثم الوضع الاحتياطي بدون قوالب تسويقية.
    """
    # لا نستخدم وصف المتجر المصدر — تجاهل source_description عن قصد
    merged = {**prod}
    cls = classify_tire_product(merged)
    brand = _norm(merged.get("brand", ""))

    explicit = (os.getenv("AUDITFLOW_SEO_PROVIDER") or "").strip().lower()
    if explicit == "none":
        provider = "none"
    elif explicit in ("openai", "gemini"):
        provider = explicit
    else:
        if (os.getenv("OPENAI_API_KEY") or "").strip():
            provider = "openai"
        elif (os.getenv("GEMINI_API_KEY") or "").strip():
            provider = "gemini"
        else:
            provider = "none"

    avoid = "\n---\n".join(prior_long_samples[-5:])[:2400]

    def try_ai() -> Optional[Dict[str, Any]]:
        system, user = _build_prompts(merged, cls, avoid)
        last_err: Optional[Exception] = None
        for attempt in range(2):
            try:
                if provider == "openai":
                    model = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()
                    raw = _openai_generate_json(system, user + ("\nRetry: tighten uniqueness." if attempt else ""), model)
                elif provider == "gemini":
                    model = (os.getenv("GEMINI_MODEL") or "gemini-2.0-flash").strip()
                    raw = _gemini_generate_json(system, user + ("\nRetry: tighten uniqueness." if attempt else ""), model)
                else:
                    return None
                bundle = _normalize_ai_bundle(raw, brand, str(merged.get("size", "")), str(merged.get("pattern", "")))
                openings = [t[:120].strip() for t in prior_long_samples[-24:] if t.strip()]
                ok, reasons = quality_check_bundle(
                    bundle, prior_long_samples, strict_internal=True, prior_openings=openings
                )
                if ok:
                    return bundle
                log.info("ai_seo qc fail attempt=%s reasons=%s", attempt, reasons)
                user += "\n\nPrevious JSON failed checks: " + ",".join(reasons[:8])
            except Exception as e:
                last_err = e
                log.warning("ai_seo attempt error: %s", e)
        if last_err:
            log.warning("ai_seo giving up after errors: %s", last_err)
        return None

    bundle: Optional[Dict[str, Any]] = None
    if provider in ("openai", "gemini"):
        bundle = try_ai()
        if bundle is not None:
            bundle["_source"] = provider
    if bundle is None:
        bundle = _minimal_fact_description(merged, cls)
        bundle["_source"] = "minimal"
        ok, reasons = quality_check_bundle(bundle, prior_long_samples, strict_internal=False)
        if not ok:
            log.warning("minimal bundle qc issues (ignored): %s", reasons)

    bundle["vehicle_segment"] = cls["vehicle_segment"]
    bundle["terrain_style"] = cls["terrain_style"]
    bundle["tire_category_ar"] = cls["tire_category_ar"]
    bundle["description_export"] = merge_export_description(bundle)
    return bundle


def apply_bundle_to_row(row: Dict[str, Any], bundle: Dict[str, Any], canonical_base: str) -> None:
    row["seo_title"] = bundle["seo_title"]
    row["meta_description"] = bundle["meta_description"]
    row["description_short"] = bundle["description_short"]
    row["description_long"] = bundle["description_long"]
    row["description"] = bundle["description_export"]
    row["keywords"] = bundle["seo_keywords"]
    row["seo_keywords"] = bundle["seo_keywords"]
    row["image_alt_text"] = bundle["image_alt_text"]
    row["faq_json"] = json.dumps(bundle.get("faq") or [], ensure_ascii=False)
    row["vehicle_segment"] = bundle.get("vehicle_segment", "")
    row["terrain_style"] = bundle.get("terrain_style", "")
    row["tire_category_ar"] = bundle.get("tire_category_ar", "")
    row["json_ld"] = build_json_ld_product(row, canonical_base)
    row["og_title"] = bundle["seo_title"][:90]
    row["og_description"] = bundle["meta_description"][:200]
    slug = hashlib.sha1(_norm(row.get("product_title", "")).encode("utf-8")).hexdigest()[:12]
    base = (canonical_base or "").rstrip("/")
    row["canonical_url"] = f"{base}/products/{slug}" if base else ""
    src = bundle.get("_source") or "minimal"
    row["seo_content_mode"] = "ai" if src in ("openai", "gemini") else "facts"
    img = _norm(row.get("image_cloudinary", ""))
    row["social_meta_json"] = json.dumps(
        {
            "og:type": "product",
            "og:title": row["og_title"],
            "og:description": row["og_description"],
            "og:image": img,
            "twitter:card": "summary_large_image",
            "twitter:title": row["og_title"],
            "twitter:description": row["og_description"],
        },
        ensure_ascii=False,
    )
    canon = row["canonical_url"]
    row["meta_link_tags_html"] = (
        (f'<link rel="canonical" href="{html.escape(canon)}" />' if canon else "")
        + "\n"
        + f'<meta property="og:title" content="{html.escape(row["og_title"])}" />'
        + "\n"
        + f'<meta property="og:description" content="{html.escape(row["og_description"])}" />'
        + (f'\n<meta property="og:image" content="{html.escape(img)}" />' if img else "")
    ).strip()
