"""
محرك SEO بالذكاء الاصطناعي لكل منتج إطار: توليد فريد، فحص جودة، JSON-LD.

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

# عبارات ممنوعة (حشو SEO / قوالب مكررة)
_BANNED_PHRASES = [
    "أفضل كفرات",
    "شراء كفرات",
    "كفرات أصلية",
    "يوفر ثبات ممتاز",
    "يقلل الانزلاق",
    "مناسب للاستخدام اليومي",
    "زوار يستعرضون",
    "إضافة إلى السلة",
    "اشتري الان",
    "سعر كفرات",
    "توصيل كفرات",
    "كفرات اونلاين",
    "شراء كفرات اونلاين",
]

_MIN = {
    "seo_title": 12,
    "meta_description": 80,
    "description_short": 50,
    "description_long": 220,
    "image_alt_text": 20,
}
_MAX = {
    "seo_title": 120,
    "meta_description": 320,
    "description_short": 600,
    "description_long": 8000,
    "image_alt_text": 200,
}


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


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
    bundle: Dict[str, Any], prior_long_texts: List[str], *, strict_internal: bool = True
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

    # تكرار جمل داخل نفس الوصف (للمخرجات AI فقط — النصوص القصيرة جدًا تُستثنى)
    if strict_internal:
        sents = _sentence_fingerprint(long_t)
        if len(sents) != len(set(sents)):
            reasons.append("duplicate_sentences_inside")

    # كثافة تكرار كلمة العلامة
    brand = _norm(bundle.get("_brand_ref", "")).lower()
    if brand and long_t:
        wc = len(re.findall(r"\w+", long_t.lower()))
        bc = len(re.findall(re.escape(brand), long_t.lower()))
        if wc and bc / wc > 0.12:
            reasons.append("brand_keyword_stuffing")

    # تشابه مع أصواف سابقة
    bg = _bigrams(long_t)
    for prev in prior_long_texts[-12:]:
        sim = _jaccard(bg, _bigrams(prev))
        if sim > 0.22:
            reasons.append(f"high_similarity:{sim:.2f}")
            break

    faq = bundle.get("faq")
    if not isinstance(faq, list) or len(faq) < 3:
        reasons.append("faq_too_small")
    else:
        for i, item in enumerate(faq[:8]):
            if not isinstance(item, dict):
                reasons.append("faq_bad_shape")
                break
            q, a = _norm(str(item.get("q", ""))), _norm(str(item.get("a", "")))
            if len(q) < 8 or len(a) < 15:
                reasons.append(f"faq_item_short:{i}")
                break

    kw = _norm(str(bundle.get("seo_keywords", "")))
    if kw:
        parts = [p.strip() for p in re.split(r"[,،;؛]", kw) if p.strip()]
        if len(parts) < 4:
            reasons.append("keywords_sparse")
        junk = sum(1 for p in parts if any(b in p.lower() for b in ("أفضل كفرات", "شراء كفرات", "كفرات أصلية")))
        if junk:
            reasons.append("keywords_junk")

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
    price = row.get("price", "")
    try:
        pval = str(float(str(price).replace(",", "")))
    except Exception:
        pval = ""
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
    if pval:
        obj["offers"] = {
            "@type": "Offer",
            "priceCurrency": "SAR",
            "price": pval,
            "availability": "https://schema.org/InStock",
            "url": url or None,
        }
        obj["offers"] = {k: v for k, v in obj["offers"].items() if v}
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
        "max_tokens": 3500,
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
        "source_hint": (prod.get("source_description") or "")[:800],
    }
    system = """You are an expert Arabic SEO copywriter for passenger/commercial tires in Saudi Arabia and the Gulf.
Write natural, human-sounding Arabic. No marketing spam. No keyword stuffing.
You MUST output a single valid JSON object only (no markdown fences).

Hard rules:
- Every field must be unique wording vs other SKUs; vary syntax, openings, and paragraph order.
- Do NOT reuse canned phrases like: "يوفر ثبات ممتاز", "يقلل الانزلاق", "مناسب للاستخدام اليومي", "أفضل كفرات", "شراء كفرات", "كفرات أصلية".
- Mention realistic driving contexts (heat, highway cruise, urban braking, light gravel, load when relevant) but stay factual.
- SEO title: Arabic, concise, includes brand + model + size + one differentiator (speed/load/terrain) — max 120 chars.
- Meta description: 1–2 sentences, benefit + spec hook, CTA-free spam, 80–300 chars preferred window.
- description_short: 2–4 sentences for listing cards.
- description_long: rich sections (مقدمة، للمن يصلح، ملاحظات مقاس/تحميل، صيانة بسيطة) — varied connectors; no duplicated sentences inside.
- seo_keywords: 6–12 items, comma-separated, tightly relevant to THIS size/use case (no generic spam list).
- faq: array of 4–6 objects {{"q":"...","a":"..."}} in Arabic, practical (noise, fitment, SUV suitability, speed index meaning) tailored to this tire.
- image_alt_text: descriptive Arabic alt for the product photo, includes brand model size, not keyword-stuffed.

If classification says SUV/AT/HT etc., reflect it without contradicting unknown specs."""

    user = f"""Generate unique Arabic SEO content for ONE tire product.
Avoid overlapping wording with these previously used snippets (paraphrase completely):
{avoid_snippets[:2500]}

Product JSON:
{json.dumps(payload, ensure_ascii=False)}

Return JSON with EXACTLY these keys:
seo_title, meta_description, description_short, description_long, seo_keywords, faq, image_alt_text
faq must be an array of objects with keys q and a only."""

    return system, user


def _minimal_fact_description(prod: Dict[str, Any], cls: Dict[str, Any]) -> Dict[str, Any]:
    """بدون AI: وصف حقائق فقط بترتيب فريد حسب الهاش (بدون قوالب تسويقية)."""
    brand = _norm(prod.get("brand", ""))
    model = _norm(prod.get("model", ""))
    size = _norm(prod.get("size", ""))
    ls = _norm(prod.get("load_speed", ""))
    seed = "|".join([brand, model, size, ls, cls.get("vehicle_segment", ""), cls.get("terrain_style", "")])
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16)
    lines = [
        f"منتج إطار: {brand} {model}، المقاس {size}.",
        f"تصنيف تقريبي: {cls.get('tire_category_ar','')} — مركبة مرجعية: {cls.get('vehicle_segment','')} — أسلوب: {cls.get('terrain_style','')}.",
        f"مؤشر الحمولة/السرعة الظاهر على المنتج: {ls}." if ls else "تحقق من بطاقة المنتج لمؤشر الحمولة والسرعة.",
        f"بلد المنشأ (إن وُجد في البيانات): {_norm(prod.get('country',''))}." if prod.get("country") else "",
        f"نقشة الإطار (إن وُجدت): {_norm(prod.get('pattern',''))}." if prod.get("pattern") else "",
    ]
    lines = [x for x in lines if x]
    # ترتيب دوّار بسيط لتقليل التطابق بين منتجات متجاورة
    rot = h % len(lines)
    lines = lines[rot:] + lines[:rot]
    long_t = "\n\n".join(lines) + "\n\nيُنصح بمطابقة المقاس مع جدول مصنع المركبة قبل الشراء."
    title = f"{brand} {model} {size} {ls}".strip()[:118]
    meta = (lines[0] + " " + (lines[1] if len(lines) > 1 else ""))[:300]
    kw = ", ".join(
        x
        for x in [
            f"إطار {brand} {size}".strip(),
            f"{brand} {model}".strip(),
            f"مقاس {size}",
            cls.get("terrain_style", ""),
            cls.get("vehicle_segment", ""),
        ]
        if x
    )
    faq = [
        {"q": "هل هذا المقاس مناسب لسيارتي؟", "a": "راجع جدول الإطارات في دليل المركبة أو استشر مركز تركيب معتمد لمطابقة المقاس والتحميل."},
        {"q": "ما أهمية رمز السرعة ومؤشر الحمولة؟", "a": "يحددان الحد الأقصى للسرعة الآمنة والحمولة لكل إطار؛ التزم بقيم مساوية أو أعلى من توصية المصنع."},
        {"q": "هل الإطار مناسب لاستخدام SUV؟", "a": f"التصنيف المرجعي للصفحة: {cls.get('vehicle_segment','غير محدد')} — راجع نوع النقشة والتوصية من المصنع لمركبتك."},
        {"q": "كيف أحافظ على عمر الإطار؟", "a": "ضغط هواء منتظم، موازنة عند التركيب، وتجنب المطبات الشديدة تساعد على توزيع التآكل بشكل أفضل."},
    ]
    alt = f"صورة إطار {brand} {model} مقاس {size}".strip()[:180]
    return {
        "seo_title": title,
        "meta_description": meta,
        "description_short": "\n".join(lines[:2]),
        "description_long": long_t,
        "seo_keywords": kw,
        "faq": faq,
        "image_alt_text": alt,
        "_brand_ref": brand,
    }


def _normalize_ai_bundle(raw: Dict[str, Any], brand: str) -> Dict[str, Any]:
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
        "description_short": _norm(str(raw.get("description_short", ""))),
        "description_long": _norm(str(raw.get("description_long", ""))),
        "seo_keywords": _norm(str(raw.get("seo_keywords", ""))),
        "faq": clean_faq,
        "image_alt_text": _norm(str(raw.get("image_alt_text", ""))),
        "_brand_ref": brand,
    }


def _faq_to_text(faq: List[Dict[str, str]]) -> str:
    parts = ["### أسئلة شائعة"]
    for it in faq:
        parts.append(f"س: {it.get('q','')}")
        parts.append(f"ج: {it.get('a','')}")
        parts.append("")
    return "\n".join(parts).strip()


def merge_export_description(bundle: Dict[str, Any]) -> str:
    """وحدة حقل الوصف للتصدير (سلة/ملفات)."""
    short = bundle.get("description_short", "")
    long_t = bundle.get("description_long", "")
    faq_txt = _faq_to_text(bundle.get("faq") or [])
    return "\n\n".join(x for x in [short, long_t, faq_txt] if x).strip()


def generate_seo_bundle(
    prod: Dict[str, Any],
    *,
    prior_long_samples: List[str],
    source_description: str = "",
) -> Dict[str, Any]:
    """
    يولد حزمة SEO كاملة. يحاول OpenAI ثم Gemini ثم الوضع الاحتياطي بدون قوالب تسويقية.
    """
    merged = {**prod, "source_description": source_description}
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
                bundle = _normalize_ai_bundle(raw, brand)
                ok, reasons = quality_check_bundle(bundle, prior_long_samples, strict_internal=True)
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
