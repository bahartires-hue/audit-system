from __future__ import annotations

from typing import Any, Dict

from .cleaner import slugify


def build_seo_fields(prod: Dict[str, Any]) -> Dict[str, str]:
    brand = (prod.get("brand") or "").strip()
    model = (prod.get("model") or "").strip()
    size = (prod.get("size") or "").strip()
    ls = (prod.get("load_speed") or "").strip()
    base = f"كفر {brand} {model} مقاس {size}".strip()
    if ls:
        base += f" تحميل {ls}"

    title = " ".join(base.split())[:120]
    meta = f"{title} - جودة عالية، سعر مناسب، وضمان موثوق. اطلب الآن."[:170]
    keywords = ", ".join(
        x
        for x in [
            "كفرات",
            brand,
            model,
            size,
            ls,
            f"{brand} {model}".strip(),
            f"{brand} {size}".strip(),
        ]
        if x
    )
    image_slug = slugify(f"{brand} {model} {size}".strip())
    image_alt_text = f"{brand} {model} {size}".strip()
    return {
        "product_title": title,
        "seo_title": title,
        "meta_description": meta,
        "keywords": keywords,
        "image_slug": image_slug,
        "image_alt_text": image_alt_text,
    }

