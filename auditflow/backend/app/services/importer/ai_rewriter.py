from __future__ import annotations

from typing import Any, Dict


def rewrite_description_fallback(prod: Dict[str, Any]) -> str:
    brand = prod.get("brand", "")
    model = prod.get("model", "")
    size = prod.get("size", "")
    ls = prod.get("load_speed", "")
    return f"إطار {brand} {model} مقاس {size} تحميل {ls}. مناسب للاستخدام اليومي ويوفر ثباتًا ممتازًا."

