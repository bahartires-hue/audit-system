from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict


def _anthropic_api_key() -> str:
    return (os.getenv("ANTHROPIC_API_KEY") or "").strip()


def ocr_enabled() -> bool:
    return bool(_anthropic_api_key())


def _extract_claude_text(data: Dict[str, Any]) -> str:
    try:
        parts = data.get("content") or []
        out = ""
        for p in parts:
            if p.get("type") == "text":
                out += p.get("text") or ""
        return out
    except Exception:
        return ""


def extract_text_from_image(image_bytes: bytes, mime: str = "image/png") -> str:
    """يرسل صورة إلى Claude (Anthropic) القادر على قراءة الصور، ويعيد النص المستخرج.

    يرفع RuntimeError برسالة عربية عند الفشل.
    """
    if not ocr_enabled():
        raise RuntimeError("خدمة استخراج النص من الصور غير مفعّلة حالياً (مفتاح ANTHROPIC_API_KEY غير موجود)")

    b64 = base64.b64encode(image_bytes).decode("ascii")

    body = json.dumps(
        {
            "model": "claude-sonnet-4-5",
            "max_tokens": 4096,
            "system": (
                "You are an OCR engine. Extract ALL visible text from the image exactly as written, "
                "preserving line breaks and reading order. Return ONLY the extracted text with no "
                "commentary, no markdown fences, and no explanations. If no text is visible, return an "
                "empty string."
            ),
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "استخرج كل النص الموجود في هذه الصورة."},
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": mime, "data": b64},
                        },
                    ],
                }
            ],
        },
        ensure_ascii=False,
    ).encode("utf-8")

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": _anthropic_api_key(),
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"فشل الاتصال بخدمة استخراج النص من الصور: {txt[:220]}")
    except Exception as e:
        raise RuntimeError(f"تعذر تشغيل استخراج النص من الصورة: {str(e)}")

    data = json.loads(raw)
    return _extract_claude_text(data).strip()
