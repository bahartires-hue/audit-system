from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict

_GEMINI_MODEL = "gemini-2.5-flash-lite"
_GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{_GEMINI_MODEL}:generateContent"


def _gemini_api_key() -> str:
    return (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()


def ocr_enabled() -> bool:
    return bool(_gemini_api_key())


def _extract_gemini_text(data: Dict[str, Any]) -> str:
    try:
        candidates = data.get("candidates") or []
        if not candidates:
            return ""
        parts = (candidates[0].get("content") or {}).get("parts") or []
        out = ""
        for p in parts:
            out += p.get("text") or ""
        return out
    except Exception:
        return ""


def extract_text_from_image(image_bytes: bytes, mime: str = "image/png") -> str:
    """يرسل صورة إلى Gemini (Google، الخطة المجانية) القادر على قراءة الصور، ويعيد النص المستخرج.

    يرفع RuntimeError برسالة عربية عند الفشل.
    """
    if not ocr_enabled():
        raise RuntimeError("خدمة استخراج النص من الصور غير مفعّلة حالياً (مفتاح GEMINI_API_KEY غير موجود)")

    b64 = base64.b64encode(image_bytes).decode("ascii")

    body = json.dumps(
        {
            "system_instruction": {
                "parts": [
                    {
                        "text": (
                            "You are an OCR engine. Extract ALL visible text from the image exactly as "
                            "written, preserving line breaks and reading order. Return ONLY the extracted "
                            "text with no commentary, no markdown fences, and no explanations. If no text "
                            "is visible, return an empty string."
                        )
                    }
                ]
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": "استخرج كل النص الموجود في هذه الصورة."},
                        {"inline_data": {"mime_type": mime, "data": b64}},
                    ],
                }
            ],
            "generationConfig": {"temperature": 0},
        },
        ensure_ascii=False,
    ).encode("utf-8")

    req = urllib.request.Request(
        f"{_GEMINI_ENDPOINT}?key={_gemini_api_key()}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"فشل الاتصال بخدمة استخراج النص من الصور (Gemini): {txt[:220]}")
    except Exception as e:
        raise RuntimeError(f"تعذر تشغيل استخراج النص من الصورة: {str(e)}")

    data = json.loads(raw)
    return _extract_gemini_text(data).strip()
