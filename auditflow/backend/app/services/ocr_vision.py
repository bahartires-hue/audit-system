from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict


def _api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def ocr_enabled() -> bool:
    return bool(_api_key())


def _extract_output_text(data: Dict[str, Any]) -> str:
    out_text = data.get("output_text") or ""
    if out_text:
        return out_text
    try:
        chunks = data.get("output") or []
        for c in chunks:
            for part in c.get("content") or []:
                if part.get("type") == "output_text":
                    out_text += part.get("text") or ""
    except Exception:
        pass
    return out_text


def extract_text_from_image(image_bytes: bytes, mime: str = "image/png") -> str:
    """Sends an image to OpenAI's vision-capable Responses API and returns the extracted text.

    Raises RuntimeError with an Arabic-friendly message on failure.
    """
    if not ocr_enabled():
        raise RuntimeError("خدمة استخراج النص غير مفعّلة حالياً (مفتاح الذكاء الاصطناعي غير موجود)")

    b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:{mime};base64,{b64}"

    body = json.dumps(
        {
            "model": "gpt-4.1-mini",
            "input": [
                {
                    "role": "system",
                    "content": (
                        "You are an OCR engine. Extract ALL visible text from the image exactly as written, "
                        "preserving line breaks and reading order. Return ONLY the extracted text with no "
                        "commentary, no markdown fences, and no explanations. If no text is visible, return an "
                        "empty string."
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "استخرج كل النص الموجود في هذه الصورة."},
                        {"type": "input_image", "image_url": data_url},
                    ],
                },
            ],
        },
        ensure_ascii=False,
    ).encode("utf-8")

    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=body,
        headers={"Authorization": f"Bearer {_api_key()}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"فشل الاتصال بخدمة استخراج النص: {txt[:220]}")
    except Exception as e:
        raise RuntimeError(f"تعذر تشغيل استخراج النص: {str(e)}")

    data = json.loads(raw)
    return _extract_output_text(data).strip()
