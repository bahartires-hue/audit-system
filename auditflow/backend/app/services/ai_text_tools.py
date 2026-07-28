from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict


def _api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def ai_text_enabled() -> bool:
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


def generate_text(system_prompt: str, user_text: str, max_chars: int = 12000) -> str:
    """يستدعي OpenAI Responses API برسالة نظام + نص مستخدم، ويعيد نصًا عاديًا (بدون JSON).

    يرفع RuntimeError برسالة عربية عند الفشل.
    """
    if not ai_text_enabled():
        raise RuntimeError("خدمة الذكاء الاصطناعي غير مفعّلة حالياً (مفتاح OPENAI_API_KEY غير موجود)")

    body = json.dumps(
        {
            "model": "gpt-4.1-mini",
            "input": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": (user_text or "")[:max_chars]},
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
        raise RuntimeError(f"فشل الاتصال بخدمة الذكاء الاصطناعي: {txt[:220]}")
    except Exception as e:
        raise RuntimeError(f"تعذر تشغيل التحليل الذكي: {str(e)}")

    data = json.loads(raw)
    return _extract_output_text(data).strip()


def transcribe_audio(content: bytes, filename: str) -> str:
    """يرفع ملف صوتي إلى OpenAI Whisper ويعيد النص المفرّغ (بالعربية أو أي لغة أخرى).

    يرفع RuntimeError برسالة عربية عند الفشل.
    """
    if not ai_text_enabled():
        raise RuntimeError("خدمة تحويل الصوت إلى نص غير مفعّلة حالياً (مفتاح OPENAI_API_KEY غير موجود)")

    import requests

    try:
        resp = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {_api_key()}"},
            data={"model": "whisper-1"},
            files={"file": (filename or "audio.mp3", content, "application/octet-stream")},
            timeout=120,
        )
    except Exception as e:
        raise RuntimeError(f"تعذر الاتصال بخدمة تحويل الصوت إلى نص: {str(e)}")

    if resp.status_code >= 400:
        raise RuntimeError(f"فشل تحويل الصوت إلى نص: {resp.text[:220]}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError("استجابة غير متوقعة من خدمة تحويل الصوت إلى نص")

    text = (data or {}).get("text") or ""
    if not text.strip():
        raise RuntimeError("لم يتم التعرف على أي كلام داخل الملف الصوتي")
    return text.strip()
