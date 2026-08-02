from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict


def _anthropic_api_key() -> str:
    return (os.getenv("ANTHROPIC_API_KEY") or "").strip()


def _openai_api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def ai_text_enabled() -> bool:
    return bool(_anthropic_api_key())


def audio_transcribe_enabled() -> bool:
    return bool(_openai_api_key())


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


def generate_text(system_prompt: str, user_text: str, max_chars: int = 12000) -> str:
    """يستدعي Anthropic Messages API (Claude) برسالة نظام + نص مستخدم، ويعيد نصاً عادياً.

    يرفع RuntimeError برسالة عربية عند الفشل.
    """
    if not ai_text_enabled():
        raise RuntimeError("خدمة الذكاء الاصطناعي غير مفعّلة حالياً (مفتاح ANTHROPIC_API_KEY غير موجود)")

    body = json.dumps(
        {
            "model": "claude-sonnet-4-5",
            "max_tokens": 4096,
            "system": system_prompt,
            "messages": [{"role": "user", "content": (user_text or "")[:max_chars]}],
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
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"فشل الاتصال بخدمة الذكاء الاصطناعي: {txt[:220]}")
    except Exception as e:
        raise RuntimeError(f"تعذر تشغيل التحليل الذكي: {str(e)}")

    data = json.loads(raw)
    text = _extract_claude_text(data).strip()
    if not text:
        raise RuntimeError("لم يتم استلام أي نص من خدمة الذكاء الاصطناعي")
    return text


def transcribe_audio(content: bytes, filename: str) -> str:
    """يرفع ملف صوتي إلى OpenAI Whisper ويعيد النص المفرغ.

    ملاحظة: تبقى هذه الأداة تحديداً على OpenAI لعدم وجود بديل من Anthropic لتحويل الصوت إلى نص.
    يرفع RuntimeError برسالة عربية عند الفشل.
    """
    if not audio_transcribe_enabled():
        raise RuntimeError("خدمة تحويل الصوت إلى نص غير مفعّلة حالياً (مفتاح OPENAI_API_KEY غير موجود)")

    import requests

    try:
        resp = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {_openai_api_key()}"},
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
        raise RuntimeError("لم يتم التعرف على أي كلام في الملف الصوتي")
    return text.strip()
