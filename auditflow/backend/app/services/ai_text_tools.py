from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict

_TEXT_MODEL = "gemini-2.5-flash-lite"
_AUDIO_MODEL = "gemini-2.5-flash"
_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

_AUDIO_MIME_BY_EXT = {
    "mp3": "audio/mp3",
    "mpeg": "audio/mpeg",
    "wav": "audio/wav",
    "aac": "audio/aac",
    "m4a": "audio/aac",
    "ogg": "audio/ogg",
    "oga": "audio/ogg",
    "flac": "audio/flac",
    "aiff": "audio/aiff",
    "webm": "audio/webm",
}


def _gemini_api_key() -> str:
    return (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()


def ai_text_enabled() -> bool:
    return bool(_gemini_api_key())


def audio_transcribe_enabled() -> bool:
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


def _call_gemini(model: str, system_prompt: str, parts: list, timeout: int = 60) -> str:
    body = json.dumps(
        {
            "system_instruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"role": "user", "parts": parts}],
        },
        ensure_ascii=False,
    ).encode("utf-8")

    req = urllib.request.Request(
        f"{_ENDPOINT.format(model=model)}?key={_gemini_api_key()}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"فشل الاتصال بخدمة الذكاء الاصطناعي (Gemini): {txt[:220]}")
    except Exception as e:
        raise RuntimeError(f"تعذر تشغيل التحليل الذكي: {str(e)}")

    data = json.loads(raw)
    return _extract_gemini_text(data).strip()


def generate_text(system_prompt: str, user_text: str, max_chars: int = 12000) -> str:
    """يستدعي Gemini (Google، الخطة المجانية) برسالة نظام + نص مستخدم، ويعيد نصاً عادياً.

    يرفع RuntimeError برسالة عربية عند الفشل.
    """
    if not ai_text_enabled():
        raise RuntimeError("خدمة الذكاء الاصطناعي غير مفعّلة حالياً (مفتاح GEMINI_API_KEY غير موجود)")

    parts = [{"text": (user_text or "")[:max_chars]}]
    text = _call_gemini(_TEXT_MODEL, system_prompt, parts, timeout=60)
    if not text:
        raise RuntimeError("لم يتم استلام أي نص من خدمة الذكاء الاصطناعي")
    return text


def _audio_mime_from_filename(filename: str) -> str:
    ext = (filename or "").rsplit(".", 1)[-1].lower() if "." in (filename or "") else ""
    return _AUDIO_MIME_BY_EXT.get(ext, "audio/mp3")


def transcribe_audio(content: bytes, filename: str) -> str:
    """يرسل ملفاً صوتياً إلى Gemini (القادر على فهم الصوت) ويعيد النص المفرغ.

    يرفع RuntimeError برسالة عربية عند الفشل.
    """
    if not audio_transcribe_enabled():
        raise RuntimeError("خدمة تحويل الصوت إلى نص غير مفعّلة حالياً (مفتاح GEMINI_API_KEY غير موجود)")

    mime = _audio_mime_from_filename(filename)
    b64 = base64.b64encode(content).decode("ascii")
    parts = [
        {"text": "فرّغ (استخرج) كل الكلام الموجود في هذا المقطع الصوتي إلى نص، بنفس اللغة المنطوقة."},
        {"inline_data": {"mime_type": mime, "data": b64}},
    ]
    system_prompt = (
        "You are a speech-to-text engine. Transcribe ALL spoken words in the audio exactly, "
        "in the same language spoken. Return ONLY the transcribed text with no commentary, "
        "no markdown fences, and no explanations. If no speech is detected, return an empty string."
    )
    text = _call_gemini(_AUDIO_MODEL, system_prompt, parts, timeout=120)
    if not text.strip():
        raise RuntimeError("لم يتم التعرف على أي كلام في الملف الصوتي")
    return text.strip()
