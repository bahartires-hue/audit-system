from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List


def _api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def ai_enabled() -> bool:
    return bool(_api_key())


def _build_payload(report: Dict[str, Any], mismatches: List[Dict[str, Any]]) -> Dict[str, Any]:
    sample = mismatches[:20]
    compact = [
        {
            "doc": str(x.get("doc") or "")[:80],
            "amount": x.get("amount"),
            "reason": str(x.get("reason") or "")[:160],
        }
        for x in sample
    ]
    prompt = {
        "report_title": report.get("title") or "تقرير بدون عنوان",
        "branch1": report.get("branch1_name"),
        "branch2": report.get("branch2_name"),
        "stats": report.get("stats") or {},
        "mismatches_sample": compact,
        "instructions": "اكتب JSON فقط بالمفاتيح: summary, top_causes (array), actions (array). بالعربية وبشكل عملي وقصير.",
    }
    return {
        "model": "gpt-4.1-mini",
        "input": [
            {
                "role": "system",
                "content": "You are a financial reconciliation analyst. Return strict JSON only.",
            },
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
        ],
    }


def explain_report(report: Dict[str, Any], mismatches: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not ai_enabled():
        raise RuntimeError("التحليل الذكي غير مفعّل: يرجى ضبط OPENAI_API_KEY")
    body = json.dumps(_build_payload(report, mismatches), ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=body,
        headers={
            "Authorization": f"Bearer {_api_key()}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=35) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"فشل الاتصال بخدمة الذكاء الاصطناعي: {txt[:220]}")
    except Exception as e:
        raise RuntimeError(f"تعذر تشغيل التحليل الذكي: {str(e)}")
    data = json.loads(raw)
    out_text = data.get("output_text") or ""
    if not out_text:
        try:
            chunks = data.get("output") or []
            for c in chunks:
                for part in c.get("content") or []:
                    if part.get("type") == "output_text":
                        out_text += part.get("text") or ""
        except Exception:
            pass
    try:
        parsed = json.loads(out_text.strip())
    except Exception:
        parsed = {
            "summary": out_text.strip()[:500] or "تعذر تفسير ناتج التحليل الذكي",
            "top_causes": [],
            "actions": [],
        }
    return {
        "summary": str(parsed.get("summary") or "").strip(),
        "top_causes": [str(x).strip() for x in (parsed.get("top_causes") or []) if str(x).strip()][:6],
        "actions": [str(x).strip() for x in (parsed.get("actions") or []) if str(x).strip()][:6],
    }
