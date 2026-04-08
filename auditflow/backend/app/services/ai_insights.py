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


def _compact_mismatches(mismatches: List[Dict[str, Any]], limit: int = 30) -> List[Dict[str, Any]]:
    sample = mismatches[:limit]
    return [
        {
            "doc": str(x.get("doc") or "")[:80],
            "amount": x.get("amount"),
            "reason": str(x.get("reason") or "")[:160],
        }
        for x in sample
    ]


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


def _call_openai(prompt_payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(
        {
            "model": "gpt-4.1-mini",
            "input": [
                {"role": "system", "content": "You are a financial reconciliation analyst. Return strict JSON only."},
                {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False)},
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
        with urllib.request.urlopen(req, timeout=35) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"فشل الاتصال بخدمة الذكاء الاصطناعي: {txt[:220]}")
    except Exception as e:
        raise RuntimeError(f"تعذر تشغيل التحليل الذكي: {str(e)}")
    data = json.loads(raw)
    out_text = _extract_output_text(data)
    try:
        return json.loads(out_text.strip())
    except Exception:
        return {"summary": out_text.strip()[:600] or "تعذر تفسير ناتج التحليل الذكي"}


def _build_payload(report: Dict[str, Any], mismatches: List[Dict[str, Any]]) -> Dict[str, Any]:
    compact = _compact_mismatches(mismatches, limit=20)
    compact = [
        {"doc": x["doc"], "amount": x["amount"], "reason": x["reason"]}
        for x in compact
    ]
    prompt = {
        "report_title": report.get("title") or "تقرير بدون عنوان",
        "branch1": report.get("branch1_name"),
        "branch2": report.get("branch2_name"),
        "stats": report.get("stats") or {},
        "mismatches_sample": compact,
        "instructions": "اكتب JSON فقط بالمفاتيح: summary, top_causes (array), actions (array). بالعربية وبشكل عملي وقصير.",
    }
    return prompt


def explain_report(report: Dict[str, Any], mismatches: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not ai_enabled():
        raise RuntimeError("التحليل الذكي غير مفعّل: يرجى ضبط OPENAI_API_KEY")
    parsed = _call_openai(_build_payload(report, mismatches))
    return {
        "summary": str(parsed.get("summary") or "").strip(),
        "top_causes": [str(x).strip() for x in (parsed.get("top_causes") or []) if str(x).strip()][:6],
        "actions": [str(x).strip() for x in (parsed.get("actions") or []) if str(x).strip()][:6],
    }


def full_analysis(report: Dict[str, Any], mismatches: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not ai_enabled():
        raise RuntimeError("التحليل الذكي غير مفعّل: يرجى ضبط OPENAI_API_KEY")
    prompt = {
        "report_title": report.get("title") or "تقرير بدون عنوان",
        "branch1": report.get("branch1_name"),
        "branch2": report.get("branch2_name"),
        "stats": report.get("stats") or {},
        "mismatches_sample": _compact_mismatches(mismatches, limit=30),
        "instructions": (
            "اكتب JSON فقط بالمفاتيح: executive_summary, root_causes (array), risk_score (0-100), "
            "recommended_actions (array), followup_messages (array). بالعربية العملية."
        ),
    }
    parsed = _call_openai(prompt)
    try:
        risk = int(parsed.get("risk_score") or 0)
    except Exception:
        risk = 0
    risk = max(0, min(100, risk))
    return {
        "executive_summary": str(parsed.get("executive_summary") or parsed.get("summary") or "").strip(),
        "root_causes": [str(x).strip() for x in (parsed.get("root_causes") or []) if str(x).strip()][:8],
        "risk_score": risk,
        "recommended_actions": [str(x).strip() for x in (parsed.get("recommended_actions") or []) if str(x).strip()][:8],
        "followup_messages": [str(x).strip() for x in (parsed.get("followup_messages") or []) if str(x).strip()][:6],
    }
