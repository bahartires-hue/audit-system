"""
سلوك التحليل الأصلي (السكربت القديم) — مطابقة حرفية لمنطق المطابقة والاستخراج.
يُفعّل من analyzer.process / analyzer.analyze عند AUDITFLOW_LEGACY_ANALYZER=1 (الافتراضي).
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pdfplumber

# نفس خريطة المستندات في السكربت القديم (بدون إضافات لاحقة)
LEGACY_DOC_MAP: Dict[str, str] = {
    "مردود مبيعات": "مردود مشتريات",
    "مردود مشتريات": "مردود مبيعات",
    "سند قبض": "سند صرف",
    "سند صرف": "سند قبض",
    "تحويل مخزني": "تحويل مخزني",
    "توريد مخزني": "صرف مخزني",
    "صرف مخزني": "توريد مخزني",
    "قيد يومية": "قيد يومية",
    "قيد افتتاحي": "قيد افتتاحي",
    "مبيعات": "مشتريات",
    "مشتريات": "مبيعات",
}


def legacy_safe(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        v = str(v).replace(",", "").strip()
        if v == "":
            return None
        return round(float(v), 2)
    except Exception:
        return None


def legacy_detect_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    df.columns = df.columns.astype(str).str.strip()

    debit_col = None
    credit_col = None
    date_col = None

    for col in df.columns:
        name = str(col).lower()
        if any(x in name for x in ["مدين", "debit", "dr"]):
            debit_col = col
        if any(x in name for x in ["دائن", "credit", "cr"]):
            credit_col = col
        if any(x in name for x in ["تاريخ", "التاريخ", "التأريخ", "date"]):
            date_col = col

    numeric_cols: List[Tuple[Any, float]] = []
    for col in df.columns:
        nums = pd.to_numeric(df[col], errors="coerce")
        valid = nums.dropna()
        if len(valid) < len(df) * 0.3:
            continue
        mean_val = float(valid.mean())
        if mean_val < 10:
            continue
        numeric_cols.append((col, mean_val))

    numeric_cols.sort(key=lambda x: x[1], reverse=True)

    if not debit_col and len(numeric_cols) >= 1:
        debit_col = numeric_cols[0][0]
    if not credit_col and len(numeric_cols) >= 2:
        credit_col = numeric_cols[1][0]

    if not date_col:
        for col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().sum() > len(df) * 0.5:
                date_col = col
                break

    return debit_col, credit_col, date_col


def legacy_read_excel(file_path: str) -> Optional[pd.DataFrame]:
    df = pd.read_excel(file_path)
    if df is None or df.empty:
        return None
    return df.dropna(how="all")


def legacy_read_pdf(file_path: str) -> Optional[pd.DataFrame]:
    rows: List[List[Any]] = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue
            for table in tables:
                for row in table:
                    if row and any(cell is not None for cell in row):
                        rows.append(row)
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df = df.dropna(how="all")
    if len(df) < 2:
        return None
    df.columns = df.iloc[0]
    df = df[1:]
    return df.dropna(how="all")


def legacy_read_any(file_path: str, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        out = legacy_read_excel(file_path)
        if out is None:
            raise ValueError("ملف Excel فارغ أو غير مقروء")
        return out
    if name.endswith(".pdf"):
        out = legacy_read_pdf(file_path)
        if out is None:
            raise ValueError("نوع الملف غير مدعوم أو PDF بدون جدول كافٍ")
        return out
    raise ValueError("نوع الملف غير مدعوم")


def legacy_clean(s: Any) -> str:
    if not s:
        return ""
    s = str(s).lower().strip()
    for w in ["رقم", "no", "doc", "ref"]:
        s = s.replace(w, "")
    s = re.sub(r"\d+", "", s)
    for ch in [" ", "-", "_", "/", "\\", ".", ","]:
        s = s.replace(ch, "")
    return s


def legacy_match_doc(d1: Any, d2: Any) -> bool:
    if not d1 and not d2:
        return True
    if not d1 or not d2:
        return False
    d1 = legacy_clean(d1)
    d2 = legacy_clean(d2)
    if not d1 and not d2:
        return True
    if not d1 or not d2:
        return False
    if d1 == d2:
        return True
    if len(d1) > 3 and len(d2) > 3:
        if d1 in d2 or d2 in d1:
            return True
    for key, val in LEGACY_DOC_MAP.items():
        k = legacy_clean(key)
        v = legacy_clean(val)
        if (k in d1 and v in d2) or (v in d1 and k in d2):
            return True
    similarity = SequenceMatcher(None, d1, d2).ratio()
    return similarity > 0.7


def legacy_date_diff_days(d1: Any, d2: Any) -> Optional[int]:
    try:
        dd1 = pd.to_datetime(d1, errors="coerce")
        dd2 = pd.to_datetime(d2, errors="coerce")
        if pd.isna(dd1) or pd.isna(dd2):
            return None
        return abs(int((dd1 - dd2).days))
    except Exception:
        return None


def legacy_process(file_path: str, filename: str, branch: str) -> List[Dict[str, Any]]:
    df = legacy_read_any(file_path, filename)
    if df is None or len(df) == 0:
        return []

    df.columns = df.columns.astype(str).str.strip()
    debit_col, credit_col, date_col = legacy_detect_columns(df)

    doc_col = None
    for col in df.columns:
        name = str(col).lower().strip()
        if any(
            x in name
            for x in [
                "مستند",
                "المستند",
                "نوع",
                "بيان",
                "وصف",
                "description",
                "desc",
                "document",
            ]
        ):
            doc_col = col
            break

    if not debit_col and not credit_col:
        numeric_cols: List[Tuple[Any, float]] = []
        for col in df.columns:
            nums = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(nums) < len(df) * 0.3:
                continue
            if float(nums.mean()) < 10:
                continue
            numeric_cols.append((col, float(nums.mean())))
        numeric_cols.sort(key=lambda x: x[1], reverse=True)
        if len(numeric_cols) >= 1:
            debit_col = numeric_cols[0][0]
        if len(numeric_cols) >= 2:
            credit_col = numeric_cols[1][0]

    data: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        if row.isna().all():
            continue

        debit = legacy_safe(row[debit_col]) if debit_col and debit_col in df.columns else None
        credit = legacy_safe(row[credit_col]) if credit_col and credit_col in df.columns else None

        if debit is None and credit is None:
            continue

        if debit and credit and debit > 0 and credit > 0:
            amount = max(debit, credit)
            data.append(
                {
                    "amount": float(amount),
                    "type": "error",
                    "branch": branch,
                    "date": None,
                    "doc": "",
                    "reason": "خطأ: الصف يحتوي مدين ودائن",
                }
            )
            continue

        if credit and credit > 0:
            amount = credit
            t = "credit"
        elif debit and debit > 0:
            amount = debit
            t = "debit"
        else:
            continue

        date: Any = None
        if date_col and date_col in df.columns:
            try:
                val = row[date_col]
                if pd.isna(val):
                    date = None
                else:
                    d = pd.to_datetime(val, errors="coerce", dayfirst=False)
                    if not pd.isna(d):
                        date = d.strftime("%Y-%m-%d")
                    else:
                        date = None
            except Exception:
                date = str(row[date_col])

        doc: Optional[str] = None
        if doc_col and doc_col in df.columns:
            val = row[doc_col]
            if pd.notna(val):
                doc = str(val).strip()

        data.append(
            {
                "amount": float(amount),
                "type": t,
                "branch": branch,
                "date": date,
                "doc": doc,
            }
        )

    return data


def legacy_analyze(d1: List[Dict[str, Any]], d2: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    res: List[Dict[str, Any]] = []
    used = [False] * len(d2)
    counts: Dict[str, int] = {}

    def remove_reversals(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned: List[Dict[str, Any]] = []
        used_local = [False] * len(data)
        for i, x1 in enumerate(data):
            if used_local[i]:
                continue
            found = False
            for j, x2 in enumerate(data):
                if i == j or used_local[j]:
                    continue
                if x1["branch"] != x2["branch"]:
                    continue
                if x1["type"] == x2["type"]:
                    continue
                if abs(x1["amount"] - x2["amount"]) > 0.01:
                    continue
                days = legacy_date_diff_days(x1["date"], x2["date"])
                if days is None or days > 1:
                    continue
                if x1.get("doc") and x2.get("doc"):
                    if not legacy_match_doc(x1["doc"], x2["doc"]):
                        continue
                used_local[i] = True
                used_local[j] = True
                found = True
                break
            if not found:
                cleaned.append(x1)
        return cleaned

    d1 = remove_reversals(d1)
    d2 = remove_reversals(d2)

    if not d2:
        for x in d1:
            res.append({**x, "reason": "لا يوجد مقابل ❌ (الفرع الثاني فارغ)"})
            b = x.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
        return res, counts

    def match_score(x1: Dict[str, Any], x2: Dict[str, Any]) -> Tuple[int, List[str]]:
        score = 0
        reasons: List[str] = []

        diff = abs(x1["amount"] - x2["amount"])
        if diff < 0.01:
            score += 50
            reasons.append("نفس المبلغ")
        elif diff < 1:
            score += 30
            reasons.append("مبلغ قريب")
        else:
            return 0, ["فرق مبلغ كبير"]

        if (x1["type"] == "credit" and x2["type"] == "debit") or (
            x1["type"] == "debit" and x2["type"] == "credit"
        ):
            score += 30
            reasons.append("اتجاه عكسي صحيح")
        else:
            return 0, ["نفس الاتجاه"]

        if x1.get("doc") and x2.get("doc"):
            if not legacy_match_doc(x1["doc"], x2["doc"]):
                return 0, ["اختلاف نوع المستند"]

        days = legacy_date_diff_days(x1["date"], x2["date"])
        if days is None:
            score -= 10
            reasons.append("تاريخ غير واضح")
        elif days == 0:
            score += 20
            reasons.append("نفس اليوم")
        elif days <= 2:
            score += 10
            reasons.append("تاريخ قريب")
        else:
            score -= 10
            reasons.append("تاريخ بعيد")

        if legacy_match_doc(x1.get("doc"), x2.get("doc")):
            score += 20
            reasons.append("نوع مستند مطابق")
        else:
            score -= 10
            reasons.append("اختلاف نوع المستند")

        return score, reasons

    for x1 in d1:
        if x1.get("type") == "error":
            res.append(x1)
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
            continue

        best_i = -1
        best_score = -1
        best_reason: List[str] = []

        for i, x2 in enumerate(d2):
            if used[i]:
                continue
            if x2.get("type") == "error":
                continue
            score, reasons = match_score(x1, x2)
            if score > best_score:
                best_score = score
                best_i = i
                best_reason = reasons

        if best_score >= 80 and best_i != -1:
            used[best_i] = True
        elif best_score >= 60 and best_i != -1:
            res.append(
                {
                    **x1,
                    "reason": f"تطابق ضعيف ⚠️ | score={best_score} | {' , '.join(best_reason)}",
                }
            )
            used[best_i] = True
        else:
            res.append(
                {
                    **x1,
                    "reason": f"لا يوجد مقابل ❌ | score={best_score} | {' , '.join(best_reason)}",
                }
            )
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1

    for i, x in enumerate(d2):
        if not used[i]:
            if x.get("type") == "error":
                res.append(x)
                b = x.get("branch") or "unknown"
                counts[b] = counts.get(b, 0) + 1
                continue
            res.append({**x, "reason": "لا يوجد مقابل ❌ (من الفرع الآخر)"})
            b = x.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1

    return res, counts
