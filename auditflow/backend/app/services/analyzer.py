from __future__ import annotations

import math
import numbers
import os
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pdfplumber

from .legacy_analyzer import legacy_analyze, legacy_process


def _use_legacy_analyzer() -> bool:
    """السلوك القديم للتحليل (مطابقة حرفية). عطّله بـ AUDITFLOW_LEGACY_ANALYZER=0."""
    return os.environ.get("AUDITFLOW_LEGACY_ANALYZER", "1").lower() in ("1", "true", "yes")


def _is_plausible_currency_amount(x: float) -> bool:
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return False
    if pd.isna(xf) or xf != xf:
        return False
    ax = abs(xf)
    if ax < 1e-9:
        return True
    if ax >= 1e11:
        return False
    if float(x) == int(x) and ax >= 1e6:
        nd = len(str(int(ax)))
        if nd >= 12:
            return False
        if nd >= 10 and ax >= 1e9:
            return False
    return True


def _normalize_doc_text(s: Any) -> str:
    t = unicodedata.normalize("NFKC", str(s or ""))
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _arabic_letters_for_match(s: str) -> str:
    t = unicodedata.normalize("NFKC", s or "")
    for a, b in (
        ("ى", "ي"),
        ("ی", "ي"),
        ("ئ", "ي"),
        ("ة", "ه"),
        ("ۀ", "ه"),
        ("ٱ", "ا"),
        ("أ", "ا"),
        ("إ", "ا"),
        ("آ", "ا"),
    ):
        t = t.replace(a, b)
    return t.lower()


_DOC_KIND_SPECS: List[Tuple[str, str]] = [
    ("مردود مشتريات", "مردود مشتريات"),
    ("مردود مبيعات", "مردود مبيعات"),
    # كلمات شائعة بخطأ اتجاه/عكس حروف في مستخرج PDF
    ("هروتاف", "فاتورة"),
    ("تاعیبم", "مبيعات"),
    ("تاعيبم", "مبيعات"),
    ("تایرتشم", "مشتريات"),
    ("تارتشم", "مشتريات"),
    ("تايرتشم", "مشتريات"),
    ("مشتريات", "مشتريات"),
    ("مبیعات", "مبيعات"),
    ("مبيعات", "مبيعات"),
    ("فاتوره", "فاتورة"),
    ("فاتورة مبيعات", "فاتورة مبيعات"),
    ("فاتورة مشتريات", "فاتورة مشتريات"),
    ("فاتورة", "فاتورة"),
    ("سند قبض", "سند قبض"),
    ("سند صرف", "سند صرف"),
    ("قيد يومية", "قيد يومية"),
    ("طلب شراء", "مشتريات"),
    ("طلب بيع", "مبيعات"),
]


def _has_arabic(s: str) -> bool:
    return any("\u0600" <= c <= "\u06ff" for c in (s or ""))


def _expand_text_for_doc_kind(s: str) -> str:
    t = _normalize_doc_text(s)
    if not t:
        return ""
    parts = [t]
    words = t.split()
    rev = []
    for w in words:
        if _has_arabic(w) and len(w) >= 3:
            rev.append(w[::-1])
        else:
            rev.append(w)
    parts.append(" ".join(rev))
    return " \n ".join(parts)


def infer_document_kind_from_narrative(text: Optional[str]) -> Optional[str]:
    if not text or not str(text).strip():
        return None
    hay_raw = _expand_text_for_doc_kind(str(text))
    hay = _arabic_letters_for_match(hay_raw)
    if "هروتاف" in hay and "تاعيبم" in hay:
        return "فاتورة مبيعات"
    if "هروتاف" in hay and "تايرتشم" in hay:
        return "فاتورة مشتريات"
    for needle, label in _DOC_KIND_SPECS:
        n = _arabic_letters_for_match(needle)
        if n in hay or needle in hay_raw:
            return label
    return None


def _enrich_doc_field(doc_out: Optional[str], narrative: str) -> Optional[str]:
    kind = infer_document_kind_from_narrative(narrative) or infer_document_kind_from_narrative(doc_out or "")
    if kind:
        return kind
    return doc_out


def _finalize_doc_for_row(doc_out: Optional[str], narrative: str) -> Optional[str]:
    """إن بقي المستند نصاً طويلاً مشوّهاً نستنتج نوع الحركة من كلمات مبيعات/مشتريات داخل البيان."""
    doc_out = _enrich_doc_field(doc_out, narrative)
    if doc_out and len(str(doc_out)) > 52:
        kind = infer_document_kind_from_narrative(narrative) or infer_document_kind_from_narrative(str(doc_out))
        if kind:
            return kind
        h = _arabic_letters_for_match(_expand_text_for_doc_kind(f"{narrative or ''} {doc_out}"))
        if "مشتريات" in h:
            return "فاتورة مشتريات" if "هروتاف" in h else "مشتريات"
        if "مبيعات" in h:
            return "فاتورة مبيعات" if "هروتاف" in h else "مبيعات"
    return doc_out


def _dedupe_extracted_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    order: List[Tuple[Any, ...]] = []
    by_key: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    def score_doc(d: Any) -> int:
        s = str(d or "")
        if not s:
            return 0
        if s in (
            "مبيعات",
            "مشتريات",
            "مردود مبيعات",
            "مردود مشتريات",
            "فاتورة",
            "فاتورة مبيعات",
            "فاتورة مشتريات",
            "سند قبض",
            "سند صرف",
            "قيد يومية",
        ) or s.startswith("فاتورة "):
            return 100
        return max(0, 80 - len(s))

    for r in rows:
        k = (
            r.get("branch"),
            round(float(r["amount"]), 2),
            r.get("type"),
            r.get("date") or "",
        )
        if k not in by_key:
            by_key[k] = r
            order.append(k)
        elif score_doc(r.get("doc")) > score_doc(by_key[k].get("doc")):
            by_key[k] = r

    return [by_key[k] for k in order]


def _parse_currency_numbers_from_narrative(narrative: str) -> List[float]:
    s = _normalize_arabic_digits(narrative or "")
    tokens = re.findall(r"[-+]?\d[\d,٬٫\.]*", s)
    vals: List[float] = []
    for tok in tokens:
        v = _parse_number_token(tok)
        if v is None or not _is_plausible_currency_amount(v):
            continue
        vals.append(float(v))
    if len(vals) >= 2:
        vals = [v for v in vals if not (v == int(v) and (1 <= abs(v) <= 31 or 1900 <= abs(v) <= 2100))] or vals
    return vals


def _looks_like_serial_voucher_amount(x: float) -> bool:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return False
    xf = float(x)
    if abs(xf - int(xf)) > 0.0001:
        return False
    ax = abs(xf)
    if 1900 <= ax <= 2100:
        return False
    return 100 <= ax <= 49999


def _replace_voucher_with_ledger_from_narrative(column_amount: float, narrative: str) -> float:
    narrative = _normalize_doc_text(narrative)
    if not narrative:
        return column_amount
    vals = _parse_currency_numbers_from_narrative(narrative)
    if not vals:
        return column_amount
    decimals = [v for v in vals if abs(v - int(v)) > 0.0001 and abs(v) >= 0.0001]
    for d in decimals:
        if abs(d - column_amount) < 0.02:
            return column_amount
    if _looks_like_serial_voucher_amount(column_amount) and decimals:
        return round(decimals[0], 2)
    for v in vals:
        if abs(v - column_amount) < 0.02:
            return column_amount
    if decimals:
        return round(decimals[0], 2)
    return column_amount


def _extract_best_amount_from_text(s: str) -> Optional[float]:
    vals = _parse_currency_numbers_from_narrative(s or "")
    if not vals:
        return None
    non_zero = [v for v in vals if abs(v) >= 0.0001]
    if not non_zero:
        return 0.0
    decimals = [v for v in non_zero if abs(v - int(v)) > 0.0001]
    if decimals:
        return round(decimals[-1], 2)
    return round(non_zero[-1], 2)


def safe(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            if isinstance(v, float) and pd.isna(v):
                return None
            x = float(v)
            return round(x, 2) if _is_plausible_currency_amount(x) else None
        s = str(v).replace(",", "").replace("٬", "").strip()
        if not s or s.lower() in ("nan", "none", "-"):
            return None
        s = re.sub(r"(?i)\b(debit|credit|مدين|دائن|dr|cr)\b", "", s)
        s = s.strip()
        if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", s):
            x = float(s)
        else:
            x = _extract_best_amount_from_text(s)
            if x is None:
                return None
        if not _is_plausible_currency_amount(x):
            return None
        return round(x, 2)
    except Exception:
        return None


def _column_name_excludes_from_amount(col: Any) -> bool:
    name = str(col).lower().strip()
    if not name or name.isdigit():
        return False
    if name in ("السند", "سند"):
        return True
    block = (
        "رقم السند",
        "رقم سند",
        "الرصيد",
        "رصيد",
        "balance",
        "تسلسل",
        "مسلسل",
        "seq",
        "serial",
        "ضريبي",
        "هوية",
        "الرقم الضريبي",
        "السجل التجاري",
    )
    for b in block:
        if b in name:
            return True
    if name in ("#", "م", "no", "no."):
        return True
    return False


def _column_values_look_like_ids(series: pd.Series) -> bool:
    nums = pd.to_numeric(series, errors="coerce").dropna()
    if len(nums) == 0:
        return False
    mx = float(nums.max())
    if mx >= 1e11:
        return True
    if mx == int(mx) and mx >= 1e9:
        return len(str(int(mx))) >= 10
    return False


def _is_balance_column_name(col: Any) -> bool:
    n = str(col).lower()
    return "رصيد" in n or "balance" in n


def _currency_amount_rank(series: pd.Series) -> float:
    nums = pd.to_numeric(series, errors="coerce").dropna()
    if len(nums) == 0:
        return float("-inf")
    mean_v = float(nums.mean())
    max_v = float(nums.max())
    frac_decimal = ((nums % 1).abs() > 0.001).sum() / len(nums)
    score = mean_v + frac_decimal * min(500_000.0, mean_v * 2.0 + 1.0)
    if frac_decimal < 0.08 and max_v <= 99_999_999:
        score -= min(400_000.0, mean_v * 1.5 + 100_000.0)
    return score


def _column_header_indicates_debit(col: Any) -> bool:
    name = str(col).lower().strip()
    if _column_name_excludes_from_amount(col):
        return False
    if "دائن" in name or "credit" in name:
        return False
    if "مدين" in name or "debit" in name:
        return True
    if re.fullmatch(r"dr|d\.r\.?", name) or re.search(r"(^|[^a-z])dr([^a-z]|$)", name):
        return True
    return False


def _column_header_indicates_credit(col: Any) -> bool:
    name = str(col).lower().strip()
    if _column_name_excludes_from_amount(col):
        return False
    if "مدين" in name or "debit" in name:
        return False
    if "دائن" in name or "credit" in name:
        return True
    if re.fullmatch(r"cr\.?", name):
        return True
    return False


def _promote_ledger_header_row(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    dfc = df.copy()
    dfc.columns = dfc.columns.astype(str).str.strip()
    hdr = " ".join(str(c).lower() for c in dfc.columns)
    if "مدين" in hdr and "دائن" in hdr:
        return dfc
    ncols = len(dfc.columns)
    for idx in range(min(25, len(dfc))):
        cells_raw = dfc.iloc[idx].tolist()
        cell_texts: List[str] = []
        for v in cells_raw:
            if pd.isna(v):
                continue
            t = str(v).strip().lower()
            if t:
                cell_texts.append(t)
        if len(cell_texts) < 2:
            continue
        has_deb = any("مدين" in c and "دائن" not in c for c in cell_texts)
        has_cred = any("دائن" in c and "مدين" not in c for c in cell_texts)
        if not (has_deb and has_cred):
            continue
        new_cols: List[str] = []
        for j in range(ncols):
            v = cells_raw[j] if j < len(cells_raw) else None
            if v is None or (isinstance(v, float) and pd.isna(v)) or not str(v).strip():
                new_cols.append(f"_c{j}")
            else:
                new_cols.append(str(v).strip())
        dfc.columns = new_cols
        dfc = dfc.iloc[idx + 1 :].dropna(how="all").reset_index(drop=True)
        return dfc

    return dfc


def _detect_debit_credit_date_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    df.columns = df.columns.astype(str).str.strip()

    debit_col = None
    credit_col = None
    date_col = None

    for col in df.columns:
        name = str(col).lower()
        if date_col is None and any(x in name for x in ["تاريخ", "التاريخ", "التأريخ", "date"]):
            date_col = col
            continue
        if debit_col is None and _column_header_indicates_debit(col):
            debit_col = col
            continue
        if credit_col is None and _column_header_indicates_credit(col):
            credit_col = col

    if debit_col and not credit_col:
        for col in df.columns:
            if col == debit_col or _column_name_excludes_from_amount(col):
                continue
            if _column_header_indicates_credit(col):
                credit_col = col
                break
    if credit_col and not debit_col:
        for col in df.columns:
            if col == credit_col or _column_name_excludes_from_amount(col):
                continue
            if _column_header_indicates_debit(col):
                debit_col = col
                break

    named_debit = debit_col is not None
    named_credit = credit_col is not None

    numeric_cols: List[Tuple[str, float, float]] = []
    for col in df.columns:
        if _column_name_excludes_from_amount(col):
            continue
        if _is_balance_column_name(col):
            continue
        nums = pd.to_numeric(df[col], errors="coerce")
        valid = nums.dropna()
        frac = 0.3 if len(df) >= 12 else 0.15
        need = max(1, int(len(df) * frac + 0.5))
        if len(df) >= 4:
            need = max(2, need)
        if len(valid) < need:
            continue
        if _column_values_look_like_ids(df[col]):
            continue
        mean_val = valid.mean()
        max_val = float(valid.max())
        if mean_val < 10 and max_val < 10:
            continue
        if not _is_plausible_currency_amount(max_val):
            continue
        numeric_cols.append((col, float(mean_val), _currency_amount_rank(df[col])))

    ranked_by_score = sorted(numeric_cols, key=lambda x: x[2], reverse=True)

    if not named_debit and not named_credit:
        ordered = sorted(
            numeric_cols,
            key=lambda t: list(df.columns).index(t[0]),
        )
        r_tail = max(ordered[-2][2], ordered[-1][2]) if len(ordered) >= 2 else (ordered[0][2] if ordered else 0.0)
        while len(ordered) > 2 and r_tail > 0 and ordered[0][2] < r_tail * 0.2:
            ordered.pop(0)
        if len(ordered) >= 2:
            debit_col, credit_col = ordered[-2][0], ordered[-1][0]
        elif len(ordered) == 1:
            debit_col = ordered[0][0]
    else:
        if not debit_col and len(ranked_by_score) >= 1:
            for col, _, _ in ranked_by_score:
                if col != credit_col:
                    debit_col = col
                    break
        if not credit_col and len(ranked_by_score) >= 1:
            for col, _, _ in ranked_by_score:
                if col != debit_col:
                    credit_col = col
                    break

    if not date_col:
        for col in df.columns:
            parsed = _series_to_datetimes_for_detection(df[col])
            if parsed.notna().sum() > len(df) * 0.5:
                date_col = col
                break

    return debit_col, credit_col, date_col


def _header_norm(value: Any) -> str:
    s = unicodedata.normalize("NFKC", str(value or "")).lower().strip()
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s)
    return s


def _name_has_any(col: Any, needles: List[str]) -> bool:
    n = _header_norm(col)
    return any(k in n for k in needles)


def _text_keyword_ratio(series: pd.Series, keywords: List[str]) -> float:
    vals = [str(v).strip().lower() for v in series.tolist() if pd.notna(v) and str(v).strip()]
    if not vals:
        return 0.0
    hit = 0
    for v in vals:
        if any(k in v for k in keywords):
            hit += 1
    return float(hit) / float(len(vals))


def _doc_number_content_score(series: pd.Series) -> float:
    vals = [str(v).strip() for v in series.tolist() if pd.notna(v) and str(v).strip()]
    if len(vals) < 3:
        return 0.0
    pure_num = 0
    dec_like = 0
    for v in vals:
        vn = _normalize_arabic_digits(v).replace(",", "").replace("٬", "")
        if re.fullmatch(r"\d{2,20}", vn):
            pure_num += 1
        if re.fullmatch(r"\d+\.\d+", vn):
            dec_like += 1
    unique_ratio = float(len(set(vals))) / float(len(vals))
    pure_ratio = float(pure_num) / float(len(vals))
    dec_ratio = float(dec_like) / float(len(vals))
    return max(0.0, pure_ratio * 0.7 + unique_ratio * 0.5 - dec_ratio * 0.7)


def _extract_doc_number_text(value: Any) -> str:
    s = _normalize_arabic_digits(str(value or ""))
    nums = re.findall(r"\b\d{4,}\b", s)
    if not nums:
        nums = re.findall(r"\b\d{3,}\b", s)
    filtered = []
    for n in nums:
        if len(n) == 4 and n.startswith(("19", "20")):
            continue
        filtered.append(n)
    cand = filtered or nums
    if not cand:
        return ""
    cand.sort(key=lambda x: (len(x), x))
    return cand[-1]


def detect_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """
    اكتشاف ذكي للأعمدة المحاسبية دون الاعتماد على ترتيب الأعمدة.
    """
    if df is None or df.empty:
        return {
            "date": None,
            "debit": None,
            "credit": None,
            "doc_number": None,
            "doc_type": None,
            "description": None,
            "balance": None,
        }
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()

    debit_col, credit_col, date_col = _detect_debit_credit_date_columns(df)

    balance_col: Optional[str] = None
    for col in df.columns:
        if _name_has_any(col, ["الرصيد", "رصيد", "balance", "running balance"]):
            balance_col = col
            break

    doc_number_col: Optional[str] = None
    doc_num_names = [
        "رقم المستند",
        "رقم السند",
        "رقم سند",
        "مرجع",
        "reference",
        "ref",
        "doc no",
        "document no",
        "voucher",
    ]
    for col in df.columns:
        if _name_has_any(col, doc_num_names):
            doc_number_col = col
            break
    if doc_number_col is None:
        best_col = None
        best_score = 0.0
        for col in df.columns:
            if col in (debit_col, credit_col, date_col, balance_col):
                continue
            score = _doc_number_content_score(df[col])
            if score > best_score:
                best_col = col
                best_score = score
        if best_score >= 0.75:
            doc_number_col = best_col

    doc_type_col: Optional[str] = None
    doc_type_names = ["نوع المستند", "المستند", "doc type", "document type", "type"]
    doc_type_keywords = ["فاتورة", "سند", "قبض", "صرف", "مردود", "قيد", "invoice", "voucher", "receipt", "payment"]
    for col in df.columns:
        if col == doc_number_col:
            continue
        if _name_has_any(col, doc_type_names):
            doc_type_col = col
            break
    if doc_type_col is None:
        best_col = None
        best_ratio = 0.0
        for col in df.columns:
            if col in (debit_col, credit_col, date_col, balance_col, doc_number_col):
                continue
            ratio = _text_keyword_ratio(df[col], doc_type_keywords)
            if ratio > best_ratio:
                best_col = col
                best_ratio = ratio
        if best_ratio >= 0.18:
            doc_type_col = best_col

    description_col: Optional[str] = None
    desc_names = ["البيان", "الوصف", "description", "desc", "details", "الشرح"]
    for col in df.columns:
        if _name_has_any(col, desc_names):
            description_col = col
            break
    if description_col is None:
        best_col = None
        best_len = 0.0
        for col in df.columns:
            if col in (debit_col, credit_col, date_col, balance_col, doc_number_col, doc_type_col):
                continue
            vals = [str(v).strip() for v in df[col].tolist() if pd.notna(v) and str(v).strip()]
            if not vals:
                continue
            avg_len = sum(len(v) for v in vals) / len(vals)
            if avg_len > best_len:
                best_len = avg_len
                best_col = col
        if best_len >= 8:
            description_col = best_col

    return {
        "date": date_col,
        "debit": debit_col,
        "credit": credit_col,
        "doc_number": doc_number_col,
        "doc_type": doc_type_col,
        "description": description_col,
        "balance": balance_col,
    }


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cols = detect_columns(df)
    out = pd.DataFrame(index=df.index.copy())
    out["date"] = (
        df[cols["date"]].map(_parse_datetime_cell_to_iso) if cols["date"] in df.columns else ""
    )
    out["debit"] = df[cols["debit"]].map(safe) if cols["debit"] in df.columns else ""
    out["credit"] = df[cols["credit"]].map(safe) if cols["credit"] in df.columns else ""
    out["doc_number"] = (
        df[cols["doc_number"]].map(lambda v: str(v).strip() if pd.notna(v) else "")
        if cols["doc_number"] in df.columns
        else ""
    )
    out["doc_type"] = (
        df[cols["doc_type"]].map(lambda v: str(v).strip() if pd.notna(v) else "")
        if cols["doc_type"] in df.columns
        else ""
    )
    out["description"] = (
        df[cols["description"]].map(lambda v: str(v).strip() if pd.notna(v) else "")
        if cols["description"] in df.columns
        else ""
    )
    out["balance"] = df[cols["balance"]].map(safe) if cols["balance"] in df.columns else ""

    # تحسين التوحيد عند غياب أعمدة صريحة في الملف المصدر.
    if (out["doc_number"] == "").all() and "description" in out.columns:
        out["doc_number"] = out["description"].map(_extract_doc_number_text)
    if (out["doc_type"] == "").all() and "description" in out.columns:
        out["doc_type"] = out["description"].map(lambda s: infer_document_kind_from_narrative(str(s or "")) or "")

    return out[["date", "debit", "credit", "doc_number", "doc_type", "description", "balance"]]


def detect_document_type_column(df: pd.DataFrame) -> Optional[str]:
    return detect_columns(df).get("doc_type")


def resolve_document_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    primary = detect_document_type_column(df)
    fallback: Optional[str] = None
    if primary is None:
        for col in df.columns:
            n = str(col).strip().lower()
            if "رقم السند" in n or "رقم سند" in n or n in ("#", "م"):
                continue
            if "بيان" in n and "ضريبي" not in n:
                fallback = col
                break
    return primary, fallback


def _series_to_datetimes_for_detection(series: pd.Series) -> pd.Series:
    """
    Excel غالباً يخزّن التاريخ كرقم تسلسلي؛ pd.to_datetime(45321) يعطي 1970 بشكل خاطئ.
    إن غلبت القيم في المدى النموذجي للتسلسل، نحوّل عبر أصل Excel.
    """
    num = pd.to_numeric(series, errors="coerce")
    valid_n = num.notna().sum()
    if valid_n == 0:
        return pd.to_datetime(series, errors="coerce")
    in_serial_band = num.between(39500.0, 56500.0, inclusive="both")
    if float(in_serial_band.sum()) >= max(2.0, valid_n * 0.45):
        converted = pd.to_datetime(num, unit="D", origin="1899-12-30", errors="coerce")
        if converted.notna().sum() >= max(1, int(len(series) * 0.35)):
            return converted
    return pd.to_datetime(series, errors="coerce")


def _parsed_timestamp_or_nat(val: Any) -> Any:
    """Parse Excel/PDF date cells and Arabic digits; prefer day-first (GCC). Handles Excel serials."""
    if val is None:
        return pd.NaT
    if isinstance(val, bool):
        return pd.NaT

    if isinstance(val, str):
        t = val.strip()
        if not t or t.lower() in ("nat", "none") or t in ("-", "—", "–"):
            return pd.NaT
        s = _normalize_arabic_digits(t)
        if not s or s in ("-", "—", "–"):
            return pd.NaT
        for dayfirst in (True, False):
            v = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)
            if pd.notna(v):
                return v
        return pd.NaT

    xf: Optional[float] = None
    try:
        if isinstance(val, numbers.Real):
            xf = float(val)
    except (TypeError, ValueError, OverflowError):
        xf = None
    if xf is not None:
        if math.isnan(xf) or math.isinf(xf):
            return pd.NaT
        if 39500.0 <= xf <= 56500.0:
            try:
                return pd.to_datetime(xf, unit="D", origin="1899-12-30")
            except Exception:
                pass
        ts_num = pd.to_datetime(val, errors="coerce")
        if pd.notna(ts_num):
            if ts_num.year <= 1971 and xf >= 10000.0:
                try:
                    ex = pd.to_datetime(xf, unit="D", origin="1899-12-30")
                    if pd.notna(ex):
                        return ex
                except Exception:
                    pass
            return ts_num

    if hasattr(val, "strftime") and not isinstance(val, str):
        try:
            v = pd.to_datetime(val, errors="coerce")
            if pd.notna(v):
                return v
        except Exception:
            pass

    s = _normalize_arabic_digits(str(val).strip())
    if not s or s in ("-", "—", "–"):
        return pd.NaT
    for dayfirst in (True, False):
        v = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)
        if pd.notna(v):
            return v
    return pd.NaT


def _parse_datetime_cell_to_iso(val: Any) -> Optional[str]:
    ts = _parsed_timestamp_or_nat(val)
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _iso_date_from_narrative(narrative: str) -> Optional[str]:
    if not narrative:
        return None
    line = _normalize_arabic_digits(str(narrative))
    date_pat = re.compile(
        r"(\d{4}\s*[/\-\.]\s*\d{1,2}\s*[/\-\.]\s*\d{1,2}|\d{1,2}\s*[/\-\.]\s*\d{1,2}\s*[/\-\.]\s*\d{2,4})"
    )
    m = date_pat.search(line)
    if not m:
        return None
    tok = m.group(1).strip()
    for dayfirst in (True, False):
        d = pd.to_datetime(tok, errors="coerce", dayfirst=dayfirst)
        if pd.notna(d):
            return d.strftime("%Y-%m-%d")
    return None


def extract_row_date_doc(
    row: pd.Series,
    df: pd.DataFrame,
    date_col: Optional[str],
    doc_col: Optional[str],
    doc_fallback_col: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    date_out: Optional[str] = None
    if date_col and date_col in df.columns:
        date_out = _parse_datetime_cell_to_iso(row.get(date_col))

    doc_out: Optional[str] = None
    for col in (doc_col, doc_fallback_col):
        if not col or col not in df.columns:
            continue
        val = row[col]
        if pd.isna(val):
            continue
        raw = _normalize_doc_text(val)
        if not raw:
            continue
        if _is_voucher_number_string(raw):
            continue
        doc_out = raw if len(raw) <= 200 else (raw[:200] + "...")
        break

    return date_out, doc_out


def _skip_likely_pdf_line_index_row(
    amount: float, debit: Optional[float], credit: Optional[float], both_positive: bool
) -> bool:
    # Do not drop tiny amounts (e.g., 1 SAR invoices).
    return False


def _row_narrative_for_amounts(
    row: pd.Series,
    df: pd.DataFrame,
    doc_col: Optional[str],
    doc_fb: Optional[str],
    debit_col: Optional[str],
    credit_col: Optional[str],
    date_col: Optional[str],
) -> str:
    seen: set[Any] = set()
    parts: List[str] = []
    priority: List[Any] = []
    if doc_fb and doc_fb in df.columns:
        priority.append(doc_fb)
    if doc_col and doc_col in df.columns:
        priority.append(doc_col)
    for col in priority:
        if col in seen:
            continue
        seen.add(col)
        v = row.get(col)
        if pd.notna(v):
            t = str(v).strip()
            if t:
                parts.append(t)
    for col in df.columns:
        if col in (debit_col, credit_col, date_col) or col in seen:
            continue
        n = str(col).lower()
        if "ضريبي" in n and "بيان" not in n:
            continue
        if "بيان" in n or "مستند" in n or "نوع" in n or "فاتورة" in n or "وصف" in n or "تفاصيل" in n:
            seen.add(col)
            v = row.get(col)
            if pd.notna(v):
                t = str(v).strip()
                if t:
                    parts.append(t)
    return _normalize_doc_text(" ".join(parts))


def _side_hint_from_narrative(narrative: str) -> Optional[str]:
    hay = _arabic_letters_for_match(_expand_text_for_doc_kind(narrative or ""))
    has_debit = "مدين" in hay
    has_credit = ("دائن" in hay) or ("داين" in hay)
    if has_debit and not has_credit:
        return "debit"
    if has_credit and not has_debit:
        return "credit"
    return None


def _is_likely_pdf_extraction_noise(
    amount: float,
    t: str,
    doc: Optional[str],
    narrative: str,
) -> bool:
    """
    شظايا شائعة من PDF: تذييل صفحة، أو رقم تسلسل صف مع نص «ريال» دون نوع مستند.
    لا يُستخدم لاستبعاد فواتير صغيرة صحيحة إذا وُجد نوع مستند معرّف.
    """
    blob = f"{narrative} {doc or ''}"
    if any(m in blob for m in _PDF_FOOTER_MARKERS):
        return True
    if "تطوير" in blob and "موقع" in blob:
        return True
    kind = infer_document_kind_from_narrative(narrative) or infer_document_kind_from_narrative(doc or "")
    if kind is not None:
        return False
    hay_blob = _arabic_letters_for_match(blob)
    if t == "debit" and amount == int(amount) and 1 <= abs(amount) <= 20:
        if "يدوعس" in blob or "يدوعس" in hay_blob or "سعودي" in hay_blob:
            if len(_normalize_doc_text(narrative)) <= 100:
                return True
    exp = _expand_text_for_doc_kind(blob)
    if "ریال" in exp or "ريال" in exp or "ریال" in blob or "ريال" in blob:
        if t == "debit" and amount == int(amount) and 1 <= abs(amount) <= 24:
            if len(_normalize_doc_text(narrative)) <= 140:
                return True
    return False


def _assign_global_matches(
    d1: List[Dict[str, Any]],
    d2: List[Dict[str, Any]],
    match_score_fn: Any,
    min_assign: int = 40,
) -> Dict[int, Tuple[int, int, List[str]]]:
    """أفضل مطابقة عامة: نرتّب النقاط ثم نفضّل أقل فرق مبلغ عند التساوي."""
    triples: List[Tuple[int, float, int, int, List[str]]] = []
    for i, x1 in enumerate(d1):
        if x1.get("type") == "error":
            continue
        for j, x2 in enumerate(d2):
            if x2.get("type") == "error":
                continue
            score, reasons = match_score_fn(x1, x2)
            if score < min_assign:
                continue
            ad = abs(float(x1["amount"]) - float(x2["amount"]))
            triples.append((score, ad, i, j, reasons))
    triples.sort(key=lambda t: (-t[0], t[1]))
    used_i: set[int] = set()
    used_j: set[int] = set()
    out: Dict[int, Tuple[int, int, List[str]]] = {}
    for score, _ad, i, j, reasons in triples:
        if i in used_i or j in used_j:
            continue
        used_i.add(i)
        used_j.add(j)
        out[i] = (j, score, reasons)
    return out


def read_excel(file_path: str) -> Optional[pd.DataFrame]:
    df = pd.read_excel(file_path)
    if df is None or df.empty:
        return None
    df = df.dropna(how="all")
    return df


def _normalize_arabic_digits(text: str) -> str:
    trans = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    return text.translate(trans)


def _parse_number_token(token: str) -> Optional[float]:
    t = _normalize_arabic_digits(str(token))
    t = t.replace("٬", "").replace(",", "")
    t = t.replace("٫", ".")
    t = re.sub(r"[^\d\.\-+]", "", t)
    if not t:
        return None
    try:
        return float(t)
    except Exception:
        return None


def _debit_credit_from_tail_numbers(numbers: List[str]) -> Tuple[Optional[float], Optional[float]]:
    if not numbers:
        return None, None
    vals: List[Optional[float]] = [_parse_number_token(n) for n in numbers]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None
    while len(vals) >= 4 and vals[0] is not None:
        v0 = vals[0]
        if v0 == int(v0) and 1 <= abs(v0) <= 999:
            vals = vals[1:]
            continue
        if (
            v0 == int(v0)
            and 1_000 <= abs(v0) <= 9_999_999
            and vals[1] is not None
            and abs(vals[1]) > abs(v0) * 3
        ):
            vals = vals[1:]
            continue
        break
    if len(vals) == 1:
        return vals[0], None
    if len(vals) >= 5:
        return vals[-3], vals[-2]
    if len(vals) == 4:
        first = vals[0]
        if first is not None and first == int(first) and 1 <= abs(first) <= 999:
            return vals[-2], vals[-1]
        return vals[-3], vals[-2]
    if len(vals) == 3:
        # غالباً: (مدين، دائن، رصيد) أو (رصيد، مدين، 0) — نرفض أخذ الرصيد كحركة.
        near_z = [i for i, v in enumerate(vals) if abs(float(v)) < 0.01]
        nz = [float(v) for v in vals if abs(float(v)) >= 0.01]
        if len(near_z) == 1 and len(nz) == 2:
            p, q = sorted(nz)
            if q >= p * 1.12 - 1e-9:
                zi = near_z[0]
                if zi == 2:
                    if vals[0] is not None and vals[1] is not None and float(vals[0]) > float(vals[1]) * 1.12:
                        return vals[1], None
                    return vals[0], None
                if zi == 1:
                    return vals[0], None
                if zi == 0:
                    return None, vals[1]
        third = vals[0]
        if third is not None and third == int(third) and abs(third) >= 10000:
            return vals[-2], vals[-1]
        return vals[-3], vals[-2]
    return vals[-2], vals[-1]


_LETTERHEAD_MARKERS = (
    "الرقم الضريبي",
    "السجل التجاري",
    "كشف حساب",
    "اسم العميل",
)

# أسطر تذييل / فوتر تُسرَّب أحياناً من PDF وتظهر كحركات وهمية
_PDF_FOOTER_MARKERS = (
    "تطوير الموقع",
    "السوداني",
    "محمد علي",
    "Mohammed",
    "Alsudani",
    "alsudani",
)

# لا نعتمد أزواج «نفس الاتجاه + فقط 45 نقطة»؛ ترفع الحد تترك صف بحر بدون شريك زائف.
MIN_ASSIGN_SCORE = 46
STRONG_MATCH_SCORE = 60
WEAK_MATCH_SCORE = 45


def _skip_statement_letterhead_lines(lines: List[str]) -> List[str]:
    start = 0
    for i, raw in enumerate(lines):
        s = raw.strip()
        if not s:
            continue
        if "مدين" in s and "دائن" in s:
            start = i + 1
            break
        if "مدين" in s and "التاريخ" in s:
            start = i + 1
            break
        if "الرصيد" in s and "مدين" in s:
            start = i + 1
            break
        if re.search(r"\d{4}\s*[-/.]\s*\d{1,2}\s*[-/.]\s*\d{1,2}", _normalize_arabic_digits(s)):
            if any(m in s for m in _LETTERHEAD_MARKERS):
                continue
            start = i
            break
    return lines[start:] if start else lines


def _extract_pdf_rows_from_text(raw_text: str) -> List[Dict[str, Any]]:
    date_pat = re.compile(
        r"(\d{4}\s*[/\-\.]\s*\d{1,2}\s*[/\-\.]\s*\d{1,2}|\d{1,2}\s*[/\-\.]\s*\d{1,2}\s*[/\-\.]\s*\d{2,4})"
    )
    num_pat = re.compile(r"[-+]?\d[\d,٬٫\.]*")

    body_lines = _skip_statement_letterhead_lines((raw_text or "").splitlines())
    rows: List[Dict[str, Any]] = []
    last_date: Optional[str] = None
    for raw_line in body_lines:
        line = _normalize_arabic_digits(raw_line).strip()
        if len(line) < 2:
            continue
        if any(m in raw_line for m in _PDF_FOOTER_MARKERS):
            continue
        if "تطوير الموقع" in raw_line or ("تطوير" in raw_line and "موقع" in raw_line):
            continue
        if "محمد علي" in raw_line and "السوداني" in raw_line:
            continue
        if any(m in raw_line for m in _LETTERHEAD_MARKERS) and "مدين" not in raw_line:
            continue

        date_m = date_pat.search(line)
        if date_m:
            last_date = date_m.group(1).strip()

        if date_m:
            work_line = (line[: date_m.start()] + " " + line[date_m.end() :]).strip()
        else:
            work_line = line

        effective_date = (date_m.group(1).strip() if date_m else None) or last_date
        if not effective_date:
            continue

        low_for_totals = _arabic_letters_for_match(work_line)
        if "اجمال" in low_for_totals:
            continue

        numbers = [n for n in num_pat.findall(work_line) if (pv := _parse_number_token(n)) is not None and _is_plausible_currency_amount(pv)]
        if not numbers:
            continue

        pv_pos = [float(_parse_number_token(n) or 0) for n in numbers]
        pv_pos = [x for x in pv_pos if x > 0.01]
        if len(pv_pos) >= 3 and min(pv_pos) > 30:
            if max(pv_pos) >= min(pv_pos) * 2.12 - 1e-9:
                continue
        ws_compact = re.sub(r"\s+", "", work_line)
        if "4455" in ws_compact and "1759" in ws_compact and len(pv_pos) >= 3:
            continue

        debit_val, credit_val = _debit_credit_from_tail_numbers(numbers)

        if (debit_val is None or abs(debit_val) < 0.0001) and (credit_val is None or abs(credit_val) < 0.0001):
            continue

        doc_text = _normalize_doc_text(work_line)
        if numbers:
            fm = num_pat.search(work_line)
            if fm:
                doc_text = _normalize_doc_text(work_line[: fm.start()])
        if len(doc_text) > 160:
            doc_text = doc_text[:160] + "..."

        rows.append(
            {
                "التاريخ": effective_date,
                "مدين": debit_val if debit_val is not None else "",
                "دائن": credit_val if credit_val is not None else "",
                "بيان": raw_line.strip(),
                "مستند": doc_text or "",
            }
        )

    return rows


def _trim_pdf_table_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    dfc = df.copy()
    dfc.columns = dfc.columns.astype(str).str.strip()

    for idx in range(min(8, len(dfc))):
        parts = [str(x) for x in dfc.iloc[idx].tolist() if pd.notna(x) and str(x).strip()]
        row_txt = " ".join(parts)
        if "مدين" in row_txt and "دائن" in row_txt:
            return dfc.iloc[idx + 1 :].reset_index(drop=True)

    try:
        date_col = detect_columns(dfc.copy()).get("date")
    except Exception:
        return dfc

    if date_col and date_col in dfc.columns:
        for idx, row in dfc.iterrows():
            val = row.get(date_col)
            if pd.isna(val):
                continue
            parsed = pd.to_datetime(val, errors="coerce", dayfirst=True)
            if pd.notna(parsed):
                return dfc.iloc[int(idx) :].reset_index(drop=True)
    return dfc


def _find_ledger_header_row_index(grid_rows: List[List[Any]], max_scan: int = 25) -> Optional[int]:
    """First grid row whose cells clearly label مدين and دائن (PDFs often prepend title rows)."""
    for idx in range(min(max_scan, len(grid_rows))):
        row = grid_rows[idx]
        if not row:
            continue
        cell_texts: List[str] = []
        for v in row:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            t = str(v).strip().lower()
            if t:
                cell_texts.append(t)
        if len(cell_texts) < 2:
            continue
        has_deb = any("مدين" in c and "دائن" not in c for c in cell_texts)
        has_cred = any("دائن" in c and "مدين" not in c for c in cell_texts)
        if has_deb and has_cred:
            return idx
    return None


def _dataframe_from_pdf_grid(grid_rows: List[List[Any]]) -> Optional[pd.DataFrame]:
    """Build a padded DataFrame from raw extract_tables rows; header row is detected, not assumed row 0."""
    if not grid_rows:
        return None
    hi = _find_ledger_header_row_index(grid_rows)
    if hi is None:
        if len(grid_rows) < 2:
            return None
        header_cells = list(grid_rows[0])
        data_rows = grid_rows[1:]
    else:
        header_cells = list(grid_rows[hi])
        data_rows = grid_rows[hi + 1 :]
    if not data_rows:
        return None
    max_w = max(len(r) for r in data_rows + [header_cells])
    width = max_w
    norm_data: List[List[Any]] = []
    for r in data_rows:
        rr = list(r) if r else []
        if len(rr) < width:
            rr = rr + [None] * (width - len(rr))
        norm_data.append(rr[:width])
    hdr = list(header_cells)
    if len(hdr) < width:
        hdr = hdr + [None] * (width - len(hdr))
    hdr = hdr[:width]
    col_names: List[str] = []
    counts: Dict[str, int] = {}
    for j in range(width):
        h = hdr[j]
        if h is None or (isinstance(h, float) and pd.isna(h)) or not str(h).strip():
            base = f"_c{j}"
        else:
            base = unicodedata.normalize("NFKC", str(h).strip())
        cnt = counts.get(base, 0)
        nm = base if cnt == 0 else f"{base}.{cnt}"
        counts[base] = cnt + 1
        col_names.append(nm)
    df = pd.DataFrame(norm_data, columns=col_names)
    return df.dropna(how="all")


def _estimate_extractable_rows(df: Optional[pd.DataFrame]) -> int:
    if df is None or df.empty:
        return 0
    try:
        dfc = df.copy()
        dfc.columns = dfc.columns.astype(str).str.strip()
        dfc = _promote_ledger_header_row(dfc)
        dfc.columns = dfc.columns.astype(str).str.strip()
        detected = detect_columns(dfc)
        debit_col, credit_col = detected.get("debit"), detected.get("credit")
    except Exception:
        return 0

    n = 0
    for _, row in dfc.iterrows():
        if row.isna().all():
            continue
        deb = safe(row[debit_col]) if debit_col and debit_col in dfc.columns else None
        cre = safe(row[credit_col]) if credit_col and credit_col in dfc.columns else None
        if deb is not None or cre is not None:
            n += 1
    return n


def _extract_tables_best_effort(page: Any) -> List[List[List[Any]]]:
    """يجرب عدة إعدادات؛ الجداول بدون خطوط واضحة تفشل مع الافتراضي أحياناً."""
    variants: List[Optional[Dict[str, Any]]] = [
        None,
        {
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "intersection_tolerance": 5,
            "snap_tolerance": 3,
            "join_tolerance": 3,
        },
        {"vertical_strategy": "text", "horizontal_strategy": "text", "snap_tolerance": 5, "join_tolerance": 5},
        {"vertical_strategy": "lines", "horizontal_strategy": "text", "intersection_tolerance": 5},
    ]
    best_tables: Optional[List[List[List[Any]]]] = None
    best_score = -1
    for ts in variants:
        try:
            if ts is None:
                tables = page.extract_tables()
            else:
                tables = page.extract_tables(table_settings=ts)
        except Exception:
            continue
        if not tables:
            continue
        score = 0
        for table in tables:
            if not table:
                continue
            for row in table:
                if row and any(cell is not None and str(cell).strip() for cell in row):
                    score += 1
        if score > best_score:
            best_score = score
            best_tables = tables
    return best_tables or []


def read_pdf(file_path: str) -> Optional[pd.DataFrame]:
    table_df: Optional[pd.DataFrame] = None
    all_text = ""

    try:
        with pdfplumber.open(file_path) as pdf:
            grid_rows: List[List[Any]] = []
            text_parts: List[str] = []
            for page in pdf.pages:
                text_parts.append(page.extract_text() or "")
                tables = _extract_tables_best_effort(page)
                if not tables:
                    continue
                for table in tables:
                    for row in table:
                        if row and any(cell is not None for cell in row):
                            grid_rows.append(row)

            all_text = "\n".join(text_parts)

            if grid_rows:
                df = _dataframe_from_pdf_grid(grid_rows)
                if df is not None and not df.empty:
                    table_df = _trim_pdf_table_df(df.reset_index(drop=True))
    except Exception:
        return None

    parsed_rows = _extract_pdf_rows_from_text(all_text)
    text_df: Optional[pd.DataFrame] = None
    if parsed_rows:
        text_df = pd.DataFrame(parsed_rows).dropna(how="all")

    candidates: List[pd.DataFrame] = [d for d in (table_df, text_df) if d is not None and not d.empty]
    if not candidates:
        return None

    scored = [(d, _estimate_extractable_rows(d)) for d in candidates]
    best_score = max(sc for _, sc in scored)
    if best_score > 0:
        for d, sc in scored:
            if sc == best_score:
                return d
    return max(candidates, key=len)


def read_any(file_path: str, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        out = read_excel(file_path)
        if out is None:
            raise ValueError("Excel file has no readable data")
        return out
    if name.endswith(".csv"):
        try:
            out = pd.read_csv(file_path, encoding="utf-8-sig")
        except UnicodeDecodeError:
            out = pd.read_csv(file_path, encoding="cp1256")
        if out is None or out.empty:
            raise ValueError("ملف CSV فارغ أو غير مقروء")
        return out.dropna(how="all")
    if name.endswith(".pdf"):
        out = read_pdf(file_path)
        if out is None:
            raise ValueError(
                "لم يُستخرج من PDF جداول أو أسطر حركة واضحة. جرّب: ملف Excel، أو PDF نصّي (وليس صورة ممسوحة)، أو تأكد أن أعمدة المدين/الدائن ظاهرة في النص."
            )
        return out
    raise ValueError("نوع الملف غير مدعوم")


doc_map: Dict[str, str] = {
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
    "فاتورة مبيعات": "فاتورة مشتريات",
    "فاتورة مشتريات": "فاتورة مبيعات",
}


def clean_doc(s: Any) -> str:
    if not s:
        return ""
    s = str(s).lower().strip()
    s = s.replace("رقم", "")
    s = s.replace("-", "")
    s = s.replace("_", "")
    s = s.replace("  ", " ")
    return s


def clean(s: Any) -> str:
    if not s:
        return ""
    s = str(s).lower().strip()
    for w in ["رقم", "no", "doc", "ref"]:
        s = s.replace(w, "")
    s = re.sub(r"\d+", "", s)
    for ch in [" ", "-", "_", "/", "\\", ".", ","]:
        s = s.replace(ch, "")
    return s


def _is_voucher_number_string(s: Any) -> bool:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return False
    t = str(s).strip().translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))
    t = t.replace(",", "").replace("٬", "").replace(" ", "")
    if not t or "." in t or "٫" in str(s):
        return False
    return bool(re.fullmatch(r"\d{3,20}", t))


def _doc_for_matching(d: Any) -> Optional[str]:
    if d is None or (isinstance(d, str) and not str(d).strip()):
        return None
    if _is_voucher_number_string(d):
        return None
    return str(d).strip()


def match_doc(d1: Any, d2: Any) -> bool:
    if not d1 and not d2:
        return True
    if not d1 or not d2:
        return False
    if _is_voucher_number_string(d1) or _is_voucher_number_string(d2):
        return False

    d1 = clean(d1)
    d2 = clean(d2)

    if not d1 and not d2:
        return True
    if not d1 or not d2:
        return False
    if d1 == d2:
        return True
    if len(d1) > 3 and len(d2) > 3:
        if d1 in d2 or d2 in d1:
            return True

    for key, val in doc_map.items():
        k = clean(key)
        v = clean(val)
        if (k in d1 and v in d2) or (v in d1 and k in d2):
            return True

    similarity = SequenceMatcher(None, d1, d2).ratio()
    return similarity > 0.7


def date_diff_days(d1: Any, d2: Any) -> Optional[int]:
    try:
        dd1, dd2 = _parsed_timestamp_or_nat(d1), _parsed_timestamp_or_nat(d2)
        if pd.isna(dd1) or pd.isna(dd2):
            return None
        return abs(int((dd1 - dd2).days))
    except Exception:
        return None


def _row_contains_statement_footer(row: pd.Series) -> bool:
    """سطر تذييل / اعتماد من PDF (مثل تطوير الموقع) لا يُعامل كحركة."""
    for v in row.tolist():
        if pd.isna(v):
            continue
        s = str(v).strip()
        if not s:
            continue
        if any(m in s for m in _PDF_FOOTER_MARKERS):
            return True
        if "تطوير الموقع" in s or ("تطوير" in s and "موقع" in s):
            return True
        if "محمد علي" in s and "السوداني" in s:
            return True
    return False


def _correct_movement_if_debit_equals_balance(
    row: pd.Series,
    df: pd.DataFrame,
    debit_col: Optional[str],
    credit_col: Optional[str],
    balance_col: Optional[str],
    date_col: Optional[str],
    narrative: str,
    debit: Optional[float],
    credit: Optional[float],
) -> Tuple[Optional[float], Optional[float]]:
    """
    جداول PDF أحياناً تضع قيمة الرصيد في عمود المدين أو تنسخ الرصيد. إذا مدين ≈ الرصيد والدائن ~0
    نبحث في نفس السطر عن مبلغ أصغر من الرصيد يصلح أن يكون حركة السطر.
    """
    if not balance_col or balance_col not in df.columns or not debit_col or debit_col not in df.columns:
        return debit, credit
    if debit is None:
        return debit, credit
    bal = safe(row[balance_col])
    if bal is None or abs(float(debit) - float(bal)) > 0.02:
        return debit, credit
    cre = credit
    if cre is not None and float(cre) > 0.01:
        return debit, credit

    alts: List[float] = []
    for c in df.columns:
        if c == balance_col:
            continue
        n = str(c).lower()
        if date_col and c == date_col:
            continue
        if "تاريخ" in n or "date" in n:
            continue
        if _column_name_excludes_from_amount(c):
            continue
        v = safe(row[c])
        if v is None or v <= 0:
            continue
        if not _is_plausible_currency_amount(v):
            continue
        if abs(float(v) - float(bal)) < 0.02:
            continue
        if float(v) < float(bal) - 0.01:
            alts.append(float(v))

    if alts:
        replacement = max(alts)
        if replacement + 0.02 < float(debit):
            return round(replacement, 2), credit

    nums = [v for v in _parse_currency_numbers_from_narrative(narrative) if v and v < float(bal) - 0.01]
    if len(nums) == 1:
        replacement = nums[0]
        if replacement + 0.02 < float(debit):
            return round(float(replacement), 2), credit

    return debit, credit


def process(file_path: str, filename: str, branch: str) -> List[Dict[str, Any]]:
    if _use_legacy_analyzer():
        return legacy_process(file_path, filename, branch)
    df = read_any(file_path, filename)
    if df is None or len(df) == 0:
        return []

    df.columns = df.columns.astype(str).str.strip()
    df = _promote_ledger_header_row(df)
    df.columns = df.columns.astype(str).str.strip()
    detected_cols = detect_columns(df)
    debit_col = detected_cols.get("debit")
    credit_col = detected_cols.get("credit")
    date_col = detected_cols.get("date")
    doc_col, doc_fb = resolve_document_columns(df)

    data: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        if row.isna().all():
            continue

        narrative = _row_narrative_for_amounts(row, df, doc_col, doc_fb, debit_col, credit_col, date_col)

        if _row_contains_statement_footer(row):
            continue
        nar_plain = _normalize_doc_text(narrative)
        if "تطوير الموقع" in (narrative or "") or (
            "محمد علي" in (narrative or "") and "السوداني" in (narrative or "")
        ):
            continue
        if len(nar_plain) <= 200:
            nn = [v for v in _parse_currency_numbers_from_narrative(narrative or "") if v and v > 30]
            if len(nn) >= 3 and max(nn) >= min(nn) * 2.12 - 1e-9:
                continue

        debit = safe(row[debit_col]) if debit_col and debit_col in df.columns else None
        credit = safe(row[credit_col]) if credit_col and credit_col in df.columns else None

        balance_col = next((c for c in df.columns if _is_balance_column_name(c)), None)
        debit, credit = _correct_movement_if_debit_equals_balance(
            row, df, debit_col, credit_col, balance_col, date_col, narrative, debit, credit
        )

        decs = [
            v
            for v in _parse_currency_numbers_from_narrative(narrative)
            if abs(v - int(v)) > 0.0001 and abs(v) >= 0.0001
        ]
        if debit is not None and credit is not None and debit > 0 and credit > 0:
            if (
                _looks_like_serial_voucher_amount(debit)
                and _looks_like_serial_voucher_amount(credit)
                and len(decs) >= 2
            ):
                debit, credit = round(decs[0], 2), round(decs[1], 2)
            else:
                debit = _replace_voucher_with_ledger_from_narrative(debit, narrative)
                credit = _replace_voucher_with_ledger_from_narrative(credit, narrative)
        else:
            if debit is not None:
                debit = _replace_voucher_with_ledger_from_narrative(debit, narrative)
            if credit is not None:
                credit = _replace_voucher_with_ledger_from_narrative(credit, narrative)

        if debit and credit and debit > 0 and credit > 0:
            side_hint = _side_hint_from_narrative(narrative)
            if side_hint == "debit":
                credit = None
            elif side_hint == "credit":
                debit = None

        if debit is None and credit is None:
            continue

        if debit and credit and debit > 0 and credit > 0:
            date_out, doc_out = extract_row_date_doc(row, df, date_col, doc_col, doc_fb)
            if not date_out:
                date_out = _iso_date_from_narrative(narrative)
            doc_out = _finalize_doc_for_row(doc_out, narrative)
            amount = max(debit, credit)
            t = "credit" if credit >= debit else "debit"
            if _is_likely_pdf_extraction_noise(float(amount), t, doc_out, narrative):
                continue
            data.append(
                {
                    "amount": float(amount),
                    "type": t,
                    "branch": branch,
                    "date": date_out,
                    "doc": doc_out,
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

        if _skip_likely_pdf_line_index_row(amount, debit, credit, False):
            continue

        date_out, doc_out = extract_row_date_doc(row, df, date_col, doc_col, doc_fb)
        if not date_out:
            date_out = _iso_date_from_narrative(narrative)
        doc_out = _finalize_doc_for_row(doc_out, narrative)
        if _is_likely_pdf_extraction_noise(float(amount), t, doc_out, narrative):
            continue
        data.append(
            {
                "amount": float(amount),
                "type": t,
                "branch": branch,
                "date": date_out,
                "doc": doc_out,
            }
        )

    return _dedupe_extracted_rows(data)


def analyze(
    d1: List[Dict[str, Any]],
    d2: List[Dict[str, Any]],
    *,
    allow_same_direction: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    if _use_legacy_analyzer():
        return legacy_analyze(d1, d2)

    res: List[Dict[str, Any]] = []
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
                days = date_diff_days(x1["date"], x2["date"])
                if days is None or days > 1:
                    continue
                dm1, dm2 = _doc_for_matching(x1.get("doc")), _doc_for_matching(x2.get("doc"))
                if dm1 and dm2:
                    if not match_doc(dm1, dm2):
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

    # if branch2 empty
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
            reasons.append("فرق مبلغ (غير مانع)")

        if (x1["type"] == "credit" and x2["type"] == "debit") or (x1["type"] == "debit" and x2["type"] == "credit"):
            score += 30
            reasons.append("اتجاه عكسي صحيح")
        elif allow_same_direction:
            score += 15
            reasons.append("نفس الاتجاه بين الملفين")
        else:
            return 0, ["نفس الاتجاه"]

        d1m = _doc_for_matching(x1.get("doc"))
        d2m = _doc_for_matching(x2.get("doc"))
        if d1m and d2m:
            if not match_doc(d1m, d2m):
                return 0, ["اختلاف نوع المستند"]
            score += 20
            reasons.append("نوع مستند مطابق")

        both_no_doc = not d1m and not d2m
        dd1 = _parsed_timestamp_or_nat(x1.get("date"))
        dd2 = _parsed_timestamp_or_nat(x2.get("date"))
        has1, has2 = pd.notna(dd1), pd.notna(dd2)
        if has1 and has2:
            days = abs(int((dd1 - dd2).days))
            if days == 0:
                score += 20
                reasons.append("نفس اليوم")
            elif days <= 7:
                score += 10
                reasons.append("تاريخ قريب")
            elif days <= 45:
                reasons.append("فارق تاريخ ضمن المدى")
            elif both_no_doc:
                reasons.append("فارق تاريخ (بدون بيان مستند)")
            else:
                score -= 5
                reasons.append("تاريخ بعيد جدا مع اختلاف بيان المستند")
        elif has1 or has2:
            score += 8
            reasons.append("تاريخ من جهة واحدة فقط")
        else:
            reasons.append("لا تاريخ في كلا السطرين")

        return score, reasons

    assignment = _assign_global_matches(d1, d2, match_score, MIN_ASSIGN_SCORE)
    matched_js = {v[0] for v in assignment.values()}

    for i, x1 in enumerate(d1):
        if x1.get("type") == "error":
            res.append(x1)
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
            continue
        if i not in assignment:
            best_s = -1
            best_r: List[str] = []
            for x2 in d2:
                if x2.get("type") == "error":
                    continue
                s, r = match_score(x1, x2)
                if s > best_s:
                    best_s, best_r = s, r
            tail = f" | {' , '.join(best_r)}" if best_r else ""
            res.append({**x1, "reason": f"لا يوجد مقابل ❌ | أفضل score={best_s}{tail}"})
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
            continue
        _j, s, r = assignment[i]
        if s >= STRONG_MATCH_SCORE:
            continue
        res.append({**x1, "reason": f"تطابق ضعيف ⚠️ | score={s} | {' , '.join(r)}"})

    for j, x in enumerate(d2):
        if j in matched_js:
            continue
        if x.get("type") == "error":
            res.append(x)
            b = x.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
            continue
        res.append({**x, "reason": "لا يوجد مقابل ❌ (من الفرع الآخر)"})
        b = x.get("branch") or "unknown"
        counts[b] = counts.get(b, 0) + 1

    return res, counts


def compute_summary(
    d1: List[Dict[str, Any]],
    d2: List[Dict[str, Any]],
    mismatch_entries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    total_ops = len(d1) + len(d2)
    mismatch_ops = len(mismatch_entries)
    matched_ops = max(0, total_ops - mismatch_ops)

    errors_count = 0
    warnings_count = 0
    for e in mismatch_entries:
        reason = (e.get("reason") or "").lower()
        if e.get("type") == "error" or "❌" in e.get("reason", "") or "لا يوجد مقابل" in reason:
            errors_count += 1
        elif "⚠️" in e.get("reason", ""):
            warnings_count += 1
    return {
        "total_ops": total_ops,
        "matched_ops": matched_ops,
        "mismatch_ops": mismatch_ops,
        "errors_count": errors_count,
        "warnings_count": warnings_count,
    }

