from __future__ import annotations

import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pdfplumber


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
        try:
            x = float(s)
        except ValueError:
            m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
            if not m:
                return None
            x = float(m.group(0))
        if not _is_plausible_currency_amount(x):
            return None
        return round(x, 2)
    except Exception:
        return None


def _column_name_excludes_from_amount(col: Any) -> bool:
    name = str(col).lower().strip()
    if not name or name.isdigit():
        return False
    block = (
        "رقم السند",
        "رقم سند",
        "السند",
        "سند",
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


def detect_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    df.columns = df.columns.astype(str).str.strip()

    debit_col = None
    credit_col = None
    date_col = None

    for col in df.columns:
        name = str(col).lower()
        if _column_name_excludes_from_amount(col):
            continue
        if any(x in name for x in ["مدين", "debit", "dr"]) and "دائن" not in name and "credit" not in name:
            debit_col = col
        if any(x in name for x in ["دائن", "credit", "cr"]) and "مدين" not in name and "debit" not in name:
            credit_col = col
        if any(x in name for x in ["تاريخ", "التاريخ", "التأريخ", "date"]):
            date_col = col

    numeric_cols: List[Tuple[str, float]] = []
    for col in df.columns:
        if _column_name_excludes_from_amount(col):
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
        numeric_cols.append((col, float(mean_val)))

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


def detect_document_type_column(df: pd.DataFrame) -> Optional[str]:
    for col in df.columns:
        n = str(col).strip().lower()
        if "رقم السند" in n or "رقم سند" in n or n in ("#", "م"):
            continue
        if n in ("العنوان", "عنوان", "العميل", "اسم العميل", "المورد"):
            continue
        if "ضريبي" in n or "سجل التجاري" in n:
            continue
        if "نوع المستند" in n:
            return col
    for col in df.columns:
        n = str(col).strip().lower()
        if "رقم السند" in n or "رقم سند" in n:
            continue
        if n == "المستند" or n.endswith(" المستند"):
            return col
    for col in df.columns:
        n = str(col).strip().lower()
        if "رقم السند" in n or "رقم سند" in n:
            continue
        if n == "نوع" or (n.startswith("نوع") and "عميل" not in n and "مورد" not in n and "حساب" not in n):
            return col
    return None


def extract_row_date_doc(
    row: pd.Series, df: pd.DataFrame, date_col: Optional[str], doc_col: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    date_out: Optional[str] = None
    if date_col and date_col in df.columns:
        try:
            val = row[date_col]
            if pd.isna(val):
                date_out = None
            else:
                d = pd.to_datetime(val, errors="coerce", dayfirst=False)
                date_out = None if pd.isna(d) else d.strftime("%Y-%m-%d")
        except Exception:
            date_out = str(row[date_col])

    doc_out: Optional[str] = None
    if doc_col and doc_col in df.columns:
        val = row[doc_col]
        if pd.notna(val):
            raw = str(val).strip()
            if not _is_voucher_number_string(raw):
                doc_out = raw

    return date_out, doc_out


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

        numbers = [
            n
            for n in num_pat.findall(work_line)
            if (pv := _parse_number_token(n)) is not None and _is_plausible_currency_amount(pv)
        ]
        if not numbers:
            continue

        debit_val, credit_val = _debit_credit_from_tail_numbers(numbers)

        if (debit_val is None or abs(debit_val) < 0.0001) and (credit_val is None or abs(credit_val) < 0.0001):
            continue

        rows.append(
            {
                "التاريخ": effective_date,
                "مدين": debit_val if debit_val is not None else "",
                "دائن": credit_val if credit_val is not None else "",
                "بيان": raw_line.strip(),
                "مستند": "",
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
        _, _, date_col = detect_columns(dfc.copy())
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


def _estimate_extractable_rows(df: Optional[pd.DataFrame]) -> int:
    if df is None or df.empty:
        return 0
    try:
        dfc = df.copy()
        dfc.columns = dfc.columns.astype(str).str.strip()
        debit_col, credit_col, _ = detect_columns(dfc)
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


def read_pdf(file_path: str) -> Optional[pd.DataFrame]:
    table_df: Optional[pd.DataFrame] = None
    all_text = ""

    try:
        with pdfplumber.open(file_path) as pdf:
            grid_rows: List[List[Any]] = []
            text_parts: List[str] = []
            for page in pdf.pages:
                text_parts.append(page.extract_text() or "")
                tables = page.extract_tables()
                if not tables:
                    continue
                for table in tables:
                    for row in table:
                        if row and any(cell is not None for cell in row):
                            grid_rows.append(row)

            all_text = "\n".join(text_parts)

            if grid_rows:
                df = pd.DataFrame(grid_rows).dropna(how="all")
                if len(df) >= 2:
                    df.columns = df.iloc[0]
                    df = df[1:].dropna(how="all")
                    if not df.empty:
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
        dd1 = pd.to_datetime(d1, errors="coerce")
        dd2 = pd.to_datetime(d2, errors="coerce")
        if pd.isna(dd1) or pd.isna(dd2):
            return None
        return abs((dd1 - dd2).days)
    except Exception:
        return None


def process(file_path: str, filename: str, branch: str) -> List[Dict[str, Any]]:
    df = read_any(file_path, filename)
    if df is None or len(df) == 0:
        return []

    df.columns = df.columns.astype(str).str.strip()
    debit_col, credit_col, date_col = detect_columns(df)
    doc_col = detect_document_type_column(df)

    if not debit_col and not credit_col:
        numeric_cols: List[Tuple[str, float]] = []
        for col in df.columns:
            if _column_name_excludes_from_amount(col):
                continue
            nums = pd.to_numeric(df[col], errors="coerce").dropna()
            frac = 0.3 if len(df) >= 12 else 0.15
            need = max(1, int(len(df) * frac + 0.5))
            if len(df) >= 4:
                need = max(2, need)
            if len(nums) < need:
                continue
            if _column_values_look_like_ids(df[col]):
                continue
            mean_val = float(nums.mean())
            max_val = float(nums.max())
            if mean_val < 10 and max_val < 10:
                continue
            if not _is_plausible_currency_amount(max_val):
                continue
            numeric_cols.append((col, mean_val))
        numeric_cols.sort(key=lambda x: x[1], reverse=True)
        if len(numeric_cols) >= 1:
            debit_col = numeric_cols[0][0]
        if len(numeric_cols) >= 2:
            credit_col = numeric_cols[1][0]

    data: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        if row.isna().all():
            continue

        debit = safe(row[debit_col]) if debit_col and debit_col in df.columns else None
        credit = safe(row[credit_col]) if credit_col and credit_col in df.columns else None

        if debit is None and credit is None:
            continue

        if debit and credit and debit > 0 and credit > 0:
            date_out, doc_out = extract_row_date_doc(row, df, date_col, doc_col)
            amount = max(debit, credit)
            t = "credit" if credit >= debit else "debit"
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

        date_out, doc_out = extract_row_date_doc(row, df, date_col, doc_col)
        data.append(
            {
                "amount": float(amount),
                "type": t,
                "branch": branch,
                "date": date_out,
                "doc": doc_out,
            }
        )

    return data


def analyze(d1: List[Dict[str, Any]], d2: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
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
            return 0, ["فرق مبلغ كبير"]

        if (x1["type"] == "credit" and x2["type"] == "debit") or (x1["type"] == "debit" and x2["type"] == "credit"):
            score += 30
            reasons.append("اتجاه عكسي صحيح")
        else:
            return 0, ["نفس الاتجاه"]

        d1m = _doc_for_matching(x1.get("doc"))
        d2m = _doc_for_matching(x2.get("doc"))
        if d1m and d2m:
            if not match_doc(d1m, d2m):
                return 0, ["اختلاف نوع المستند"]
            score += 20
            reasons.append("نوع مستند مطابق")

        days = date_diff_days(x1["date"], x2["date"])
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
            res.append({**x1, "reason": f"تطابق ضعيف ⚠️ | score={best_score} | {' , '.join(best_reason)}"})
            used[best_i] = True
        else:
            res.append({**x1, "reason": f"لا يوجد مقابل ❌ | score={best_score} | {' , '.join(best_reason)}"})
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1

    # remaining from branch2
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

