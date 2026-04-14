from __future__ import annotations

import io
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

from .analyzer import (
    _is_balance_column_name,
    _normalize_arabic_digits,
    _promote_ledger_header_row,
    detect_columns,
    detect_document_type_column,
    infer_document_kind_from_narrative,
    read_pdf,
    resolve_document_columns,
    safe,
)


def _has_arabic_script(s: str) -> bool:
    for c in s:
        o = ord(c)
        if 0x0600 <= o <= 0x06FF or 0xFB50 <= o <= 0xFDFF or 0xFE70 <= o <= 0xFEFF:
            return True
    return False


def _excel_arabic_display(value: Any) -> Any:
    """تطبيع خفيف للنص العربي دون قلب ترتيب الأرقام داخل Excel."""
    if value is None:
        return value
    if isinstance(value, float) and pd.isna(value):
        return value
    if not isinstance(value, str):
        return value
    s = value.strip()
    if not s or not _has_arabic_script(s):
        return value
    try:
        # NFKC يحول presentation forms لنص عربي قياسي بدون تغيير ترتيب الكلمات/الأرقام.
        return unicodedata.normalize("NFKC", s)
    except Exception:
        return value

# تاريخ في السطر (كشوف عربية غالبًا يوم/شهر/سنة)
_DATE_IN_ROW = re.compile(
    r"(\d{4}\s*[/\-\.]\s*\d{1,2}\s*[/\-\.]\s*\d{1,2}|\d{1,2}\s*[/\-\.]\s*\d{1,2}\s*[/\-\.]\s*\d{2,4})"
)


def _row_text_blob(row: pd.Series, columns) -> str:
    parts: list[str] = []
    for c in columns:
        v = row.get(c)
        if pd.notna(v) and str(v).strip():
            parts.append(str(v).strip())
    return _normalize_arabic_digits(" ".join(parts))


def _row_has_currency_amount(row: pd.Series, columns) -> bool:
    for c in columns:
        v = safe(row.get(c))
        if v is not None and abs(float(v)) > 0.005:
            return True
    return False


def _trim_table_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """إزالة صفوف ترويسة الشركة/التقرير؛ البدء من أول صف يشبه حركة (تاريخ + مبلغ)."""
    dfc = df.copy()
    dfc.columns = dfc.columns.astype(str).str.strip()
    dfc = _promote_ledger_header_row(dfc)
    dfc.columns = dfc.columns.astype(str).str.strip()
    if dfc.empty:
        return dfc

    for idx in range(len(dfc)):
        row = dfc.iloc[idx]
        if row.isna().all():
            continue
        blob = _row_text_blob(row, dfc.columns)
        if not _DATE_IN_ROW.search(blob):
            continue
        if _row_has_currency_amount(row, dfc.columns):
            return dfc.iloc[idx:].reset_index(drop=True)

    try:
        debit_col, credit_col, _ = detect_columns(dfc.copy())
    except Exception:
        return dfc

    for idx in range(len(dfc)):
        row = dfc.iloc[idx]
        if row.isna().all():
            continue
        deb = safe(row[debit_col]) if debit_col and debit_col in dfc.columns else None
        cre = safe(row[credit_col]) if credit_col and credit_col in dfc.columns else None
        if deb is not None or cre is not None:
            return dfc.iloc[idx:].reset_index(drop=True)

    return dfc


def _trim_text_style_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """جدول مستخرج من النص (أعمدة التاريخ/مدين/دائن)."""
    if "التاريخ" not in df.columns:
        return df
    start = 0
    for i in range(len(df)):
        row = df.iloc[i]
        val = row.get("التاريخ")
        if pd.isna(val):
            start = i + 1
            continue
        s = _normalize_arabic_digits(str(val).strip())
        if not _DATE_IN_ROW.search(s):
            start = i + 1
            continue
        if _row_has_currency_amount(row, df.columns):
            start = i
            break
        if pd.notna(row.get("بيان")) and str(row.get("بيان")).strip():
            start = i
            break
    return df.iloc[start:].reset_index(drop=True) if len(df) else df


def _prepare_dataframe_for_excel_export(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = [str(c).strip() for c in df.columns]
    if "التاريخ" in cols and "مدين" in cols:
        return _trim_text_style_for_excel(df)
    return _trim_table_for_excel(df)


def _narrative_blob(row: pd.Series, df: pd.DataFrame) -> str:
    """نص للاستدلال على نوع المستند من أعمدة البيان/المستند."""
    parts: list[str] = []
    for name in ("بيان", "المستند", "مستند", "الشرح", "الوصف"):
        if name not in df.columns:
            continue
        v = row.get(name)
        if pd.notna(v) and str(v).strip():
            parts.append(str(v).strip())
    return " ".join(parts)


def _infer_doc_type_fallback(narrative: str) -> str:
    """كلمات شائعة في كشوف العملاء إذا لم يُستنتج النوع من القاموس الرئيسي."""
    if not narrative:
        return ""
    s = unicodedata.normalize("NFKC", _normalize_arabic_digits(narrative))
    if "افتتاح" in s or "الافتتاحي" in s or "ﺍﻺﻔﺘﺗﺎﺤﻳ" in narrative:
        return "الرصيد الافتتاحي"
    if "سند قبض" in s or "ﻕﺑﻗ ﺩﻧﺳ" in narrative:
        if "بنك" in s or "ﻲﻛﻧﺑ" in narrative:
            return "سند قبض بنكي"
        return "سند قبض"
    if "سند صرف" in s:
        return "سند صرف"
    if ("توريد" in s and "مخزن" in s) or ("ديروت" in s and "ينزخم" in s) or ("ﻲﻧزﺧﻣ" in narrative and "دﯾروﺗ" in narrative):
        return "توريد مخزني"
    if ("صرف" in s and "مخزن" in s) or ("فرص" in s and "ينزخم" in s) or ("ﻲﻧزﺧﻣ" in narrative and "فرﺻ" in narrative):
        return "صرف مخزني"
    if "تحويل" in s and "مخزن" in s:
        return "تحويل مخزني"
    if "فاتورة مبيعات" in s or ("مبيعات" in s and "فاتور" in s):
        if "آجل" in s or "ﺝﻵﺍ" in narrative:
            return "فاتورة مبيعات آجلة"
        return "فاتورة مبيعات"
    if "فاتورة مشتريات" in s or ("مشتريات" in s and "فاتور" in s):
        return "فاتورة مشتريات"
    if "مردود" in s:
        return "مردود"
    if "دفع" in s or "ﻊﻓﺩ" in narrative:
        return "دفعة / تسوية"
    return ""


_NUM_TOKEN = re.compile(r"[-+]?\d[\d,٬٫\.]*")


def _extract_amount_candidates(text: str) -> list[float]:
    vals: list[float] = []
    s = _normalize_arabic_digits(text or "")
    for tok in _NUM_TOKEN.findall(s):
        v = safe(tok)
        if v is None:
            continue
        x = float(v)
        # تجاهل الأرقام الصغيرة غالباً (رقم تسلسل/يوم)
        if abs(x) < 1:
            continue
        vals.append(abs(x))
    return vals


def _normalize_debit_credit_by_context(doc_t: str, narrative: str, deb: Any, cre: Any) -> tuple[Any, Any]:
    """تصحيح انحرافات استخراج مدين/دائن عندما يظهر المبلغ الحقيقي داخل البيان فقط."""
    d = safe(deb)
    c = safe(cre)
    d0 = abs(float(d)) if d is not None else 0.0
    c0 = abs(float(c)) if c is not None else 0.0

    nums = _extract_amount_candidates(narrative)
    major = max(nums) if nums else 0.0
    if major <= 0:
        return deb, cre

    ns = unicodedata.normalize("NFKC", _normalize_arabic_digits(narrative or ""))
    has_credit_hint = ("دائن" in ns) or ("نئاد" in ns) or ("ﺩﺍﺋﻥ" in narrative)
    has_debit_hint = ("مدين" in ns) or ("نيدم" in ns) or ("ﻣﺩﻳﻥ" in narrative)
    # تلميح صريح في السطر له الأولوية حتى لو الكشف الرقمي يبدو "قوياً".
    if has_credit_hint and not has_debit_hint:
        return "", round(major, 2)
    if has_debit_hint and not has_credit_hint:
        return round(major, 2), ""

    kind = (doc_t or "").strip()
    # إذا المبلغ المستخرج في الخانات أصغر بكثير من المبلغ الموجود في البيان، نستخدم الأكبر.
    weak_detect = (max(d0, c0) <= 0) or (major >= max(d0, c0) * 1.8 and major >= 50)
    if not weak_detect:
        return deb, cre

    if "قبض" in kind:
        return "", round(major, 2)
    if "صرف" in kind:
        return round(major, 2), ""
    if "مبيعات" in kind or "افتتاح" in kind:
        return round(major, 2), ""
    if "مشتريات" in kind:
        return "", round(major, 2)
    # افتراضي: حافظ على الجانب الأقوى إن وجد، وإلا ضعها مدين
    if c0 > d0:
        return "", round(major, 2)
    return round(major, 2), ""


def _is_noise_ledger_row(dt: Any, doc_t: str, deb: Any, cre: Any, bal: Any) -> bool:
    """حذف صفوف ضجيج شائعة (مثل رقم 1 القادم من الترويسة) قبل التصدير."""
    d = safe(deb)
    c = safe(cre)
    b = safe(bal)
    d0 = abs(float(d)) if d is not None else 0.0
    c0 = abs(float(c)) if c is not None else 0.0
    b0 = abs(float(b)) if b is not None else 0.0
    has_doc = bool((doc_t or "").strip())
    has_date = pd.notna(dt) and str(dt).strip() not in ("", "NaT", "nat")
    # صف بتاريخ + مبلغ صغير جداً + بدون نوع مستند/رصيد => غالباً ليس حركة.
    return has_date and not has_doc and b0 <= 0.0001 and max(d0, c0) <= 1.0


def _reframe_ledger_columns_clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    صيغة موحّدة للتصدير: التاريخ، نوع المستند، مدين، دائن، الرصيد (فارغ إن لم يُستخرج عمود رصيد).
    """
    if df is None or df.empty:
        return df

    debit_col, credit_col, date_col = detect_columns(df.copy())
    if "التاريخ" in df.columns:
        date_col = date_col or "التاريخ"
    if debit_col is None and "مدين" in df.columns:
        debit_col = "مدين"

    if date_col is None or debit_col is None or date_col not in df.columns or debit_col not in df.columns:
        return df

    balance_col = None
    for c in df.columns:
        if _is_balance_column_name(c):
            balance_col = c
            break

    doc_type_col = detect_document_type_column(df)
    if not doc_type_col:
        prim, _fb = resolve_document_columns(df)
        doc_type_col = prim

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        dt = row.get(date_col)
        deb = row.get(debit_col)
        cre = row.get(credit_col) if credit_col and credit_col in df.columns else None
        bal = row.get(balance_col) if balance_col and balance_col in df.columns else None

        if doc_type_col and doc_type_col in df.columns:
            raw_dt = row.get(doc_type_col)
            doc_t = str(raw_dt).strip() if pd.notna(raw_dt) and str(raw_dt).strip() else ""
        else:
            doc_t = ""

        if not doc_t:
            nar = _narrative_blob(row, df)
            doc_t = infer_document_kind_from_narrative(nar) or _infer_doc_type_fallback(nar)
        else:
            nar = _narrative_blob(row, df)

        deb, cre = _normalize_debit_credit_by_context(doc_t, nar, deb, cre)

        def _blank_num(x: Any) -> Any:
            if x is None:
                return ""
            if isinstance(x, float) and pd.isna(x):
                return ""
            v = safe(x)
            if v is not None and abs(float(v)) <= 0.0001:
                return ""
            return x

        if _is_noise_ledger_row(dt, doc_t, deb, cre, bal):
            continue

        rows.append(
            {
                "التاريخ": dt,
                "نوع المستند": doc_t or "",
                "مدين": deb,
                "دائن": _blank_num(cre),
                "الرصيد": _blank_num(bal),
            }
        )

    return pd.DataFrame(rows)


def pdf_to_excel_bytes(file_path: str) -> bytes:
    df = read_pdf(file_path)
    if df is None or df.empty:
        raise ValueError(
            "تعذر استخراج بيانات من PDF. جرّب ملفاً نصياً (وليس صورة ممسوحة)، أو صدّره من البرنامج كـ PDF، أو استخدم Excel/CSV."
        )
    raw = df.copy()
    df = _prepare_dataframe_for_excel_export(df)
    # إن أزالت إزالة الترويسة كل الصفوف، نصدّر الجدول الخام كما استُخرج (أفضل من فشل كامل).
    if df is None or df.empty:
        df = raw

    df = _reframe_ledger_columns_clean(df)

    df = _apply_arabic_for_excel_export(df)

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="converted")
        ws = writer.book["converted"]
        ws.sheet_view.rightToLeft = True
        _format_converted_excel_sheet(ws)
    return out.getvalue()


def _format_converted_excel_sheet(ws: Any) -> None:
    """عرض أوضح: تفاف نص، عرض أعمدة يمنع قص التواريخ/العربي في الواجهة."""
    from openpyxl.styles import Alignment
    from openpyxl.utils import get_column_letter

    body = Alignment(wrap_text=True, vertical="top")
    header = Alignment(wrap_text=True, vertical="center", horizontal="center")

    for cell in ws[1]:
        cell.alignment = header
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            cell.alignment = body

    max_scan = min(ws.max_row, 800)
    for c in range(1, ws.max_column + 1):
        max_len = 14
        for r in range(1, max_scan + 1):
            val = ws.cell(row=r, column=c).value
            if val is None:
                continue
            s = str(val)
            max_len = max(max_len, min(len(s), 120))
        # عربي يحتاج عرضاً أكبر من عدد الحروف؛ حد أعلى معقول لـ Excel
        width = min(max(max_len * 1.12 + 2, 14), 78)
        ws.column_dimensions[get_column_letter(c)].width = width


def _apply_arabic_for_excel_export(df: pd.DataFrame) -> pd.DataFrame:
    """يُطبَّق على الأعمدة والخلايا النصية فقط؛ الأرقام تبقى أرقاماً."""
    out = df.copy()
    out.columns = [_excel_arabic_display(str(c)) for c in out.columns]
    for col in out.columns:
        out[col] = out[col].map(lambda x: _excel_arabic_display(x) if isinstance(x, str) else x)
    return out


def pdf_file_to_excel_bytes(pdf_path: str | Path) -> bytes:
    """للاختبار أو الاستدعاء من مسارات ملف."""
    return pdf_to_excel_bytes(str(pdf_path))
