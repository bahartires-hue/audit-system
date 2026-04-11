from __future__ import annotations

import io
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

from .analyzer import (
    _normalize_arabic_digits,
    _promote_ledger_header_row,
    detect_columns,
    read_pdf,
    safe,
)


def _has_arabic_script(s: str) -> bool:
    for c in s:
        o = ord(c)
        if 0x0600 <= o <= 0x06FF or 0xFB50 <= o <= 0xFDFF or 0xFE70 <= o <= 0xFEFF:
            return True
    return False


def _excel_arabic_display(value: Any) -> Any:
    """عرض عربي صحيح في Excel: NFKC + ربط الحروف + اتجاه ثنائي (مثل تصدير PDF)."""
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
        import arabic_reshaper
        from bidi.algorithm import get_display

        t = unicodedata.normalize("NFKC", s)
        return get_display(arabic_reshaper.reshape(t))
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
