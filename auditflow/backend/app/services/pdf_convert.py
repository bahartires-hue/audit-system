from __future__ import annotations

import io
import re
from pathlib import Path

import pandas as pd

from .analyzer import (
    _normalize_arabic_digits,
    _promote_ledger_header_row,
    detect_columns,
    read_pdf,
    safe,
)

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
        raise ValueError("تعذر استخراج بيانات من PDF. تأكد أن الملف نصي وفيه جدول واضح.")
    df = _prepare_dataframe_for_excel_export(df)
    if df is None or df.empty:
        raise ValueError("تعذر إنتاج Excel بعد إزالة الترويسة. جرّب ملف PDF أوضح.")
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="converted")
    return out.getvalue()


def pdf_file_to_excel_bytes(pdf_path: str | Path) -> bytes:
    """للاختبار أو الاستدعاء من مسارات ملف."""
    return pdf_to_excel_bytes(str(pdf_path))
