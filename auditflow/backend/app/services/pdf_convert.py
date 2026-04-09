from __future__ import annotations

import io

import pandas as pd

from .analyzer import read_pdf


def pdf_to_excel_bytes(file_path: str) -> bytes:
    df = read_pdf(file_path)
    if df is None or df.empty:
        raise ValueError("تعذر استخراج بيانات من PDF. تأكد أن الملف نصي وفيه جدول واضح.")
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="converted")
    return out.getvalue()
