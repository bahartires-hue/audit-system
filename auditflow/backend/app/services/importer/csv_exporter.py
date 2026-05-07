from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def safe_set(row: Dict[str, Any], columns: List[str], col_name: str, value: Any) -> None:
    if col_name in columns:
        row[col_name] = value if value is not None else ""


def _load_template_df(template_path: Path) -> pd.DataFrame:
    if not template_path.exists():
        raise ValueError(f"قالب سلة غير موجود: {template_path}")
    template_df = pd.read_excel(template_path)
    cols = [str(c).strip() for c in template_df.columns]
    if "أسم المنتج" in cols and "سعر المنتج" in cols and "الوصف" in cols:
        template_df.columns = cols
        return template_df
    raw = pd.read_excel(template_path, header=None)
    header_idx = None
    for i in range(min(len(raw), 20)):
        vals = [str(x).strip() for x in raw.iloc[i].tolist() if str(x).strip() and str(x) != "nan"]
        if "أسم المنتج" in vals and "سعر المنتج" in vals and "الوصف" in vals:
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("تعذر تحديد صف الأعمدة داخل قالب سلة")
    cols = [str(x).strip() if str(x) != "nan" else f"Unnamed: {idx}" for idx, x in enumerate(raw.iloc[header_idx].tolist())]
    return pd.DataFrame(columns=cols)


def export_to_salla_template(products: List[Dict[str, Any]], template_path: Path, output_path: Path) -> Path:
    template_df = _load_template_df(template_path)
    columns = [str(c) for c in template_df.columns]
    output_rows: List[Dict[str, Any]] = []
    for p in products:
        row = {col: "" for col in columns}
        promo_bits = []
        if p.get("year"):
            promo_bits.append(f"سنة الصنع {p.get('year')}")
        if p.get("warranty"):
            promo_bits.append(f"الضمان {p.get('warranty')}")
        promo = " - ".join(promo_bits)
        image_value = p.get("image_local") or p.get("image_url") or ""

        safe_set(row, columns, "النوع ", "منتج")
        safe_set(row, columns, "أسم المنتج", p.get("product_title", ""))
        safe_set(row, columns, "تصنيف المنتج", "قسم الإطارات")
        safe_set(row, columns, "صورة المنتج", image_value)
        safe_set(row, columns, "وصف صورة المنتج", p.get("image_alt_text", ""))
        safe_set(row, columns, "نوع المنتج", "منتج جاهز")
        safe_set(row, columns, "سعر المنتج", p.get("price", ""))
        safe_set(row, columns, "الوصف", p.get("description", ""))
        safe_set(row, columns, "هل يتطلب شحن؟", "نعم")
        safe_set(row, columns, "الوزن", 25)
        safe_set(row, columns, "وحدة الوزن", "kg")
        safe_set(row, columns, "الماركة", p.get("brand", ""))
        safe_set(row, columns, "العنوان الترويجي", promo)
        safe_set(row, columns, "تثبيت المنتج", "لا")
        safe_set(row, columns, "خاضع للضريبة ؟", "نعم")

        safe_set(row, columns, "[1] الاسم", "Tire Size")
        safe_set(row, columns, "[1] النوع", "نص")
        safe_set(row, columns, "[1] القيمة", p.get("size", ""))
        safe_set(row, columns, "[2] الاسم", "Load / Speed")
        safe_set(row, columns, "[2] النوع", "نص")
        safe_set(row, columns, "[2] القيمة", p.get("load_speed", ""))
        safe_set(row, columns, "[3] الاسم", "Width")
        safe_set(row, columns, "[3] النوع", "نص")
        safe_set(row, columns, "[3] القيمة", p.get("width", ""))
        safe_set(row, columns, "[4] الاسم", "Rim")
        safe_set(row, columns, "[4] النوع", "نص")
        safe_set(row, columns, "[4] القيمة", p.get("rim", ""))
        output_rows.append(row)

    output_df = pd.DataFrame(output_rows, columns=columns)
    if list(output_df.columns) != list(template_df.columns):
        raise ValueError("أعمدة ملف سلة غير متطابقة مع القالب الأصلي")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_excel(output_path, index=False)
    return output_path


def _resolve_salla_template_path(output_dir: Path) -> Path:
    candidates = [
        output_dir / "Salla Products Template.xlsx",
        output_dir.parent / "Salla Products Template.xlsx",
        output_dir.parent.parent / "Salla Products Template.xlsx",
        output_dir / "Salla Products Template (3).xlsx",
        output_dir.parent / "Salla Products Template (3).xlsx",
        output_dir.parent.parent / "Salla Products Template (3).xlsx",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise ValueError("تعذر العثور على ملف القالب Salla Products Template.xlsx داخل المشروع")


def export_products_files(products: List[Dict[str, Any]], csv_path: Path, xlsx_path: Path) -> Dict[str, str]:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(products)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    salla_xlsx_path = xlsx_path.parent / "salla_products_ready.xlsx"
    template_path = _resolve_salla_template_path(xlsx_path.parent)
    out_path = export_to_salla_template(products, template_path, salla_xlsx_path)
    return {
        "csv_path": str(csv_path),
        "xlsx_path": str(xlsx_path),
        "salla_csv_path": "",
        "salla_xlsx_path": str(out_path),
    }

