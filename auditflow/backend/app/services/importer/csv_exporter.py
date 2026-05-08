from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def safe_set(row: Dict[str, Any], columns: List[str], col_name: str, value: Any) -> None:
    if col_name in columns:
        row[col_name] = value if value is not None else ""


def _resolve_template_path(base_dir: Path) -> Path:
    candidates = [
        base_dir / "Salla Products Template.xlsx",
        base_dir.parent / "Salla Products Template.xlsx",
        base_dir.parent.parent / "Salla Products Template.xlsx",
        base_dir / "Salla Products Template (3).xlsx",
        base_dir.parent / "Salla Products Template (3).xlsx",
        base_dir.parent.parent / "Salla Products Template (3).xlsx",
    ]
    for p in candidates:
        if p.exists() and p.is_file():
            return p
    raise ValueError("تعذر العثور على ملف القالب Salla Products Template.xlsx داخل المشروع")


def export_to_salla_template(products: List[Dict[str, Any]], template_path: Path, output_path: Path) -> Path:
    template_df = pd.read_excel(template_path)
    columns = list(template_df.columns)
    rows: List[Dict[str, Any]] = []

    for p in products:
        row = {col: "" for col in columns}
        promo_bits = []
        if p.get("year"):
            promo_bits.append(f"سنة الصنع {p.get('year')}")
        if p.get("warranty"):
            promo_bits.append(f"الضمان {p.get('warranty')}")
        promo_title = " - ".join(promo_bits)
        public_image_url = str(p.get("image_cloudinary") or "").strip()
        if not public_image_url.startswith("https://res.cloudinary.com/"):
            public_image_url = ""

        safe_set(row, columns, "النوع ", "منتج")
        safe_set(row, columns, "أسم المنتج", p.get("product_title", ""))
        safe_set(row, columns, "تصنيف المنتج", "قسم الإطارات")
        safe_set(row, columns, "صورة المنتج", public_image_url)
        safe_set(row, columns, "وصف صورة المنتج", p.get("image_alt_text", ""))
        safe_set(row, columns, "نوع المنتج", "منتج جاهز")
        safe_set(row, columns, "سعر المنتج", p.get("price", ""))
        safe_set(row, columns, "الوصف", p.get("description", ""))
        safe_set(row, columns, "هل يتطلب شحن؟", "نعم")
        safe_set(row, columns, "الوزن", 25)
        safe_set(row, columns, "وحدة الوزن", "kg")
        safe_set(row, columns, "الماركة", p.get("brand", ""))
        safe_set(row, columns, "العنوان الترويجي", promo_title)
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

        rows.append(row)

    output_df = pd.DataFrame(rows, columns=columns)
    assert list(output_df.columns) == list(template_df.columns), "أعمدة ملف سلة غير متطابقة مع القالب الأصلي"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_excel(output_path, index=False)
    return output_path


def export_products_files(products: List[Dict[str, Any]], csv_path: Path, xlsx_path: Path) -> Dict[str, str]:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(products)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    salla_xlsx_path = xlsx_path.parent / "salla_products_ready.xlsx"
    template_path = _resolve_template_path(xlsx_path.parent)
    out_path = export_to_salla_template(products, template_path, salla_xlsx_path)
    return {
        "csv_path": str(csv_path),
        "xlsx_path": str(xlsx_path),
        "salla_csv_path": "",
        "salla_xlsx_path": str(out_path),
    }

