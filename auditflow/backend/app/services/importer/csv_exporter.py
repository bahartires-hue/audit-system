from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

_DEFAULT_SALLA_COLUMNS = [
    "النوع ",
    "أسم المنتج",
    "تصنيف المنتج",
    "صورة المنتج",
    "وصف صورة المنتج",
    "نوع المنتج",
    "سعر المنتج",
    "الوصف",
    "هل يتطلب شحن؟",
    "رمز المنتج sku",
    "سعر التكلفة",
    "السعر المخفض",
    "تاريخ بداية التخفيض",
    "تاريخ نهاية التخفيض",
    "اقصي كمية لكل عميل",
    "إخفاء خيار تحديد الكمية",
    "اضافة صورة عند الطلب",
    "الوزن",
    "وحدة الوزن",
    "الماركة",
    "العنوان الترويجي",
    "تثبيت المنتج",
    "الباركود",
    "السعرات الحرارية",
    "MPN",
    "GTIN",
    "خاضع للضريبة ؟",
    "سبب عدم الخضوع للضريبة",
    "[1] الاسم",
    "[1] النوع",
    "[1] القيمة",
    "[1] الصورة / اللون",
    "[2] الاسم",
    "[2] النوع",
    "[2] القيمة",
    "[2] الصورة / اللون",
    "[3] الاسم",
    "[3] النوع",
    "[3] القيمة",
    "[3] الصورة / اللون",
]

def safe_set(row: Dict[str, Any], columns: List[str], col_name: str, value: Any) -> None:
    if col_name in columns:
        row[col_name] = value if value is not None else ""


def build_public_image_url(filename: str) -> str:
    base = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    safe = (filename or "").strip().replace("\\", "/").split("/")[-1]
    if not safe:
        return ""
    if base:
        return f"{base}/uploads/products/{safe}"
    return ""


def _to_public_image_value(p: Dict[str, Any]) -> str:
    local = str(p.get("image_local") or "").strip()
    if local:
        lower_local = local.lower()
        if lower_local.startswith("/opt/render/") or lower_local.startswith("c:\\") or lower_local.startswith("d:\\"):
            return build_public_image_url(Path(local).name)
        if lower_local.startswith("http://") or lower_local.startswith("https://"):
            return local
        return build_public_image_url(Path(local).name)
    src = str(p.get("image_url") or "").strip()
    if src.lower().startswith("https://"):
        return src
    source_src = str(p.get("source_image_url") or "").strip()
    if source_src.lower().startswith("https://"):
        return source_src
    return ""


def export_to_salla_template(products: List[Dict[str, Any]], template_path: Path | None, output_path: Path) -> Path:
    # external template is intentionally ignored to avoid any runtime dependency.
    template_df = pd.DataFrame(columns=_DEFAULT_SALLA_COLUMNS)
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
        image_value = _to_public_image_value(p)

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
        raise ValueError("أعمدة ملف سلة المدمجة غير متطابقة")
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
    out_path = export_to_salla_template(products, None, salla_xlsx_path)
    return {
        "csv_path": str(csv_path),
        "xlsx_path": str(xlsx_path),
        "salla_csv_path": "",
        "salla_xlsx_path": str(out_path),
    }

