from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from openpyxl import load_workbook
from copy import copy

_FALLBACK_TEMPLATE_COLUMNS = [
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


def _resolve_template_path(base_dir: Path) -> Path:
    candidates = [
        Path(__file__).resolve().parent / "templates" / "Salla Products Template (3).xlsx",
        Path(__file__).resolve().parent / "templates" / "Salla Products Template.xlsx",
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
    return None


def export_to_salla_template(products: List[Dict[str, Any]], template_path: Path | None, output_path: Path) -> Path:
    if not template_path:
        raise ValueError("تعذر العثور على ملف قالب سلة الأصلي. يرجى وضعه داخل importer/templates")

    wb = load_workbook(template_path)
    ws = wb.active
    header_map: Dict[str, int] = {}
    for col in range(1, ws.max_column + 1):
        name = ws.cell(row=1, column=col).value
        key = str(name).strip() if name is not None else ""
        if key:
            header_map[key] = col

    rows: List[Dict[str, Any]] = []
    for p in products:
        if not str(p.get("brand", "")).strip():
            continue
        if not str(p.get("size", "")).strip():
            continue
        if not str(p.get("price", "")).strip():
            continue
        public_image_url = str(p.get("image_cloudinary") or "").strip()
        if not public_image_url.startswith("https://res.cloudinary.com/"):
            continue

        promo_bits = []
        if p.get("year"):
            promo_bits.append(f"سنة الصنع {p.get('year')}")
        if p.get("warranty"):
            promo_bits.append(f"الضمان {p.get('warranty')}")
        promo_title = " - ".join(promo_bits)
        rows.append(
            {
                "النوع ": "منتج",
                "أسم المنتج": p.get("product_title", ""),
                "تصنيف المنتج": "قسم الإطارات",
                "صورة المنتج": public_image_url,
                "وصف صورة المنتج": p.get("image_alt_text", ""),
                "نوع المنتج": "منتج جاهز",
                "سعر المنتج": p.get("price", ""),
                "الوصف": p.get("description", ""),
                "هل يتطلب شحن؟": "نعم",
                "الوزن": 25,
                "وحدة الوزن": "kg",
                "الماركة": p.get("brand", ""),
                "العنوان الترويجي": promo_title,
                "تثبيت المنتج": "لا",
                "خاضع للضريبة ؟": "نعم",
                "[1] الاسم": "مقاس الإطار",
                "[1] النوع": "نص",
                "[1] القيمة": p.get("size", ""),
                "[2] الاسم": "",
                "[2] النوع": "",
                "[2] القيمة": "",
                "[2] الصورة / اللون": "",
                "[3] الاسم": "",
                "[3] النوع": "",
                "[3] القيمة": "",
                "[3] الصورة / اللون": "",
            }
        )

    start_row = 2
    # Preserve template styling for newly added rows by cloning row 2 styles.
    template_style_row = 2 if ws.max_row >= 2 else 1
    base_height = ws.row_dimensions[template_style_row].height
    base_styles = {c: copy(ws.cell(row=template_style_row, column=c)._style) for c in range(1, ws.max_column + 1)}
    base_num_formats = {c: ws.cell(row=template_style_row, column=c).number_format for c in range(1, ws.max_column + 1)}
    base_alignments = {c: copy(ws.cell(row=template_style_row, column=c).alignment) for c in range(1, ws.max_column + 1)}
    base_fills = {c: copy(ws.cell(row=template_style_row, column=c).fill) for c in range(1, ws.max_column + 1)}
    base_fonts = {c: copy(ws.cell(row=template_style_row, column=c).font) for c in range(1, ws.max_column + 1)}
    base_borders = {c: copy(ws.cell(row=template_style_row, column=c).border) for c in range(1, ws.max_column + 1)}
    max_clear_row = max(ws.max_row, start_row + len(rows) - 1)
    for r in range(start_row, max_clear_row + 1):
        if base_height is not None:
            ws.row_dimensions[r].height = base_height
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(row=r, column=c)
            cell._style = copy(base_styles[c])
            cell.number_format = base_num_formats[c]
            cell.alignment = copy(base_alignments[c])
            cell.fill = copy(base_fills[c])
            cell.font = copy(base_fonts[c])
            cell.border = copy(base_borders[c])
            ws.cell(row=r, column=c).value = None

    for idx, row_data in enumerate(rows, start=start_row):
        for key, value in row_data.items():
            col = header_map.get(key)
            if col:
                ws.cell(row=idx, column=col).value = value

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
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

