from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


_SALLA_COLUMNS = [
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


def _to_salla_row(p: Dict[str, Any]) -> Dict[str, Any]:
    promo = []
    if p.get("warranty"):
        promo.append(f"الضمان {p.get('warranty')}")
    if p.get("year"):
        promo.append(f"سنة الصنع {p.get('year')}")
    promo_text = " - ".join(promo) if promo else "إطار سيارات بجودة عالية"
    image_value = (p.get("image_url") or "").strip()
    if p.get("image_local"):
        image_value = p.get("image_local")
    return {
        "النوع ": "منتج",
        "أسم المنتج": p.get("product_title") or p.get("name") or "",
        "تصنيف المنتج": "قسم الإطارات",
        "صورة المنتج": image_value,
        "وصف صورة المنتج": p.get("image_alt_text", ""),
        "نوع المنتج": "منتج جاهز",
        "سعر المنتج": p.get("price", ""),
        "الوصف": p.get("description", ""),
        "هل يتطلب شحن؟": "نعم",
        "رمز المنتج sku": "",
        "سعر التكلفة": "",
        "السعر المخفض": "",
        "تاريخ بداية التخفيض": "",
        "تاريخ نهاية التخفيض": "",
        "اقصي كمية لكل عميل": "",
        "إخفاء خيار تحديد الكمية": "",
        "اضافة صورة عند الطلب": "",
        "الوزن": 25,
        "وحدة الوزن": "kg",
        "الماركة": p.get("brand", ""),
        "العنوان الترويجي": promo_text,
        "تثبيت المنتج": "لا",
        "الباركود": "",
        "السعرات الحرارية": "",
        "MPN": "",
        "GTIN": "",
        "خاضع للضريبة ؟": "نعم",
        "سبب عدم الخضوع للضريبة": "",
        "[1] الاسم": "مقاس الإطار",
        "[1] النوع": "نص",
        "[1] القيمة": p.get("size", ""),
        "[1] الصورة / اللون": "",
        "[2] الاسم": "مؤشر الحمولة والسرعة",
        "[2] النوع": "نص",
        "[2] القيمة": p.get("load_speed", ""),
        "[2] الصورة / اللون": "",
        "[3] الاسم": "XL",
        "[3] النوع": "نص",
        "[3] القيمة": "نعم" if p.get("xl") else "لا",
        "[3] الصورة / اللون": "",
    }


def export_products_files(products: List[Dict[str, Any]], csv_path: Path, xlsx_path: Path) -> Dict[str, str]:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(products)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    salla_csv_path = csv_path.parent / "salla_products_ready.csv"
    salla_xlsx_path = xlsx_path.parent / "salla_products_ready.xlsx"
    salla_df = pd.DataFrame([_to_salla_row(p) for p in products], columns=_SALLA_COLUMNS)
    salla_df.to_csv(salla_csv_path, index=False, encoding="utf-8-sig")
    salla_df.to_excel(salla_xlsx_path, index=False)
    return {
        "csv_path": str(csv_path),
        "xlsx_path": str(xlsx_path),
        "salla_csv_path": str(salla_csv_path),
        "salla_xlsx_path": str(salla_xlsx_path),
    }

