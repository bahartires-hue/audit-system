from __future__ import annotations

import csv
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import unquote, urlparse

from .ai_rewriter import rewrite_description_fallback
from .cloudinary_uploader import upload_to_cloudinary
from .csv_exporter import export_products_files
from .image_downloader import download_image
from .parser import normalize_brand_name, parse_tire_name
from .scraper import scrape_products
from .seo_optimizer import build_seo_fields

log = logging.getLogger("importer.pipeline")


def _report_progress(cb: Optional[Callable[[int, str], None]], pct: int, message: str) -> None:
    if not cb:
        return
    p = max(1, min(99, int(pct)))
    cb(p, message)


def _fetch_images_for_unit(unit: Dict[str, Any]) -> Dict[str, Any]:
    """Download + Cloudinary for one product (thread-safe paths per slug)."""
    image_dir = Path(unit["image_dir"])
    item = unit["item"]
    seo = unit["seo"]
    local_image, image_status = download_image(item.get("image_url", ""), image_dir, seo["image_slug"])
    cloud_url = ""
    cloud_status = ""
    if local_image:
        cloud_url, cloud_status = upload_to_cloudinary(local_image, seo["image_slug"])
    elif (item.get("image_url") or "").startswith("http"):
        cloud_url, cloud_status = upload_to_cloudinary(item.get("image_url", ""), seo["image_slug"])
    if not cloud_url:
        image_status = "needs_review"
    return {
        **unit,
        "local_image": local_image,
        "image_status": image_status,
        "cloud_url": cloud_url,
        "cloud_status": cloud_status,
    }


def _norm_brand(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _norm_size(s: str) -> str:
    v = (s or "").upper().replace(" ", "")
    v = v.replace("ZR", "R")
    return v


def _extract_size_from_text(s: str) -> str:
    m = re.search(r"(\d{3})\s*/\s*(\d{2,3})\s*Z?R\s*(\d{2})", (s or ""), flags=re.IGNORECASE)
    if not m:
        return ""
    return _norm_size(f"{m.group(1)}/{m.group(2)}R{m.group(3)}")


def _normalize_price(raw: str) -> str:
    t = re.sub(r"[^\d,\.]", "", str(raw or "").strip())
    if not t:
        return ""
    if "," in t and "." in t:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
    else:
        if t.count(",") == 1 and t.count(".") == 0:
            t = t.replace(",", ".")
        elif t.count(",") > 1 and t.count(".") == 0:
            t = t.replace(",", "")
        elif t.count(".") > 1 and t.count(",") == 0:
            t = t.replace(".", "")
    try:
        v = float(t)
    except Exception:
        return ""
    if v <= 0:
        return ""
    return f"{v:.2f}".rstrip("0").rstrip(".")


def _infer_brand_from_name(name: str) -> str:
    s = str(name or "").strip()
    if not s:
        return ""
    low = s.lower()
    if "laufenn" in low or "لاوفين" in low:
        return "Laufenn"
    if "alpha" in low or "ألفا" in low or "الفا" in low:
        return "Alpha"
    first = s.split()[0] if s.split() else ""
    return normalize_brand_name(first)


def _normalize_brand_strict(value: str) -> str:
    v = normalize_brand_name(str(value or "").strip())
    low = v.lower()
    if low in {"ألفا", "الفا", "alpha"}:
        return "alpha"
    if low in {"لاوفين", "laufenn"}:
        return "laufenn"
    return low


def _infer_brand_from_url(site_url: str) -> str:
    path = unquote((urlparse(site_url).path or "").strip("/")).lower()
    if not path:
        return ""
    tokens = [t for t in re.split(r"[/\-_]+", path) if t]
    for token in tokens:
        candidate = _normalize_brand_strict(token)
        if candidate in {"alpha", "laufenn"}:
            return candidate
    return ""


def filter_products(products: List[Dict[str, Any]], brand: str = "", size: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    b_raw = (brand or "").strip()
    b_norm = normalize_brand_name(b_raw) if b_raw else ""
    b = _norm_brand(b_norm or b_raw)
    sz = _norm_size(size)
    out: List[Dict[str, Any]] = []
    cap = limit if int(limit or 0) > 0 else 10**9
    for item in products:
        if len(out) >= cap:
            break
        parsed = item.get("_parsed") or {}
        p_brand = _normalize_brand_strict(parsed.get("brand", ""))
        p_size = _norm_size(parsed.get("size", ""))
        if b and p_brand != _normalize_brand_strict(b):
            continue
        if sz and not p_size:
            p_size = _extract_size_from_text(item.get("name", ""))
        if sz and sz not in p_size:
            continue
        out.append(item)
    return out


def run_import_pipeline(
    site_url: str,
    uploads_root: Path,
    *,
    brand: str = "",
    size: str = "",
    limit: int = 20,
    max_pages: int = 10,
    multi_pages: bool = True,
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> Dict[str, Any]:
    selected_brand = _normalize_brand_strict(brand) if brand else _infer_brand_from_url(site_url)
    if not selected_brand:
        raise ValueError("تعذر تحديد الماركة. أدخل brand أو استخدم رابط قسم ماركة واضح.")

    _report_progress(progress_cb, 2, "جاري جلب صفحات المنتجات...")
    raw_items = scrape_products(site_url, multi_pages=multi_pages, max_pages=max_pages, limit=0)
    _report_progress(progress_cb, 16, f"تم جلب {len(raw_items)} عنصر من الموقع")
    products: List[Dict[str, Any]] = []
    seen = set()
    image_dir = uploads_root / "products"
    exports_dir = uploads_root.parent / "exports"
    prepared: List[Dict[str, Any]] = []

    for item in raw_items:
        parsed = parse_tire_name(item.get("name") or "")
        prepared.append({**item, "_parsed": parsed})

    scoped_items = filter_products(prepared, brand=selected_brand, size=size, limit=limit)
    if not scoped_items and prepared and not (brand or size):
        # avoid hard-zero output when strict filter input is mismatched
        scoped_items = prepared[: max(1, limit)]
        log.warning(
            "importer relaxed filters because scoped=0 raw=%s brand=%s size=%s",
            len(prepared),
            brand,
            size,
        )
    log.info(
        "importer filter applied raw=%s scoped=%s brand=%s size=%s limit=%s multi_pages=%s",
        len(raw_items),
        len(scoped_items),
        selected_brand,
        size,
        limit,
        multi_pages,
    )
    _report_progress(progress_cb, 22, f"بعد الفلترة: {len(scoped_items)} منتج للمعالجة")

    work_units: List[Dict[str, Any]] = []
    image_dir_str = str(image_dir.resolve())

    for item in scoped_items:
        parsed = item.get("_parsed") or parse_tire_name(item.get("name") or "")
        raw_name = str(item.get("name", "")).strip()
        if raw_name == "ابحث !":
            continue
        if not parsed.get("brand") and re.search(r"\bAL[A-Z0-9\-]*\b", str(item.get("sku", "")), flags=re.IGNORECASE):
            parsed["brand"] = "Alpha"
        if not parsed.get("brand") and re.search(r"alpha|ألفا|الفا", f"{raw_name} {item.get('product_url','')}", flags=re.IGNORECASE):
            parsed["brand"] = "Alpha"
        if re.search(r"alpha|ألفا|الفا", f"{parsed.get('brand','')} {raw_name} {item.get('product_url','')}", flags=re.IGNORECASE):
            parsed["brand"] = "Alpha"
        if re.search(r"لاوفين|laufenn", f"{parsed.get('brand','')} {raw_name}", flags=re.IGNORECASE):
            parsed["brand"] = "Laufenn"
        if parsed.get("parse_status") == "non_english_name" or re.search(r"[\u0600-\u06FF]", f"{parsed.get('brand','')} {parsed.get('model','')}"):
            # final fallback: keep product but clear non-English fragments
            parsed["brand"] = re.sub(r"[\u0600-\u06FF]+", "", parsed.get("brand", "")).strip() or parsed.get("brand", "")
            parsed["model"] = re.sub(r"[\u0600-\u06FF]+", "", parsed.get("model", "")).strip() or "Standard"
            parsed["parse_status"] = "ok" if parsed.get("size") else "size_missing"
        seo = build_seo_fields(parsed)
        key = (
            parsed.get("brand", "").lower(),
            parsed.get("model", "").lower(),
            parsed.get("size", "").lower(),
            parsed.get("load_speed", "").lower(),
        )
        if key in seen:
            continue
        seen.add(key)

        price = _normalize_price(item.get("price", ""))
        if raw_name == "ابحث !" or not parsed.get("size") or not price:
            continue
        if selected_brand:
            product_brand = _normalize_brand_strict(parsed.get("brand", "") or _infer_brand_from_name(raw_name))
            if selected_brand and product_brand and product_brand != selected_brand:
                log.info(
                    "SKIPPED_WRONG_BRAND selected_brand=%s product_brand=%s name=%s",
                    selected_brand,
                    product_brand,
                    raw_name,
                )
                continue
            log.info(
                "ACCEPTED selected_brand=%s product_brand=%s name=%s",
                selected_brand,
                product_brand,
                raw_name,
            )
        work_units.append({"item": item, "parsed": parsed, "seo": seo, "price": price, "raw_name": raw_name, "image_dir": image_dir_str})

    enriched_units: List[Dict[str, Any]] = []
    n_units = len(work_units)
    if n_units == 0:
        _report_progress(progress_cb, 88, "لا توجد منتجات بعد الفلترة")
    else:
        cpu = os.cpu_count() or 4
        max_workers = min(16, max(6, cpu * 2), n_units)
        span_lo, span_hi = 28, 86
        done = 0
        _report_progress(progress_cb, span_lo, f"تحميل الصور والرفع ({n_units} منتج)...")
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            fut_to_idx = {pool.submit(_fetch_images_for_unit, u): i for i, u in enumerate(work_units)}
            enriched_units = [None] * n_units  # type: ignore[list-assignment]
            for fut in as_completed(fut_to_idx):
                idx = fut_to_idx[fut]
                enriched_units[idx] = fut.result()
                done += 1
                if progress_cb and n_units:
                    pct = span_lo + int((done / n_units) * (span_hi - span_lo))
                    _report_progress(progress_cb, pct, f"صور {done}/{n_units}")

    _report_progress(progress_cb, 88, "بناء الأوصاف والصفوف...")
    for unit in enriched_units:
        if not unit:
            continue
        item = unit["item"]
        parsed = unit["parsed"]
        seo = unit["seo"]
        price = unit["price"]
        raw_name = unit["raw_name"]
        local_image = unit.get("local_image", "")
        image_status = unit.get("image_status", "")
        cloud_url = unit.get("cloud_url", "")
        cloud_status = unit.get("cloud_status", "")

        price_status = "ok" if price else "price_missing"
        seo_status = "ok" if parsed.get("size") else "needs_review"
        row = {
            "name": item.get("name", ""),
            "product_title": " ".join(
                x for x in [normalize_brand_name(parsed.get("brand", "")), parsed.get("model", ""), parsed.get("size", ""), parsed.get("load_speed", "")]
                if str(x).strip()
            ).strip(),
            "brand": parsed["brand"],
            "model": parsed["model"],
            "size": parsed["size"],
            "width": parsed["width"],
            "profile": parsed["profile"],
            "rim": parsed["rim"],
            "load_speed": parsed["load_speed"],
            "xl": parsed["xl"],
            "price": price,
            "old_price": item.get("old_price", ""),
            "product_url": item.get("product_url", ""),
            "source_image_url": item.get("image_url", ""),
            "image_url": local_image,
            "image_local": local_image,
            "image_cloudinary": cloud_url,
            "cloudinary_status": cloud_status,
            "year": item.get("year", ""),
            "warranty": item.get("warranty", ""),
            "country": item.get("country", ""),
            "pattern": item.get("pattern", ""),
            "description": rewrite_description_fallback(
                {
                    **parsed,
                    "year": item.get("year", ""),
                    "warranty": item.get("warranty", ""),
                    "country": item.get("country", ""),
                    "pattern": item.get("pattern", ""),
                },
                source_description=item.get("description", ""),
            ),
            "parse_status": parsed.get("parse_status", ""),
            "seo_title": seo["seo_title"],
            "meta_description": seo["meta_description"],
            "keywords": seo["keywords"],
            "image_alt_text": seo["image_alt_text"],
            "image_status": image_status,
            "seo_status": seo_status,
            "price_status": price_status,
            "status": (
                "مراجعة"
                if image_status in {"failed", "needs_review"} or seo_status != "ok" or price_status != "ok"
                else "جاهز"
            ),
        }
        if image_status in {"failed", "no_image_url"} and (item.get("image_url") or "").startswith("https://"):
            row["status"] = "مراجعة"
        products.append(row)

    if selected_brand:
        for product in products:
            product_brand = _normalize_brand_strict(product.get("brand", ""))
            if selected_brand and product_brand != selected_brand:
                log.error(
                    "wrong brand detected before export selected_brand=%s product_brand=%s name=%s",
                    selected_brand,
                    product_brand,
                    product.get("name", ""),
                )
                raise ValueError("Wrong brand detected before export")

    _report_progress(progress_cb, 93, "تصدير CSV و Excel...")
    csv_path = exports_dir / "tire_products.csv"
    xlsx_path = exports_dir / "tire_products.xlsx"
    exports = export_products_files(products, csv_path, xlsx_path)

    # Export image failures for later retry/review.
    failed_images_path = exports_dir / "failed_images.csv"
    failed_rows = [
        {
            "name": p.get("name", ""),
            "brand": p.get("brand", ""),
            "size": p.get("size", ""),
            "image_status": p.get("image_status", ""),
            "cloudinary_status": p.get("cloudinary_status", ""),
            "source_image_url": p.get("source_image_url", ""),
            "product_url": p.get("product_url", ""),
        }
        for p in products
        if str(p.get("image_status", "")).strip().lower() != "ok"
    ]
    failed_images_path.parent.mkdir(parents=True, exist_ok=True)
    with failed_images_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["name", "brand", "size", "image_status", "cloudinary_status", "source_image_url", "product_url"],
        )
        writer.writeheader()
        writer.writerows(failed_rows)

    log.info("importer done count=%s csv=%s xlsx=%s", len(products), exports["csv_path"], exports["xlsx_path"])
    _report_progress(progress_cb, 99, "اكتمل التجهيز")
    return {
        "count": len(products),
        "csv_path": exports["csv_path"],
        "xlsx_path": exports["xlsx_path"],
        "salla_csv_path": exports["salla_csv_path"],
        "salla_xlsx_path": exports["salla_xlsx_path"],
        "failed_images_path": str(failed_images_path),
        "items": products,
    }

