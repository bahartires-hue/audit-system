from __future__ import annotations

import csv
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import unquote, urlparse

from .ai_seo_engine import apply_bundle_to_row, build_json_ld_product, generate_seo_bundle
from .clean_seo_description import make_simple_description, normalize_pattern_display
from .cloudinary_uploader import upload_to_cloudinary
from .csv_exporter import export_products_files
from .image_downloader import download_image
from .parser import BRAND_TRANSLATIONS, hydrate_parsed_from_size_token, normalize_brand_name, parse_tire_name
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
    """استخراج slug الماركة من مسار URL (مثل /brands/michelin) لأي ماركة وليس alpha/laufenn فقط."""
    path = unquote((urlparse(site_url).path or "").strip("/")).lower()
    if not path:
        return ""
    tokens = [t for t in re.split(r"[/\-_]+", path) if t]
    skip_next = {"page", "search", "all", "list", "products", "items", "category", "tag", "id"}
    brandish = {"brands", "brand", "العلامات", "ماركة", "brandname"}
    for i, token in enumerate(tokens):
        if token in brandish and i + 1 < len(tokens):
            slug = tokens[i + 1]
            if slug and slug not in skip_next:
                return _normalize_brand_strict(slug.replace("-", " "))
    for token in tokens:
        candidate = _normalize_brand_strict(token.replace("-", " "))
        if candidate in {"alpha", "laufenn"}:
            return candidate
    # أقسام WooCommerce: /product-category/sailun-tires/ → استنتاج «sailun» من أول جزء في slug
    m_cat = re.match(r"^product-category/([^/]+)/?", path)
    if m_cat:
        slug_raw = (m_cat.group(1) or "").strip()
        if slug_raw and slug_raw not in skip_next:
            brand_guess = slug_raw.split("-")[0].strip()
            if brand_guess:
                return _normalize_brand_strict(brand_guess.replace("-", " "))
    return ""


def _display_brand_from_key(key: str) -> str:
    if not key:
        return ""
    return normalize_brand_name(str(key).replace("-", " ").strip())


_MANUFACTURER_COUNTRY_BY_BRAND = {
    "accelera": "إندونيسيا",
    "bridgestone": "اليابان",
    "continental": "ألمانيا",
    "goodyear": "الولايات المتحدة",
    "hankook": "كوريا الجنوبية",
    "kumho": "كوريا الجنوبية",
    "laufenn": "كوريا الجنوبية",
    "linglong": "الصين",
    "michelin": "فرنسا",
    "nexen": "كوريا الجنوبية",
    "pirelli": "إيطاليا",
    "sailun": "الصين",
    "triangle": "الصين",
    "yokohama": "اليابان",
}


def _infer_manufacturer_country(brand: str = "", raw_name: str = "") -> str:
    brand_key = _normalize_brand_strict(brand or _infer_brand_from_name(raw_name))
    return _MANUFACTURER_COUNTRY_BY_BRAND.get(brand_key, "")


def _resolve_country_value(item: Dict[str, Any], parsed: Dict[str, Any], raw_name: str) -> tuple[str, str]:
    """Prefer scraped country; otherwise infer manufacturer country only when year is present."""
    scraped = str(item.get("country", "") or "").strip()
    if scraped:
        return scraped, "scraped"
    year = str(item.get("year", "") or "").strip()
    if not year:
        return "", ""
    inferred = _infer_manufacturer_country(str(parsed.get("brand", "") or ""), raw_name)
    if inferred:
        return inferred, "brand_inferred"
    return "", ""


def _name_signals_brand(item: Dict[str, Any], want_key: str) -> bool:
    """هل اسم المنتج أو الرابط يوحي بالماركة المطلوبة (want_key بعد _normalize_brand_strict)."""
    if not want_key:
        return False
    blob = f"{item.get('name', '')} {item.get('product_url', '')} {item.get('sku', '')}"
    low = blob.lower()
    compact = re.sub(r"\s+", "", low)
    if want_key in compact or want_key in low:
        return True
    for ar, en in BRAND_TRANSLATIONS.items():
        if _normalize_brand_strict(en) != want_key:
            continue
        if ar in blob or en.lower() in low:
            return True
    return False


def filter_products(products: List[Dict[str, Any]], brand: str = "", size: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    b_raw = (brand or "").strip()
    b_norm = normalize_brand_name(b_raw) if b_raw else ""
    want_key = _normalize_brand_strict(b_norm or b_raw) if b_raw else ""
    sz = _norm_size(size)
    out: List[Dict[str, Any]] = []
    cap = limit if int(limit or 0) > 0 else 10**9
    for item in products:
        if len(out) >= cap:
            break
        parsed = item.get("_parsed") or {}
        p_brand = _normalize_brand_strict(parsed.get("brand", ""))
        p_size = _norm_size(parsed.get("size", ""))
        if want_key:
            if p_brand != want_key:
                if _name_signals_brand(item, want_key):
                    pass
                elif not p_brand:
                    pass
                else:
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
    user_brand = (brand or "").strip()
    user_size = (size or "").strip()
    full_catalog = int(limit or 0) <= 0
    # عند limit<=0 وبدون ماركة يدوية: لا نفلتر بماركة الصفحة حتى لا نُسقط منتجات يُفسَّر اسمها/ماركته خطأً.
    relaxed_brand_scope = full_catalog and not user_brand

    selected_brand = _normalize_brand_strict(user_brand) if user_brand else _infer_brand_from_url(site_url)
    if not selected_brand:
        raise ValueError("تعذر تحديد الماركة. أدخل brand أو استخدم رابط قسم ماركة واضح.")
    listing_brand_display = _display_brand_from_key(user_brand) if user_brand else _display_brand_from_key(selected_brand)

    # limit <= 0: سحب كامل للكتالوج (السقف الفعلي في scrape_tireex عبر AUDITFLOW_IMPORTER_MAX_ITEMS).
    # limit > 0: نجمع هامشاً أكبر من العدد المطلوب لأن الفلترة تقلل النتائج.
    if int(limit or 0) <= 0:
        scrape_cap = 0
    else:
        user_lim = max(1, int(limit))
        scrape_cap = min(max(user_lim * 4, user_lim + 200), 500_000)

    def on_scrape_progress(local_pct: int, msg: str) -> None:
        """مرحلة السحب من الموقع: 0–100 محلية → تقريباً 3–14 عالمياً."""
        lp = max(0, min(100, int(local_pct)))
        g = 3 + int(lp * 11 / 100)
        _report_progress(progress_cb, g, msg)

    _report_progress(progress_cb, 2, "جاري جلب صفحات المنتجات...")
    raw_items = scrape_products(
        site_url,
        multi_pages=multi_pages,
        max_pages=max_pages,
        limit=scrape_cap,
        progress_cb=on_scrape_progress if progress_cb else None,
    )
    _report_progress(progress_cb, 16, f"تم جلب {len(raw_items)} عنصر من الموقع")
    products: List[Dict[str, Any]] = []
    seen = set()
    image_dir = uploads_root / "products"
    exports_dir = uploads_root.parent / "exports"
    prepared: List[Dict[str, Any]] = []

    for item in raw_items:
        parsed = parse_tire_name(item.get("name") or "")
        parsed = hydrate_parsed_from_size_token(
            item.get("name") or "",
            item.get("_size_token"),
            parsed,
        )
        prepared.append({**item, "_parsed": parsed})

    scope_brand = "" if relaxed_brand_scope else (user_brand if user_brand else selected_brand)
    scoped_items = filter_products(prepared, brand=scope_brand, size=user_size, limit=limit)
    if not scoped_items and prepared and not (user_brand or user_size):
        # avoid hard-zero output when strict filter input is mismatched
        cap_slice = max(1, int(limit)) if int(limit or 0) > 0 else len(prepared)
        scoped_items = prepared[:cap_slice]
        log.warning(
            "importer relaxed filters because scoped=0 raw=%s brand=%s size=%s",
            len(prepared),
            user_brand,
            user_size,
        )
    log.info(
        "importer filter applied raw=%s scoped=%s brand=%s size=%s limit=%s multi_pages=%s user_brand=%s",
        len(raw_items),
        len(scoped_items),
        selected_brand,
        user_size,
        limit,
        multi_pages,
        user_brand,
    )
    if int(limit or 0) > 0 and len(raw_items) > len(scoped_items) * 3:
        log.warning(
            "importer limit=%s may truncate results (scoped=%s raw=%s); use limit=0 for full export",
            limit,
            len(scoped_items),
            len(raw_items),
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
        if not parsed.get("size"):
            sz_guess = _extract_size_from_text(raw_name)
            if sz_guess:
                m_sz = re.match(r"^(\d{3})/(\d{2,3})R(\d{2})$", sz_guess, flags=re.IGNORECASE)
                if m_sz:
                    parsed = {
                        **parsed,
                        "width": m_sz.group(1),
                        "profile": m_sz.group(2),
                        "rim": m_sz.group(3),
                        "size": sz_guess,
                    }
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
        if raw_name == "ابحث !":
            continue
        # في وضع الكتالوج الكامل نبقي المنتجات ذات المقاس غير القياسي للمراجعة
        # بدل حذفها نهائياً (مثل: 175 R13 C). في الأوضاع المقيّدة نظل أكثر صرامة.
        if not parsed.get("size") and not full_catalog:
            continue
        if not price:
            if not full_catalog:
                continue
            price = "0"
        if selected_brand and not relaxed_brand_scope:
            product_brand = _normalize_brand_strict(parsed.get("brand", "") or _infer_brand_from_name(raw_name))
            if product_brand and product_brand != selected_brand:
                if _name_signals_brand(item, selected_brand):
                    parsed["brand"] = listing_brand_display or parsed.get("brand", "")
                else:
                    log.info(
                        "SKIPPED_WRONG_BRAND selected_brand=%s product_brand=%s name=%s",
                        selected_brand,
                        product_brand,
                        raw_name,
                    )
                    continue
            elif not product_brand:
                parsed["brand"] = listing_brand_display or parsed.get("brand", "")
            log.info(
                "ACCEPTED selected_brand=%s product_brand=%s name=%s",
                selected_brand,
                _normalize_brand_strict(parsed.get("brand", "")),
                raw_name,
            )
        elif selected_brand and relaxed_brand_scope:
            pb = _normalize_brand_strict(parsed.get("brand", "") or _infer_brand_from_name(raw_name))
            if not pb:
                parsed["brand"] = listing_brand_display or parsed.get("brand", "") or "Tire"
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
    _seo_long_history: List[str] = []
    _canonical_base = (os.getenv("AUDITFLOW_STORE_PUBLIC_BASE") or os.getenv("PUBLIC_BASE_URL") or "").strip()

    for unit in enriched_units:
        if not unit:
            continue
        item = unit["item"]
        parsed = unit["parsed"]
        price = unit["price"]
        raw_name = unit["raw_name"]
        local_image = unit.get("local_image", "")
        image_status = unit.get("image_status", "")
        cloud_url = unit.get("cloud_url", "")
        cloud_status = unit.get("cloud_status", "")
        country_value, _country_source = _resolve_country_value(item, parsed, raw_name)

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
            "country": country_value,
            "pattern": item.get("pattern", ""),
            "traction": str(item.get("traction", "") or "").strip(),
            "temperature": str(item.get("temperature", "") or "").strip(),
            "treadwear": str(item.get("treadwear", "") or "").strip(),
            "parse_status": parsed.get("parse_status", ""),
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
        prod_for_seo = {
            **parsed,
            "year": item.get("year", ""),
            "warranty": item.get("warranty", ""),
            "country": country_value,
            "pattern": item.get("pattern", ""),
        }
        bundle = generate_seo_bundle(
            prod_for_seo,
            prior_long_samples=_seo_long_history,
            source_description="",
        )
        apply_bundle_to_row(row, bundle, _canonical_base, omit_body_copy=True)

        # وصف المنتج للسلة/التصدير: صيغة ثابتة فقط (بدون AI وبدون SEO إضافي وبدون «بحر الإطارات» داخل الوصف).
        simple_desc = {
            "brand": str(row.get("brand", "") or parsed.get("brand", "") or "").strip(),
            "size": str(row.get("size", "") or parsed.get("size", "") or "").strip(),
            "load_speed": str(row.get("load_speed", "") or parsed.get("load_speed", "") or "").strip(),
            "pattern": normalize_pattern_display(str(row.get("pattern", "") or item.get("pattern", "") or "")),
            "country": str(row.get("country", "") or "").strip(),
            "year": str(row.get("year", "") or item.get("year", "") or "").strip(),
            "warranty": str(row.get("warranty", "") or item.get("warranty", "") or "").strip(),
            "traction": row.get("traction", "") or "",
            "temperature": row.get("temperature", "") or "",
            "treadwear": row.get("treadwear", "") or "",
        }
        cart_desc = make_simple_description(simple_desc)

        row["description_long"] = cart_desc
        row["description"] = cart_desc
        row["description_export"] = cart_desc
        row["description_short"] = cart_desc

        row["json_ld"] = build_json_ld_product(row, _canonical_base)

        _seo_long_history.append(cart_desc)

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
        "scraped_count": len(raw_items),
        "after_filter_count": len(scoped_items),
        "import_limit": int(limit or 0),
        "csv_path": exports["csv_path"],
        "xlsx_path": exports["xlsx_path"],
        "salla_csv_path": exports["salla_csv_path"],
        "salla_xlsx_path": exports["salla_xlsx_path"],
        "failed_images_path": str(failed_images_path),
        "items": products,
    }

