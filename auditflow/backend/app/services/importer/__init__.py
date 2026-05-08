from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List

from .ai_rewriter import rewrite_description_fallback
from .cloudinary_uploader import upload_to_cloudinary
from .csv_exporter import export_products_files
from .image_downloader import download_image
from .parser import normalize_brand_name, parse_tire_name
from .scraper import scrape_products
from .seo_optimizer import build_seo_fields

log = logging.getLogger("importer.pipeline")


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


def _infer_brand_from_url(url: str) -> str:
    path = (re.sub(r"[?#].*$", "", (url or "").strip())).strip("/")
    if not path:
        return ""
    # use the last meaningful slug segment as brand hint
    parts = [p for p in path.split("/") if p]
    if not parts:
        return ""
    tail = parts[-1]
    tail = re.sub(r"^(product-category|category|brand|brands)-?", "", tail, flags=re.IGNORECASE)
    tail = re.sub(r"-tires?$|-tyres?$|-كفرات$|-اطارات$|-إطارات$", "", tail, flags=re.IGNORECASE)
    tail = tail.replace("-", " ").replace("_", " ").strip()
    if tail:
        return normalize_brand_name(tail)
    return ""


def _infer_brand_from_product_name(name: str) -> str:
    guess = normalize_brand_name((name or "").split(" ")[0] if (name or "").strip() else "")
    if guess:
        return guess
    n = normalize_brand_name(name or "")
    # if full normalized name has spaces, first token is the likely brand
    return (n.split(" ")[0] if n else "")


def _name_contains_brand(name: str, selected_brand: str) -> bool:
    n = normalize_brand_name(name or "").lower()
    b = normalize_brand_name(selected_brand or "").lower()
    return bool(b and b in n)


def _is_explicit_other_brand(name: str, selected_brand: str) -> bool:
    b = normalize_brand_name(selected_brand or "").lower()
    guessed = normalize_brand_name(parse_tire_name(name or "").get("brand", "")).lower()
    return bool(b and guessed and guessed != b)


def filter_products(products: List[Dict[str, Any]], brand: str = "", size: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    b_raw = (brand or "").strip()
    b_norm = normalize_brand_name(b_raw) if b_raw else ""
    b = _norm_brand(b_norm or b_raw)
    sz = _norm_size(size)
    out: List[Dict[str, Any]] = []
    for item in products:
        if len(out) >= max(1, limit):
            break
        parsed = item.get("_parsed") or {}
        name = _norm_brand(item.get("name", ""))
        url = _norm_brand(item.get("product_url", ""))
        p_brand = _norm_brand(parsed.get("brand", ""))
        p_size = _norm_size(parsed.get("size", ""))
        # Do not drop neutral rows when parsed brand is missing.
        # Reject only explicit brand mismatch.
        if b and p_brand and (b != p_brand) and (b not in name) and (b not in url):
            continue
        if sz and not p_size:
            p_size = _extract_size_from_text(item.get("name", ""))
        if sz and sz != p_size:
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
    multi_pages: bool = False,
) -> Dict[str, Any]:
    selected_brand = normalize_brand_name((brand or "").strip())
    if not selected_brand:
        selected_brand = _infer_brand_from_url(site_url)
    raw_items = scrape_products(site_url, multi_pages=multi_pages, limit=limit, selected_brand=selected_brand)
    products: List[Dict[str, Any]] = []
    seen = set()
    image_dir = uploads_root / "products"
    exports_dir = uploads_root.parent / "exports"
    prepared: List[Dict[str, Any]] = []
    accepted_products_count = 0
    skipped_wrong_brand_count = 0
    skipped_image_failed_count = 0

    for item in raw_items:
        parsed = parse_tire_name(item.get("name") or "")
        prepared.append({**item, "_parsed": parsed})

    scoped_items = filter_products(prepared, brand=selected_brand, size=size, limit=limit)
    log.info(
        "importer filter applied raw=%s scoped=%s brand=%s size=%s limit=%s multi_pages=%s",
        len(raw_items),
        len(scoped_items),
        selected_brand,
        size,
        limit,
        multi_pages,
    )

    for item in scoped_items:
        parsed = item.get("_parsed") or parse_tire_name(item.get("name") or "")
        if not parsed.get("size"):
            fallback_size = str(item.get("_size_token") or "").strip().upper().replace(" ", "")
            m = re.match(r"^(\d{3})/(\d{2,3})R(\d{2})$", fallback_size)
            if m:
                parsed["size"] = fallback_size
                parsed["width"] = m.group(1)
                parsed["profile"] = m.group(2)
                parsed["rim"] = m.group(3)
                if parsed.get("parse_status") in {"failed", "size_missing"}:
                    parsed["parse_status"] = "ok"
        if not parsed.get("size"):
            log.info("skip invalid/no-size product name=%s", item.get("name", ""))
            continue
        if re.search(r"ابحث|search", str(item.get("name", "")), flags=re.IGNORECASE):
            log.info("skip non-product row name=%s", item.get("name", ""))
            continue
        if selected_brand:
            selected = normalize_brand_name(selected_brand).lower().strip()
            parsed_brand = normalize_brand_name(str(parsed.get("brand", ""))).lower().strip()
            accepted_by_name = _name_contains_brand(item.get("name", ""), selected_brand)
            # If parser could not infer brand, trust selected category/input brand.
            if not parsed_brand and selected:
                parsed["brand"] = selected_brand
                parsed_brand = selected
            # Reject only clear mismatches; keep neutral names to avoid false zero results.
            if parsed_brand and parsed_brand != selected and not accepted_by_name:
                log.info("SKIPPED_WRONG_BRAND selected=%s parsed=%s name=%s", selected, parsed_brand, item.get("name", ""))
                skipped_wrong_brand_count += 1
                continue
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

        local_image, image_status = download_image(item.get("image_url", ""), image_dir, seo["image_slug"])
        cloud_url = ""
        cloud_status = ""
        if local_image:
            cloud_url, cloud_status = upload_to_cloudinary(local_image, seo["image_slug"])
        elif (item.get("image_url") or "").startswith("http"):
            # fallback: ask Cloudinary to fetch source URL directly
            cloud_url, cloud_status = upload_to_cloudinary(item.get("image_url", ""), seo["image_slug"])
        image_status = "ok" if (cloud_url and str(cloud_url).startswith("https://res.cloudinary.com/")) else "failed"
        if image_status != "ok":
            skipped_image_failed_count += 1
        price = (item.get("price") or "").strip()
        price_status = "ok" if price else "price_missing"
        seo_status = "ok" if parsed.get("size") else "needs_review"
        row = {
            "name": item.get("name", ""),
            "product_title": parsed.get("product_title", ""),
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
                if image_status != "ok" or seo_status != "ok" or price_status != "ok"
                else "جاهز"
            ),
        }
        if image_status in {"failed", "no_image_url"} and (item.get("image_url") or "").startswith("https://"):
            row["status"] = "مراجعة"
        products.append(row)
        accepted_products_count += 1

    # enforce selected brand without hard-failing whole export
    if selected_brand:
        selected_norm = normalize_brand_name(selected_brand).lower().strip()
        filtered_products: List[Dict[str, Any]] = []
        for p in products:
            b = normalize_brand_name(str(p.get("brand", ""))).lower().strip()
            name_has_selected = selected_norm in normalize_brand_name(str(p.get("name", ""))).lower()
            if not b and selected_norm:
                p["brand"] = normalize_brand_name(selected_brand)
                b = selected_norm
            if b != selected_norm and not name_has_selected:
                skipped_wrong_brand_count += 1
                log.info("SKIPPED_WRONG_BRAND selected=%s parsed=%s name=%s", selected_norm, b, p.get("name", ""))
                continue
            p["brand"] = normalize_brand_name(selected_brand)
            filtered_products.append(p)
        products = filtered_products

    log.info("selected_brand=%s", selected_brand)
    log.info("accepted_products_count=%s", accepted_products_count)
    log.info("skipped_wrong_brand_count=%s", skipped_wrong_brand_count)
    log.info("skipped_image_failed_count=%s", skipped_image_failed_count)

    csv_path = exports_dir / "tire_products.csv"
    xlsx_path = exports_dir / "tire_products.xlsx"
    exports = export_products_files(products, csv_path, xlsx_path)
    log.info("importer done count=%s csv=%s xlsx=%s", len(products), exports["csv_path"], exports["xlsx_path"])
    return {
        "count": len(products),
        "csv_path": exports["csv_path"],
        "xlsx_path": exports["xlsx_path"],
        "salla_csv_path": exports["salla_csv_path"],
        "salla_xlsx_path": exports["salla_xlsx_path"],
        "items": products,
    }

