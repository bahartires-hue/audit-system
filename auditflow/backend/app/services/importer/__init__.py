from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List

from .ai_rewriter import rewrite_description_fallback
from .csv_exporter import export_products_csv
from .image_downloader import download_image
from .parser import parse_tire_name
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


def filter_products(products: List[Dict[str, Any]], brand: str = "", size: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    b = _norm_brand(brand)
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
        if b and not (b in name or b in url or b == p_brand):
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
    raw_items = scrape_products(site_url, multi_pages=multi_pages, limit=limit)
    products: List[Dict[str, Any]] = []
    seen = set()
    image_dir = uploads_root / "products"
    exports_dir = uploads_root.parent / "exports"
    prepared: List[Dict[str, Any]] = []

    for item in raw_items:
        parsed = parse_tire_name(item.get("name") or "")
        prepared.append({**item, "_parsed": parsed})

    scoped_items = filter_products(prepared, brand=brand, size=size, limit=limit)
    log.info(
        "importer filter applied raw=%s scoped=%s brand=%s size=%s limit=%s multi_pages=%s",
        len(raw_items),
        len(scoped_items),
        brand,
        size,
        limit,
        multi_pages,
    )

    for item in scoped_items:
        parsed = item.get("_parsed") or parse_tire_name(item.get("name") or "")
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
            "image_url": item.get("image_url", ""),
            "image_local": local_image,
            "year": item.get("year", ""),
            "warranty": item.get("warranty", ""),
            "country": item.get("country", ""),
            "pattern": item.get("pattern", ""),
            "description": item.get("description", "") or rewrite_description_fallback(parsed),
            "parse_status": parsed.get("parse_status", ""),
            "seo_title": seo["seo_title"],
            "meta_description": seo["meta_description"],
            "keywords": seo["keywords"],
            "image_alt_text": seo["image_alt_text"],
            "image_status": image_status,
            "seo_status": seo_status,
            "price_status": price_status,
            "status": "جاهز" if seo_status == "ok" and image_status != "failed" else "مراجعة",
        }
        products.append(row)

    csv_path = exports_dir / "tire_products.csv"
    csv_out = export_products_csv(products, csv_path)
    log.info("importer done count=%s csv=%s", len(products), csv_out)
    return {"count": len(products), "csv_path": csv_out, "items": products}

