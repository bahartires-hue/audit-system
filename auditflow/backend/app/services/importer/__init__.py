from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

from .ai_rewriter import rewrite_description_fallback
from .csv_exporter import export_products_csv
from .image_downloader import download_image
from .parser import parse_tire_name
from .scraper import scrape_products
from .seo_optimizer import build_seo_fields

log = logging.getLogger("importer.pipeline")


def run_import_pipeline(site_url: str, uploads_root: Path) -> Dict[str, Any]:
    raw_items = scrape_products(site_url)
    products: List[Dict[str, Any]] = []
    seen = set()
    image_dir = uploads_root / "products"
    exports_dir = uploads_root.parent / "exports"

    for item in raw_items:
        parsed = parse_tire_name(item.get("name") or "")
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

