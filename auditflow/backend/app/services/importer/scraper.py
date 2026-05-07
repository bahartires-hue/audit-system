from __future__ import annotations

import logging
from typing import Any, Dict, List
from urllib.parse import urlparse

from .tireex_scraper import scrape_tireex

log = logging.getLogger("importer.scraper")


def classify_url(url: str) -> str:
    p = (urlparse(url).path or "").lower().strip("/")
    if p and ("product" in p or "products" in p or "shop" in p):
        return "product_or_listing"
    if p:
        return "page"
    return "site"


def scrape_products(site_url: str, *, multi_pages: bool = False, max_pages: int = 5, limit: int = 20) -> List[Dict[str, Any]]:
    domain = (urlparse(site_url).netloc or "").lower()
    kind = classify_url(site_url)
    log.info("importer domain=%s kind=%s multi_pages=%s limit=%s url=%s", domain, kind, multi_pages, limit, site_url)
    if "tireex.com" in domain or "tireex" in domain:
        return scrape_tireex(site_url, multi_pages=multi_pages, max_pages=max_pages, limit=limit)
    raise ValueError("هذا الموقع غير مدعوم حاليًا. المتاح الآن: tireex.com")

