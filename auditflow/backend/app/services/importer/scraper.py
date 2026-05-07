from __future__ import annotations

import logging
import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from .tireex_scraper import scrape_tireex

log = logging.getLogger("importer.scraper")
_UA = {"User-Agent": "Mozilla/5.0"}
_SIZE_RE = re.compile(r"(\d{3})\s*/\s*(\d{2,3})\s*(?:ZR|R)?\s*(\d{2})", re.IGNORECASE)


def classify_url(url: str) -> str:
    p = (urlparse(url).path or "").lower().strip("/")
    if p and ("product" in p or "products" in p or "shop" in p):
        return "product_or_listing"
    if p:
        return "page"
    return "site"


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _fetch(url: str) -> BeautifulSoup:
    r = requests.get(url, timeout=30, headers=_UA)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def _looks_product_link(url: str) -> bool:
    p = (urlparse(url).path or "").lower()
    if any(x in p for x in ["/cart", "/checkout", "/tag/", "/category/"]):
        return False
    return ("/product" in p) or ("/shop/" in p) or (len(p.strip("/").split("/")) >= 2)


def _extract_generic_product_links(listing_url: str, soup: BeautifulSoup, limit: int) -> List[str]:
    out: List[str] = []
    seen = set()
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        u = urljoin(listing_url, href)
        if urlparse(u).netloc != urlparse(listing_url).netloc:
            continue
        if not _looks_product_link(u):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= limit * 3:
            break
    return out


def _extract_generic_product(product_url: str) -> Dict[str, Any]:
    doc = _fetch(product_url)
    name = ""
    for sel in ["h1.product_title", ".product_title", ".product-title", "h1", "meta[property='og:title']"]:
        el = doc.select_one(sel)
        if not el:
            continue
        if el.name == "meta":
            name = _clean(el.get("content") or "")
        else:
            name = _clean(el.get_text(" ", strip=True))
        if name:
            break
    text_all = _clean(doc.get_text(" ", strip=True))
    price = ""
    for sel in [".price", ".woocommerce-Price-amount", "bdi", "[class*='price']"]:
        el = doc.select_one(sel)
        if el:
            price = _clean(el.get_text(" ", strip=True))
            break
    image = ""
    og = doc.select_one("meta[property='og:image']")
    if og and og.get("content"):
        image = urljoin(product_url, og.get("content"))
    if not image:
        img = doc.select_one(".woocommerce-product-gallery img, img.wp-post-image, img")
        if img:
            image = urljoin(product_url, (img.get("data-src") or img.get("src") or "").strip())
    year = ""
    ym = re.search(r"(20[1-9][0-9])", text_all)
    if ym:
        year = ym.group(1)
    return {
        "name": name,
        "price": price,
        "old_price": "",
        "product_url": product_url,
        "image_url": image,
        "year": year,
        "country": "",
        "warranty": "",
        "pattern": "",
        "description": "",
    }


def scrape_generic(site_url: str, *, multi_pages: bool = False, max_pages: int = 5, limit: int = 20) -> List[Dict[str, Any]]:
    current = site_url
    visited = set()
    page_count = 0
    links: List[str] = []
    while current and current not in visited and page_count < max_pages:
        visited.add(current)
        page_count += 1
        try:
            doc = _fetch(current)
        except Exception as e:
            log.warning("generic skip page=%s err=%s", current, e)
            break
        for u in _extract_generic_product_links(current, doc, limit):
            if u not in links:
                links.append(u)
        if len(links) >= limit * 2 or not multi_pages:
            break
        nxt = doc.select_one("a[rel='next'], a.next, .pagination a.next, .page-numbers.next")
        if not nxt or not nxt.get("href"):
            break
        next_url = urljoin(current, nxt.get("href"))
        if urlparse(next_url).netloc != urlparse(site_url).netloc:
            break
        current = next_url
    products: List[Dict[str, Any]] = []
    for u in links:
        if len(products) >= limit:
            break
        try:
            p = _extract_generic_product(u)
            if p.get("name"):
                products.append(p)
        except Exception as e:
            log.warning("generic skip product=%s err=%s", u, e)
    return products


def scrape_products(site_url: str, *, multi_pages: bool = False, max_pages: int = 5, limit: int = 20) -> List[Dict[str, Any]]:
    domain = (urlparse(site_url).netloc or "").lower()
    kind = classify_url(site_url)
    log.info("importer domain=%s kind=%s multi_pages=%s limit=%s url=%s", domain, kind, multi_pages, limit, site_url)
    if "tireex.com" in domain or "tireex" in domain:
        return scrape_tireex(site_url, multi_pages=multi_pages, max_pages=max_pages, limit=limit)
    # fallback عام لأي موقع شبيه بمتاجر المنتجات.
    return scrape_generic(site_url, multi_pages=multi_pages, max_pages=max_pages, limit=limit)

