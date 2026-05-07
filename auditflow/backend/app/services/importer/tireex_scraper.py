from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("importer.tireex")

_UA = {"User-Agent": "Mozilla/5.0"}


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _fetch(url: str) -> BeautifulSoup:
    r = requests.get(url, timeout=30, headers=_UA)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def _pick_text(doc: BeautifulSoup, selectors: List[str]) -> str:
    for s in selectors:
        el = doc.select_one(s)
        if el:
            v = _clean(el.get_text(" ", strip=True))
            if v:
                return v
    return ""


def _pick_attr(doc: BeautifulSoup, selectors: List[str], attr: str) -> str:
    for s in selectors:
        el = doc.select_one(s)
        if el and el.get(attr):
            v = _clean(el.get(attr))
            if v:
                return v
    return ""


def _is_product_url(url: str) -> bool:
    p = (urlparse(url).path or "").lower()
    return "/product" in p or "/products/" in p or "/shop/" in p


def _extract_product_links(base_url: str, soup: BeautifulSoup) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if not href:
            continue
        u = urljoin(base_url, href)
        if urlparse(u).netloc != urlparse(base_url).netloc:
            continue
        if not _is_product_url(u):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _next_page_url(base_url: str, soup: BeautifulSoup) -> str:
    candidates = [
        "a.next.page-numbers",
        ".pagination a.next",
        "a[rel='next']",
        ".woocommerce-pagination a.next",
    ]
    for sel in candidates:
        a = soup.select_one(sel)
        if a and a.get("href"):
            u = urljoin(base_url, a.get("href"))
            if urlparse(u).netloc == urlparse(base_url).netloc:
                return u
    return ""


def _parse_product_page(product_url: str) -> Dict[str, Any]:
    doc = _fetch(product_url)
    name = _pick_text(doc, ["h1", ".product_title", ".product-title", "h2"])
    price = _pick_text(doc, [".price .amount", ".price", "[class*='price'] .amount"])
    old_price = _pick_text(doc, [".price del .amount", ".price .old", ".was-price"])
    image = _pick_attr(doc, ["img.wp-post-image", ".product img", "img"], "data-src") or _pick_attr(doc, ["img.wp-post-image", ".product img", "img"], "src")
    image = urljoin(product_url, image) if image else ""
    year = _pick_text(doc, [".year", "[data-year]", ".manufacture-year"])
    warranty = _pick_text(doc, [".warranty", "[class*='warranty']"])
    country = _pick_text(doc, [".country", "[class*='origin']", ".origin"])
    pattern = _pick_text(doc, [".pattern", "[class*='pattern']"])
    desc = _pick_text(doc, [".product-description", ".woocommerce-product-details__short-description", ".entry-content"])
    return {
        "name": name,
        "price": price,
        "old_price": old_price,
        "product_url": product_url,
        "image_url": image,
        "year": year,
        "country": country,
        "warranty": warranty,
        "pattern": pattern,
        "description": desc,
    }


def scrape_tireex(url: str, *, multi_pages: bool = False, max_pages: int = 5) -> List[Dict[str, Any]]:
    links: List[str] = []
    if _is_product_url(url):
        links = [url]
    else:
        current = url
        visited_pages: Set[str] = set()
        page_count = 0
        while current and current not in visited_pages and page_count < max_pages:
            visited_pages.add(current)
            page_count += 1
            try:
                doc = _fetch(current)
            except Exception as e:
                log.warning("skip listing page %s: %s", current, e)
                break
            for u in _extract_product_links(current, doc):
                if u not in links:
                    links.append(u)
            if not multi_pages:
                break
            nxt = _next_page_url(current, doc)
            if not nxt:
                break
            cur_path = (urlparse(url).path or "").strip("/")
            nxt_path = (urlparse(nxt).path or "").strip("/")
            if cur_path and not nxt_path.startswith(cur_path.split("/")[0]):
                break
            current = nxt
    products: List[Dict[str, Any]] = []
    for u in links[:250]:
        try:
            p = _parse_product_page(u)
            if p.get("name"):
                products.append(p)
        except Exception as e:
            log.warning("skip product %s: %s", u, e)
    return products

