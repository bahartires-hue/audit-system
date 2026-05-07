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


def _looks_like_product_link(base_url: str, u: str) -> bool:
    p = (urlparse(u).path or "").lower().strip("/")
    if not p:
        return False
    if urlparse(u).netloc != urlparse(base_url).netloc:
        return False
    blocked = ("cart", "checkout", "account", "login", "register", "category", "tag", "search", "brands", "brand")
    if any(x in p for x in blocked):
        return False
    # product pages عادة تكون صفحات داخلية وليست أقسام عامة.
    return len(p.split("/")) >= 2


def _extract_product_links(base_url: str, soup: BeautifulSoup) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    selectors = [
        "a.woocommerce-LoopProduct-link[href]",
        "li.product a[href]",
        ".products .product a[href]",
        ".product-item a[href]",
        ".shop-item a[href]",
        "article.product a[href]",
    ]

    def _add(href: str) -> None:
        if not href:
            return
        u = urljoin(base_url, href)
        if u in seen:
            return
        if _is_product_url(u) or _looks_like_product_link(base_url, u):
            seen.add(u)
            out.append(u)

    for sel in selectors:
        for a in soup.select(sel):
            _add(a.get("href") or "")

    # fallback شامل إذا لم نجد عبر selectors المعروفة.
    if not out:
        for a in soup.select("a[href]"):
            _add(a.get("href") or "")
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


def _extract_list_products(base_url: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    cards = soup.select("li.product, .product-item, article.product, .products .product")
    for card in cards:
        a = card.select_one("a[href]")
        if not a or not a.get("href"):
            continue
        product_url = urljoin(base_url, a.get("href"))
        if product_url in seen:
            continue
        seen.add(product_url)
        name = _clean(
            (card.select_one(".woocommerce-loop-product__title, .product-title, h2, h3") or a).get_text(" ", strip=True)
        )
        price = _clean((card.select_one(".price .amount, .price, [class*='price']") or {}).get_text(" ", strip=True) if card.select_one(".price .amount, .price, [class*='price']") else "")
        old_price = _clean((card.select_one(".price del .amount, .old-price, .was-price") or {}).get_text(" ", strip=True) if card.select_one(".price del .amount, .old-price, .was-price") else "")
        img = card.select_one("img")
        image_url = ""
        if img:
            raw = img.get("data-src") or img.get("data-lazy-src") or img.get("src") or ""
            image_url = urljoin(base_url, raw) if raw else ""
        if not name:
            continue
        out.append(
            {
                "name": name,
                "price": price,
                "old_price": old_price,
                "product_url": product_url,
                "image_url": image_url,
                "year": "",
                "country": "",
                "warranty": "",
                "pattern": "",
                "description": "",
            }
        )
    return out


def _in_same_scope(seed_url: str, candidate_url: str) -> bool:
    seed = urlparse(seed_url)
    cand = urlparse(candidate_url)
    if seed.netloc != cand.netloc:
        return False
    seed_path = (seed.path or "").strip("/")
    cand_path = (cand.path or "").strip("/")
    if not seed_path:
        # للرابط الجذري: نسمح فقط بروابط pagination المعروفة.
        return ("page/" in cand_path) or ("paged=" in cand.query) or (cand_path == "")
    base_prefix = seed_path.split("/")[0]
    return cand_path.startswith(base_prefix)


def _parse_product_page(product_url: str) -> Dict[str, Any]:
    doc = _fetch(product_url)
    name = _pick_text(doc, ["h1", ".product_title", ".product-title", "h2"])
    if not name:
        name = _pick_attr(doc, ["meta[property='og:title']", "meta[name='twitter:title']"], "content")
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


def scrape_tireex(url: str, *, multi_pages: bool = False, max_pages: int = 5, limit: int = 20) -> List[Dict[str, Any]]:
    links: List[str] = []
    listing_items: List[Dict[str, Any]] = []
    max_items = max(1, int(limit or 20))
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
            listing_items.extend(_extract_list_products(current, doc))
            for u in _extract_product_links(current, doc):
                if u not in links:
                    links.append(u)
                if len(links) >= max_items:
                    break
            if len(links) >= max_items:
                break
            if not multi_pages:
                break
            nxt = _next_page_url(current, doc)
            if not nxt:
                break
            if not _in_same_scope(url, nxt):
                log.info("stop pagination outside scope seed=%s next=%s", url, nxt)
                break
            current = nxt
    products: List[Dict[str, Any]] = []
    for u in links[: max_items * 2]:
        if len(products) >= max_items:
            break
        try:
            p = _parse_product_page(u)
            if p.get("name"):
                products.append(p)
        except Exception as e:
            log.warning("skip product %s: %s", u, e)
    if products:
        return products[:max_items]
    # fallback إذا فشل parsing صفحات المنتج: نعيد منتجات الكروت من صفحة الماركة/البحث.
    return listing_items[:max_items]

