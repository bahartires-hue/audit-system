from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("importer.tireex")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
}

BAD_LINK_PARTS = [
    "/cdn-cgi/",
    "email-protection",
    "add-to-cart",
    "product-category",
    "/cart",
    "/checkout",
    "/my-account",
    "mailto:",
    "tel:",
    "#",
]

_SIZE_RE = re.compile(r"(\d{3})\s*/\s*(\d{2,3})\s*Z?R\s*(\d{2})", re.IGNORECASE)
_BAD_NAME_WORDS = ["وقود", "راحة", "إضافة", "السلة", "للإطار", "الضمان", "النقشة"]


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _extract_size_token(text: str) -> str:
    m = _SIZE_RE.search(text or "")
    if not m:
        return ""
    return f"{m.group(1)}/{m.group(2)}R{m.group(3)}"


def clean_price(text: str) -> str:
    if not text:
        return ""
    token = re.sub(r"[^\d,\.]", "", text)
    if not token:
        return ""
    if "," in token and "." in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").replace(",", ".")
        else:
            token = token.replace(",", "")
    else:
        if token.count(",") == 1 and token.count(".") == 0:
            token = token.replace(",", ".")
        elif token.count(",") > 1 and token.count(".") == 0:
            token = token.replace(",", "")
        elif token.count(".") > 1 and token.count(",") == 0:
            token = token.replace(".", "")
    return token.strip()


def get_html(url: str) -> Tuple[str, str, int]:
    r = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
    return r.text or "", str(r.url), int(r.status_code)


def extract_tireex_product_links(html: str, page_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(page_url, href)
        full = full.split("?")[0].rstrip("/") + "/"
        low = full.lower()
        if "/product/" not in low:
            continue
        if any(x in low for x in BAD_LINK_PARTS):
            continue
        host = (urlparse(full).netloc or "").lower()
        if "tireex.com" not in host:
            continue
        links.add(full)
    return sorted(links)


def is_real_product_name(name: str, selected_brand: str = "") -> bool:
    n = _clean(name)
    if not n:
        return False
    if any(w in n for w in _BAD_NAME_WORDS):
        return False
    if "R" not in n.upper():
        return False
    if selected_brand and selected_brand.strip():
        return selected_brand.strip().lower() in n.lower()
    return True


def dedupe_products(products: List[Dict[str, Any]], selected_brand: str = "") -> List[Dict[str, Any]]:
    clean: Dict[str, Dict[str, Any]] = {}
    for p in products:
        name = _clean(p.get("name", ""))
        if not is_real_product_name(name, selected_brand):
            continue
        price_raw = clean_price(str(p.get("price", "")))
        try:
            price_num = float(price_raw or "0")
        except Exception:
            continue
        if price_num < 100:
            continue
        p["name"] = name
        p["price"] = str(int(price_num)) if price_num.is_integer() else str(price_num)
        clean[name] = p
    return list(clean.values())


def _extract_products_from_cards(html: str, page_url: str, selected_brand: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("ul.products li.product, .products .product, .woocommerce ul.products li.product, .product-card")
    products: List[Dict[str, Any]] = []
    seen = set()
    for card in cards:
        name_el = card.select_one("h2, h3, a.product-card-content-title, .woocommerce-loop-product__title, a")
        if not name_el:
            continue
        name = _clean(name_el.get_text(" ", strip=True))
        if not is_real_product_name(name, selected_brand):
            continue

        a = card.select_one("a[href]")
        product_url = page_url
        if a and a.get("href"):
            full = urljoin(page_url, a.get("href"))
            low = full.lower()
            if "/product/" in low and not any(x in low for x in BAD_LINK_PARTS):
                product_url = full.split("?")[0].rstrip("/") + "/"

        price = ""
        old_price = ""
        price_el = card.select_one(".price, .amount")
        if price_el:
            nums = re.findall(r"\b\d[\d,\.]*\b", price_el.get_text(" ", strip=True))
            if nums:
                price = clean_price(nums[0])
                old_price = clean_price(nums[1]) if len(nums) > 1 else ""
        if not price:
            nums = re.findall(r"\b\d[\d,\.]*\b", card.get_text(" ", strip=True))
            if nums:
                price = clean_price(nums[0])
                old_price = clean_price(nums[1]) if len(nums) > 1 else ""

        image_url = ""
        img = card.select_one("img")
        if img:
            image_url = _clean((img.get("data-src") or img.get("src") or ""))
            if image_url:
                image_url = urljoin(page_url, image_url)

        card_text = _clean(card.get_text(" ", strip=True))
        year = ""
        m_year = re.search(r"سنة\s*الصنع\s*[:：]?\s*(20\d{2})", card_text, flags=re.IGNORECASE)
        if m_year:
            year = m_year.group(1)
        warranty = ""
        m_w = re.search(r"الضمان\s*[:：]?\s*([^\n\r|]+)", card_text, flags=re.IGNORECASE)
        if m_w:
            warranty = _clean(m_w.group(1))
        pattern = ""
        m_p = re.search(r"النقشة\s*[:：]?\s*([^\n\r|]+)", card_text, flags=re.IGNORECASE)
        if m_p:
            pattern = _clean(m_p.group(1))

        size_token = _extract_size_token(name)
        key = ((product_url or name).lower(), size_token, clean_price(price))
        if key in seen:
            continue
        seen.add(key)

        products.append(
            {
                "name": name,
                "price": clean_price(price),
                "old_price": clean_price(old_price),
                "product_url": product_url,
                "image_url": image_url,
                "year": year,
                "country": "",
                "warranty": warranty,
                "pattern": pattern,
                "description": "",
                "_size_token": size_token,
            }
        )
    return products


def scrape_tireex(
    url: str,
    *,
    multi_pages: bool = False,
    max_pages: int = 5,
    limit: int = 20,
    selected_brand: str = "",
) -> List[Dict[str, Any]]:
    started_at = time.perf_counter()
    base = (url or "").rstrip("/")
    max_items = max(1, int(limit or 20))
    pages_to_scan = max(1, int(max_pages or 5)) if multi_pages else 1

    products: List[Dict[str, Any]] = []
    for page in range(1, pages_to_scan + 1):
        page_url = f"{base}/" if page == 1 else f"{base}/page/{page}/"
        try:
            html, final_url, status = get_html(page_url)
        except Exception as e:
            log.warning("TIREEX PAGE FETCH ERROR = %s err=%s", page_url, e)
            continue

        log.warning("TIREEX PAGE = %s", page_url)
        log.warning("TIREEX STATUS = %s", status)
        log.warning("TIREEX FINAL URL = %s", final_url)
        log.warning("TIREEX HTML LENGTH = %s", len(html))
        log.warning("TIREEX HTML START = %s", (html[:500] or "").replace("\n", " "))

        if status >= 400:
            continue
        links = extract_tireex_product_links(html, final_url)
        log.warning("TIREEX PRODUCT LINKS COUNT = %s", len(links))
        log.warning("TIREEX FIRST LINKS = %s", links[:5])

        page_products = _extract_products_from_cards(html, final_url, selected_brand)
        log.warning("TIREEX PAGE PRODUCTS FROM CARDS = %s", len(page_products))
        if not page_products and page > 1:
            break
        products.extend(page_products)
        if len(products) >= max_items:
            break

    log.warning("PRODUCTS BEFORE CLEAN = %s", len(products))
    products = dedupe_products(products, selected_brand)
    log.warning("PRODUCTS AFTER CLEAN = %s", len(products))
    log.warning("FINAL EXPORT COUNT = %s", len(products))

    elapsed = time.perf_counter() - started_at
    log.info("tireex scrape done total_seconds=%.2f", elapsed)
    return products[:max_items]

