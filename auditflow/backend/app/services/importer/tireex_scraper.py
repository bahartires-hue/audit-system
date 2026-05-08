from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("importer.tireex")

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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
}

_SIZE_RE = re.compile(r"(\d{3})\s*/\s*(\d{2,3})\s*Z?R\s*(\d{2})", re.IGNORECASE)
_SIZE_URL_RE = re.compile(r"(\d{3})[-_/](\d{2,3})[-_]?r(\d{2})", re.IGNORECASE)
_NAME_RE = re.compile(
    r"([A-Za-z\u0600-\u06FF][A-Za-z0-9\u0600-\u06FF\s\-]{1,60}?\s+\d{3}\s*/\s*\d{2,3}\s*R?\s*\d{2}(?:\s+\d{2,3}(?:/\d{2,3})?[A-Z])?)",
    re.IGNORECASE,
)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _extract_size_token(text: str) -> str:
    m = _SIZE_RE.search(text or "")
    if not m:
        return ""
    return f"{m.group(1)}/{m.group(2)}R{m.group(3)}"


def _extract_size_from_url(url: str) -> str:
    slug = (urlparse(url).path or "").strip("/").lower()
    m = _SIZE_URL_RE.search(slug)
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
    links: Set[str] = set()
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


def scrape_tireex_product(product_url: str) -> Dict[str, Any]:
    html, final_url, status = get_html(product_url)
    if status >= 400:
        return {}
    soup = BeautifulSoup(html, "html.parser")

    name = ""
    h1 = soup.select_one("h1")
    if h1:
        name = _clean(h1.get_text(" ", strip=True))
    if not name:
        og_title = soup.select_one('meta[property="og:title"]')
        if og_title:
            name = _clean(og_title.get("content", ""))
    if not name:
        t = soup.select_one("title")
        if t:
            name = _clean(t.get_text(" ", strip=True))

    price = ""
    price_el = soup.select_one(".price .amount, .amount, .price")
    if price_el:
        price = clean_price(price_el.get_text(" ", strip=True))
    if not price:
        meta_price = soup.select_one('meta[property="product:price:amount"]')
        if meta_price:
            price = clean_price(meta_price.get("content", ""))

    image = ""
    og_img = soup.select_one('meta[property="og:image"]')
    if og_img:
        image = _clean(og_img.get("content", ""))
    if not image:
        img = soup.select_one("img.wp-post-image, .woocommerce-product-gallery img, img")
        if img:
            image = _clean((img.get("src") or img.get("data-src") or ""))
    if image:
        image = urljoin(final_url, image)

    desc = ""
    desc_el = soup.select_one(".woocommerce-product-details__short-description")
    if desc_el:
        desc = _clean(desc_el.get_text(" ", strip=True))
    if not desc:
        meta_desc = soup.select_one('meta[name="description"]')
        if meta_desc:
            desc = _clean(meta_desc.get("content", ""))

    if not name:
        return {}

    return {
        "name": name,
        "price": price,
        "old_price": "",
        "product_url": final_url,
        "image_url": image,
        "year": "",
        "country": "",
        "warranty": "",
        "pattern": "",
        "description": desc,
        "_size_token": _extract_size_token(name) or _extract_size_from_url(final_url),
    }


def _extract_products_from_listing_text(text: str, selected_brand: str, page_url: str) -> List[Dict[str, Any]]:
    full = _clean(text or "")
    products: List[Dict[str, Any]] = []
    seen = set()
    matches = list(_NAME_RE.finditer(full))
    for idx, m_name in enumerate(matches):
        name = _clean(m_name.group(1))
        if selected_brand:
            sb = (selected_brand or "").strip().lower()
            if sb and sb not in name.lower():
                continue
        start = m_name.end()
        end = matches[idx + 1].start() if (idx + 1) < len(matches) else min(len(full), start + 500)
        c = full[start:end]
        # price line around "للإطار الواحد"
        m_price_line = re.search(r"(\d[\d,\.]*)\s+(\d[\d,\.]*)\s+للإطار\s+الواحد", c)
        price = ""
        old_price = ""
        if m_price_line:
            price = clean_price(m_price_line.group(1))
            old_price = clean_price(m_price_line.group(2))
        else:
            nums = re.findall(r"\b\d[\d,\.]*\b", c)
            if nums:
                price = clean_price(nums[0])
                old_price = clean_price(nums[1]) if len(nums) > 1 else ""
        year = ""
        m_year = re.search(r"سنة\s*الصنع\s*[:：]?\s*(20\d{2})", c, flags=re.IGNORECASE)
        if m_year:
            year = m_year.group(1)
        warranty = ""
        m_w = re.search(r"الضمان\s*[:：]?\s*([^\n\r|]+)", c, flags=re.IGNORECASE)
        if m_w:
            warranty = _clean(m_w.group(1))
        pattern = ""
        m_p = re.search(r"النقشة\s*[:：]?\s*([^\n\r|]+)", c, flags=re.IGNORECASE)
        if m_p:
            pattern = _clean(m_p.group(1))
        size_token = _extract_size_token(name)
        key = (name.lower(), size_token, price)
        if key in seen:
            continue
        seen.add(key)
        products.append(
            {
                "name": name,
                "price": price,
                "old_price": old_price,
                "product_url": page_url,
                "image_url": "",
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
    all_links: List[str] = []
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
        all_links.extend(links)

        text_blob = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
        page_products = _extract_products_from_listing_text(text_blob, selected_brand, final_url)
        log.warning("TIREEX PAGE PRODUCTS FROM TEXT = %s", len(page_products))
        if not page_products and page > 1:
            break
        products.extend(page_products)
        if len(products) >= max_items:
            break

    # Enrich listing-text products with real product URLs/images from product pages.
    unique_links = sorted(set(all_links))
    detail_items: List[Dict[str, Any]] = []
    for link in unique_links[: max_items * 3]:
        try:
            d = scrape_tireex_product(link)
            if d.get("name"):
                detail_items.append(d)
            time.sleep(0.2)
        except Exception:
            continue
    for p in products:
        if p.get("image_url"):
            continue
        p_size = str(p.get("_size_token") or "")
        p_name = str(p.get("name") or "").lower()
        match = None
        for d in detail_items:
            d_size = str(d.get("_size_token") or "")
            d_name = str(d.get("name") or "").lower()
            if p_size and d_size and p_size == d_size:
                match = d
                break
            if p_name and d_name and p_name.split(" ")[0] in d_name:
                match = d
        if match:
            p["product_url"] = match.get("product_url") or p.get("product_url")
            p["image_url"] = match.get("image_url") or p.get("image_url")
            if not p.get("description"):
                p["description"] = match.get("description", "")

    log.warning("TIREEX FINAL PRODUCTS COUNT = %s", len(products))
    elapsed = time.perf_counter() - started_at
    log.info("tireex scrape done total_seconds=%.2f", elapsed)
    return products[:max_items]

