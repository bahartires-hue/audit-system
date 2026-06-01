from __future__ import annotations

import csv
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set
import json
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("universal_scraper")

# =========================
# 1) إعداد المواقع (قابل للتوسّع)
# =========================

SITES_CONFIG: Dict[str, Dict[str, Any]] = {
    "tireex": {
        "base_url": "https://tireex.com",
        "product_selector": "div.product-grid-item",
        "title_selector": ".product-title",
        "price_selector": ".price",
        "image_selector": "img",
        "link_selector": "a",
        "pagination_param": "paged",
    },
    "lumitires": {
        "base_url": "https://lumitiress.com",
        "product_selector": "div.product-box",
        "title_selector": ".product-box-title",
        "price_selector": ".product-box-price",
        "image_selector": "img",
        "link_selector": "a",
        "pagination_param": "page",
    },
    "kafaratplus": {
        "base_url": "https://kafaratplus.com",
        "product_selector": "div.product-box, ul.products li.product",
        "title_selector": ".product-box-title, h2.woocommerce-loop-product__title",
        "price_selector": ".product-box-price, span.price",
        "image_selector": "img",
        "link_selector": "a.woocommerce-LoopProduct-link, a[href*='/product/']",
        "pagination_param": "page",
    },
    "etar": {
        "base_url": "https://etar.com",
        "product_selector": "li.product",
        "title_selector": "h2.woocommerce-loop-product__title",
        "price_selector": "span.price",
        "image_selector": "img",
        "link_selector": "a",
        "pagination_param": "paged",
    },
    # متجر Salla — روابط المنتج: /products/{slug}
    "almurad": {
        "base_url": "https://almuradstore.com",
        "platform": "almurad_salla",
        "enrich_detail_pages": True,
        "product_selector": "a[href*='/products/']",
        "title_selector": "h2, h3",
        "price_selector": ".price",
        "image_selector": "img",
        "link_selector": "a[href*='/products/']",
        "pagination_param": "page",
    },
    "brwx": {
        "base_url": "https://brwx.com",
        "enrich_detail_pages": True,
        "product_selector": "ul.products li.product",
        "title_selector": "h2.woocommerce-loop-product__title, .woocommerce-loop-product__title",
        "price_selector": "span.price, .price",
        "image_selector": "img",
        "link_selector": "a.woocommerce-LoopProduct-link, a[href*='/product/']",
        "pagination_param": "paged",
    },
}

# إعدادات Brand Deep Scan
DEEP_SCAN_SITES: Dict[str, Dict[str, Any]] = {
    "tireex": {
        "base_url": "https://tireex.com",
        "start_urls": ["https://tireex.com/product-category/accelera-tires/"],
        "product_link_selectors": [
            "a.product-card-content-title[href]",
            "ul.products li.product a.woocommerce-LoopProduct-link[href]",
            "a.woocommerce-LoopProduct-link[href]",
        ],
        "use_gtm_embed": True,
        "product_title_selector": "h1.product_title, h1.product-title, h1",
        "brand_selector": None,
        "price_selector": "p.price, .summary .price, .price",
        "image_selector": "figure.woocommerce-product-gallery__wrapper img, .woocommerce-product-gallery img, .product img",
        "description_selector": (
            "div.woocommerce-Tabs-panel--description, #tab-description, "
            ".woocommerce-product-details__short-description"
        ),
    },
    "lumitires": {
        "base_url": "https://lumitiress.com",
        "start_urls": ["https://lumitiress.com/shop/"],
        "product_link_selectors": ["ul.products li.product a[href]"],
        "use_gtm_embed": False,
        "product_title_selector": "h1.product_title",
        "brand_selector": None,
        "price_selector": "p.price",
        "image_selector": "div.woocommerce-product-gallery__wrapper img, figure.woocommerce-product-gallery__wrapper img",
        "description_selector": "div.woocommerce-product-details__short-description",
    },
    "kafaratplus": {
        "base_url": "https://kafaratplus.com",
        "start_urls": ["https://kafaratplus.com/shop/"],
        "product_link_selectors": [
            "div.product-box a[href]",
            "div.product-box a[href*='/product/']",
            "ul.products li.product a[href]",
            "a.woocommerce-LoopProduct-link[href]",
        ],
        "use_gtm_embed": False,
        "product_title_selector": "h1.product_title, h1.product-title, .product-box-title, h1",
        "brand_selector": None,
        "price_selector": "p.price, .product-box-price, .price",
        "image_selector": "figure.woocommerce-product-gallery__wrapper img, .product-box img, img",
        "description_selector": "div.woocommerce-product-details__short-description, .product-box",
    },
    "etar": {
        "base_url": "https://etar.com",
        "start_urls": ["https://etar.com/shop/"],
        "product_link_selectors": ["ul.products li.product a[href]"],
        "use_gtm_embed": False,
        "product_title_selector": "h1.product_title",
        "brand_selector": None,
        "price_selector": "p.price",
        "image_selector": "figure.woocommerce-product-gallery__wrapper img",
        "description_selector": "div.woocommerce-product-details__short-description",
    },
    "almurad": {
        "base_url": "https://almuradstore.com",
        "start_urls": [
            "https://almuradstore.com/products",
            "https://almuradstore.com/",
        ],
        "product_link_selectors": [
            "a[href*='/products/']",
        ],
        "use_gtm_embed": False,
        "product_title_selector": "h1, h2, .product-title, meta[property='og:title']",
        "brand_selector": None,
        "price_selector": ".price, [class*='price'], [class*='Price']",
        "image_selector": "img[src], img[data-src]",
        "description_selector": "[class*='description'], .product-description, article",
    },
}

_DEEP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
}

_HTTP_SESSION: Optional[requests.Session] = None
_PDP_ENRICH_WORKERS = max(1, min(6, int(os.getenv("AUDITFLOW_PDP_WORKERS", "4") or "4")))


def _http_session() -> requests.Session:
    global _HTTP_SESSION
    if _HTTP_SESSION is None:
        _HTTP_SESSION = requests.Session()
        _HTTP_SESSION.headers.update(_DEEP_HEADERS)
    return _HTTP_SESSION


_BRAND_TITLE_ALIASES: Dict[str, tuple[str, ...]] = {
    "accelera": ("accelera", "اكسيليرا", "أكسيليرا", "إطارات اكسيليرا"),
    "hankook": ("hankook", "هانكوك"),
    "michelin": ("michelin", "ميشلان"),
    "goodyear": ("goodyear", "جوديير"),
    "sailun": ("sailun", "سايلون", "سايلن"),
    "continental": ("continental", "كونتيننتال", "كونتيننتال"),
}

_KAFARATPLUS_SKIP_PATHS = frozenset(
    {
        "shop",
        "cart",
        "checkout",
        "blog",
        "contact",
        "about",
        "en",
        "ar",
        "product",
        "products",
        "api",
    }
)

_KAFARATPLUS_TIRE_TITLE_RE = re.compile(r"\d{3}\s*/?\s*\d{2,3}\s*Z?R\s*\d{2}", re.IGNORECASE)


def _deep_brand_tokens(brand_name: str) -> tuple[str, ...]:
    key = re.sub(r"\s+", " ", (brand_name or "").strip().lower())
    extra = _BRAND_TITLE_ALIASES.get(key, ())
    return (key,) + tuple(x.lower() for x in extra if x)


def _deep_title_matches_brand(title: str, brand_name: str) -> bool:
    t = (title or "").lower()
    for tok in _deep_brand_tokens(brand_name):
        if tok and tok in t:
            return True
    return False


def _deep_url_matches_brand(url: str, brand_name: str) -> bool:
    u = (url or "").lower()
    for tok in _deep_brand_tokens(brand_name):
        if tok and tok in u:
            return True
    return False


def _deep_product_title_from_soup(soup: BeautifulSoup, cfg: Dict[str, Any]) -> str:
    for sel in (cfg.get("product_title_selector") or "h1").split(","):
        el = soup.select_one(sel.strip())
        if el:
            t = _deep_extract_text(el)
            if t:
                return t
    og = soup.select_one("meta[property='og:title'], meta[name='twitter:title']")
    if og and og.get("content"):
        return _clean_meta_text(og["content"])
    if soup.title:
        return _clean_meta_text(soup.title.get_text(" ", strip=True))
    return ""


def _request_headers_for_url(url: str) -> Dict[str, str]:
    headers = dict(_DEEP_HEADERS)
    if "almuradstore.com" in (url or "").lower():
        headers["Referer"] = "https://almuradstore.com/"
        headers["Origin"] = "https://almuradstore.com"
    return headers


def _deep_get_soup(url: str) -> BeautifulSoup:
    try:
        r = _http_session().get(url, timeout=28, headers=_request_headers_for_url(url))
        r.raise_for_status()
    except requests.RequestException as e:
        raise ValueError(f"تعذّر فتح الرابط: {url} — {e}") from e
    text = r.text or ""
    if len(text) < 300:
        log.warning("short_html_response url=%s len=%s", url, len(text))
    try:
        return BeautifulSoup(text, "lxml")
    except Exception:
        return BeautifulSoup(text or "<html></html>", "html.parser")


def _deep_normalize_shop_url(base_url: str, href: str) -> str:
    if not href:
        return ""
    full = urljoin(base_url, href.split("?")[0])
    path = (urlparse(full).path or "").lower().rstrip("/")
    if not path or path in {"/", "/shop", "/cart", "/checkout", "/my-account", "/products"}:
        return ""
    if re.search(r"/products/[^/]+/?$", path):
        return full
    if "/product/" in path or "/shop/" in path:
        return full
    # روابط منتج مباشرة مثل /product-name/ (شائعة في كفرات بلس)
    segments = [s for s in path.split("/") if s]
    if len(segments) == 1 and segments[0] not in {
        "shop",
        "cart",
        "checkout",
        "blog",
        "contact",
        "about",
        "wp-admin",
        "wp-content",
    }:
        return full
    return ""


def _deep_collect_gtm_links(base_url: str, soup: BeautifulSoup, out: Set[str]) -> None:
    for el in soup.select("[data-gtm4wp_product_data]"):
        raw = (el.get("data-gtm4wp_product_data") or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(data, dict):
            continue
        link = (data.get("productlink") or "").strip()
        if not link:
            continue
        if not link.startswith("http"):
            link = urljoin(base_url, link)
        u = _deep_normalize_shop_url(base_url, link)
        if u:
            out.add(u)


def _deep_extract_text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


def _clean_meta_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


# =========================
# 2) أنماط استخراج البلد/السنة/الضمان
# =========================

CARD_COUNTRY_YEAR_PATTERN = re.compile(
    r"([أ-يA-Za-z\u0600-\u06FF][أ-يA-Za-z\u0600-\u06FF\s\-]{0,38}?)\s*[/\\|,،]\s*(?:تاريخ(?:\s*الصنع)?)?\s*(\d{4})",
    re.UNICODE,
)


def extract_country_year_from_card(text: str) -> tuple[str, str]:
    text = _clean_meta_text(text)
    if not text:
        return "", ""
    m = CARD_COUNTRY_YEAR_PATTERN.search(text)
    if not m:
        return "", ""
    country = _clean_meta_text(m.group(1))
    year = _clean_meta_text(m.group(2))
    return country, year


def _extract_country_year_from_text(text: str) -> tuple[str, str]:
    """
    يدعم أنماطًا شائعة مثل:
    - رومانيا / تاريخ 2024
    - إسبانيا / تاريخ 2025
    - بلد المنشأ: اليابان
    - Made in Romania / 2024
    """
    t = _clean_meta_text(text)
    if not t:
        return "", ""

    year = ""
    m_year = re.search(r"\b(20[1-9][0-9])\b", t)
    if m_year:
        year = m_year.group(1)

    country = ""
    m_year_only = re.search(r"(?:^|[\s،,])تاريخ\s*(\d{4})\b", t, flags=re.IGNORECASE)
    if m_year_only and not year:
        year = m_year_only.group(1)

    m_pair = re.search(
        r"([^\|,\n\r/]{2,}?)\s*/\s*(?:تاريخ(?:\s*الصنع)?|date|production\s*date)\s*[:：]?\s*(20[1-9][0-9])",
        t,
        flags=re.IGNORECASE,
    )
    if m_pair:
        country = _clean_meta_text(m_pair.group(1))
        year = m_pair.group(2)
    else:
        m_country = re.search(
            r"(?:بلد(?:\s+المنشأ|\s+الصنع|\s+الإنتاج)?|origin|country(?:\s+of\s+origin)?|made in|manufactured in)\s*[:：-]?\s*([^\|,\n\r/]+)",
            t,
            flags=re.IGNORECASE,
        )
        if m_country:
            country = _clean_meta_text(m_country.group(1))

    if not country and year:
        pre_year = re.split(r"\b20[1-9][0-9]\b", t, maxsplit=1, flags=re.IGNORECASE)[0]
        if "/" in pre_year:
            country = _clean_meta_text(pre_year.split("/", 1)[0])

    country = re.sub(
        r"(?:تاريخ(?:\s*الصنع)?|سنة\s*الصنع|production\s*date|date|بلد(?:\s+المنشأ|\s+الصنع|\s+الإنتاج)?|origin|country(?:\s+of\s+origin)?|made in|manufactured in)\s*[:：-]?\s*",
        " ",
        country,
        flags=re.IGNORECASE,
    )
    country = re.sub(r"[/|,\-]+", " ", country)
    country = _clean_meta_text(country)
    if len(country) > 40:
        country = ""
    return country, year


def extract_warranty_from_text(text: str) -> str:
    """
    أمثلة:
    - الضمان: خمس سنوات
    - الضمان سنتين
    - Warranty: 5 Years
    """
    t = _clean_meta_text(text)
    if not t:
        return ""
    # عربي
    m = re.search(r"الضمان[:\s]*([^\n\r]+)", t)
    if m:
        return _clean_meta_text(m.group(1))
    # إنجليزي
    m = re.search(r"(?:warranty|guarantee)[:\s]*([^\n\r]+)", t, flags=re.IGNORECASE)
    if m:
        return _clean_meta_text(m.group(1))
    return ""


def _is_weak_image_url(url: str) -> bool:
    u = (url or "").strip()
    if not u:
        return True
    low = u.lower()
    if u.startswith("data:"):
        return True
    if any(x in low for x in ("placeholder", "lazy", "1x1", "blank", "loading", "spinner", "icon", ".svg", "logo")):
        return True
    if low.endswith(".gif") and "loading" in low:
        return True
    return False


def _og_image_from_soup(soup: BeautifulSoup, page_url: str) -> str:
    for sel in ("meta[property='og:image']", "meta[name='twitter:image']", "meta[property='og:image:secure_url']"):
        og = soup.select_one(sel)
        if og and og.get("content"):
            cand = urljoin(page_url, og["content"].strip())
            if not _is_weak_image_url(cand):
                return cand
    for img in soup.select("img[src], img[data-src]"):
        cand = _deep_extract_image_url(img, page_url)
        if cand and not _is_weak_image_url(cand):
            return cand
    return ""


def _fetch_og_image_url(page_url: str) -> str:
    """جلب صورة المنتج من og:image — طلب HTTP إضافي (تجنّبه إن أمكن)."""
    if not page_url.startswith(("http://", "https://")):
        return ""
    try:
        soup = _deep_get_soup(page_url)
    except Exception as e:
        log.warning("og_image_fetch_failed url=%s err=%s", page_url, e)
        return ""
    return _og_image_from_soup(soup, page_url)


def resolve_product_image_url(image_url: str, product_url: str = "") -> str:
    u = (image_url or "").strip()
    if u and not _is_weak_image_url(u):
        return u
    if product_url:
        og = _fetch_og_image_url(product_url)
        if og:
            return og
    return u


def _json_ld_product_nodes(data: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        if str(data.get("@type") or "").lower() == "product":
            out.append(data)
        graph = data.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                out.extend(_json_ld_product_nodes(node))
    elif isinstance(data, list):
        for node in data:
            out.extend(_json_ld_product_nodes(node))
    return out


def _parse_json_ld_product(soup: BeautifulSoup) -> Dict[str, Any]:
    for script in soup.select("script[type='application/ld+json']"):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        for node in _json_ld_product_nodes(data):
            offers = node.get("offers")
            price = ""
            if isinstance(offers, dict):
                price = str(offers.get("price") or offers.get("lowPrice") or "")
            elif isinstance(offers, list) and offers:
                price = str((offers[0] or {}).get("price") or "")
            image = node.get("image")
            if isinstance(image, list):
                image = image[0] if image else ""
            if isinstance(image, dict):
                image = image.get("url") or ""
            return {
                "title": str(node.get("name") or "").strip(),
                "description": str(node.get("description") or "").strip(),
                "price": price,
                "image": str(image or "").strip(),
            }
    return {}


def _extract_price_sar_from_text(text: str) -> str:
    blob = _clean_meta_text(text)
    if not blob:
        return ""
    m = re.search(
        r"([\d]{1,3}(?:[,\s]\d{3})*(?:\.\d{1,2})?)\s*(?:ر\.?\s*س|ريال|SAR|ر\.س)",
        blob,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1).replace(" ", "").replace(",", "")
    return ""


def _parse_salla_product_from_soup(soup: BeautifulSoup, product_url: str) -> Dict[str, str]:
    ld = _parse_json_ld_product(soup)
    title = ld.get("title") or _deep_product_title_from_soup(
        soup, {"product_title_selector": "h1, .product-title, [class*='product-title']"}
    )
    price = ld.get("price") or _extract_price_sar_from_text(_deep_extract_text(soup))
    description = _extract_salla_description(soup)
    image = (ld.get("image") or "").strip()
    if image and not image.startswith(("http://", "https://")):
        image = urljoin(product_url, image)
    if not image or _is_weak_image_url(image):
        image = _og_image_from_soup(soup, product_url)
    return {
        "title": title,
        "price": price,
        "description": description,
        "image": image,
    }


def _extract_salla_description(soup: BeautifulSoup) -> str:
    ld = _parse_json_ld_product(soup)
    if ld.get("description") and len(ld["description"]) > 30:
        return ld["description"]
    for sel in (
        "[class*='product-description']",
        "[class*='ProductDescription']",
        ".salla-tab-content",
        ".tab-content",
        "article.product",
        "main [class*='description']",
    ):
        el = soup.select_one(sel)
        if not el:
            continue
        text = _deep_extract_text(el)
        if len(text) > 40:
            return text
    meta = soup.select_one("meta[name='description']")
    if meta and meta.get("content"):
        return _clean_meta_text(meta["content"])
    return ""


def _enrich_salla_product_page(product_url: str) -> Dict[str, str]:
    try:
        soup = _deep_get_soup(product_url)
    except Exception as e:
        log.warning("salla_pdp_failed url=%s err=%s", product_url, e)
        return {}
    return _parse_salla_product_from_soup(soup, product_url)


def _raw_product_needs_pdp_enrich(it: RawProduct) -> bool:
    desc_ok = len((it.description or "").strip()) >= 40
    price_ok = bool(normalize_price(it.price_raw))
    name_ok = len((it.name or "").strip()) >= 3
    return not (desc_ok and price_ok and name_ok)


def _is_pdp_url(url: str, site_key: str) -> bool:
    path = (urlparse(url).path or "").lower()
    if site_key == "almurad" or "almuradstore.com" in (url or "").lower():
        return bool(re.search(r"/products/[^/]+/?$", path))
    if site_key == "brwx" or "brwx.com" in (url or "").lower():
        return bool(re.search(r"/product/[^/]+/?$", path))
    return bool(re.search(r"/(?:product|products)/[^/]+/?$", path))


def _raw_product_from_salla_detail(url: str) -> Optional[RawProduct]:
    detail = _enrich_salla_product_page(url)
    if not detail.get("title"):
        return None
    return RawProduct(
        name=detail["title"],
        price_raw=detail.get("price") or "",
        image_url=detail.get("image") or "",
        product_url=url,
        description=detail.get("description") or "",
    )


def _apply_pdp_detail_to_raw(it: RawProduct, detail: Dict[str, str], product_url: str) -> None:
    if detail.get("title"):
        it.name = detail["title"]
    if detail.get("price"):
        it.price_raw = detail["price"]
    if detail.get("description"):
        it.description = detail["description"]
    img = (detail.get("image") or "").strip()
    if img:
        it.image_url = urljoin(product_url, img) if not img.startswith(("http://", "https://")) else img
    elif not it.image_url:
        it.image_url = img


def _enrich_one_raw_product(it: RawProduct, site_key: str, cfg: Dict[str, Any], is_salla: bool) -> None:
    url = (it.product_url or "").strip()
    if not url.startswith(("http://", "https://")) or not _raw_product_needs_pdp_enrich(it):
        return
    if is_salla:
        detail = _enrich_salla_product_page(url)
    else:
        rows = _scrape_product_detail_as_one(url, cfg)
        detail = (
            {
                "title": rows[0].name,
                "price": rows[0].price_raw,
                "description": rows[0].description,
                "image": rows[0].image_url,
            }
            if rows
            else {}
        )
    _apply_pdp_detail_to_raw(it, detail, url)


def _extract_woo_description(soup: BeautifulSoup) -> str:
    for sel in (
        "div.woocommerce-Tabs-panel--description",
        ".woocommerce-product-details__short-description",
        "#tab-description",
        ".product-description",
        ".summary",
    ):
        el = soup.select_one(sel)
        if el:
            text = _deep_extract_text(el)
            if len(text) > 30:
                return text
    return ""


def _should_enrich_detail_pages(site_key: str) -> bool:
    cfg = SITES_CONFIG.get(site_key) or {}
    return bool(cfg.get("enrich_detail_pages"))


def enrich_raw_products_from_detail_pages(
    items: List[RawProduct],
    site_key: str,
    *,
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> None:
    """زيارة صفحة كل منتج لجلب الاسم والسعر والوصف (طلب واحد لكل منتج، متوازي عند الكثرة)."""
    if not items or not _should_enrich_detail_pages(site_key):
        return
    cfg = SITES_CONFIG.get(site_key) or {}
    is_salla = cfg.get("platform") == "almurad_salla" or site_key == "almurad"
    todo = [it for it in items if _raw_product_needs_pdp_enrich(it)]
    if not todo:
        return

    total = len(todo)
    done = 0
    _universal_report(progress_cb, 38, f"جلب تفاصيل المنتجات (0/{total})...")

    if len(todo) == 1:
        try:
            _enrich_one_raw_product(todo[0], site_key, cfg, is_salla)
        except Exception as e:
            log.warning("pdp_enrich_skip err=%s", e)
        _universal_report(progress_cb, 86, f"اكتملت تفاصيل {total} منتج")
        return

    with ThreadPoolExecutor(max_workers=_PDP_ENRICH_WORKERS) as pool:
        futures = {
            pool.submit(_enrich_one_raw_product, it, site_key, cfg, is_salla): it for it in todo
        }
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                log.warning("pdp_enrich_skip err=%s", e)
            done += 1
            if progress_cb and total:
                _universal_report(
                    progress_cb,
                    38 + int(done / total * 48),
                    f"تفاصيل المنتج {done}/{total}",
                )
    _universal_report(progress_cb, 86, f"اكتملت تفاصيل {total} منتج")


def enrich_universal_items_images(
    items: List[Dict[str, Any]],
    uploads_dir: Path,
    *,
    site_key: str = "",
    download_images: bool = True,
) -> None:
    """تحميل صور + Cloudinary — عطّل download_images للسحب السريع (رابط CDN فقط)."""
    if not items:
        return
    from .cloudinary_uploader import upload_to_cloudinary
    from .image_downloader import download_image, sanitize_filename
    from .parser import parse_tire_name
    from .seo_optimizer import build_seo_fields

    # نفس مجلد Tireex: uploads/products — يُعرض عبر GET /importer/image
    image_dir = Path(uploads_dir) / "products"
    image_dir.mkdir(parents=True, exist_ok=True)

    for item in items:
        product_url = (item.get("product_url") or item.get("url") or "").strip()
        raw_img = (item.get("image_url") or item.get("image") or "").strip()
        if raw_img.startswith(("http://", "https://")) and not _is_weak_image_url(raw_img):
            src_url = raw_img
        else:
            src_url = resolve_product_image_url(raw_img, product_url) if download_images else raw_img
        item["source_image_url"] = src_url

        if not download_images:
            item["image_url"] = src_url
            item["image_status"] = "remote_only"
            item["cloudinary_status"] = "skipped_fast_scrape"
            continue

        parsed = parse_tire_name(item.get("name") or item.get("product_title") or "")
        prod = {
            "brand": item.get("brand") or parsed.get("brand") or "",
            "model": item.get("model") or parsed.get("model") or "",
            "size": item.get("size") or parsed.get("size") or "",
            "load_speed": item.get("load_speed") or parsed.get("load_speed") or "",
        }
        seo = build_seo_fields(prod)
        slug = (seo.get("image_slug") or "").strip()
        if not slug or slug == "tire-product":
            slug = sanitize_filename((item.get("name") or item.get("product_title") or "product")[:120])
        url_slug = (urlparse(product_url).path or "").rstrip("/").split("/")[-1]
        if url_slug:
            slug = sanitize_filename(f"{site_key}-{url_slug}"[:140]) if site_key else sanitize_filename(url_slug[:140])

        local_path, image_status = download_image(src_url, image_dir, slug)
        item["image_local"] = local_path
        item["image_status"] = image_status

        cloud_url = ""
        cloud_status = ""
        if local_path:
            cloud_url, cloud_status = upload_to_cloudinary(local_path, slug)
        elif src_url.startswith(("http://", "https://")):
            cloud_url, cloud_status = upload_to_cloudinary(src_url, slug)

        item["image_cloudinary"] = cloud_url
        item["cloudinary_status"] = cloud_status

        if cloud_status == "uploaded" and cloud_url.startswith("https://res.cloudinary.com/"):
            item["image_url"] = cloud_url
            item["image_status"] = "ok"
        elif image_status in {"downloaded", "exists"} and local_path:
            item["image_url"] = local_path
        else:
            item["image_url"] = src_url
            if cloud_status != "uploaded" and image_status not in {"downloaded", "exists"}:
                item["image_status"] = image_status or "needs_review"


def _deep_extract_image_url(el, page_url: str) -> str:
    if not el:
        return ""
    for attr in ("data-large_image", "data-src", "data-lazy-src", "data-original", "data-image"):
        v = (el.get(attr) or "").strip()
        if v and not _is_weak_image_url(v):
            return urljoin(page_url, v)
    srcset = el.get("srcset") or ""
    if srcset:
        chunks = [c.strip() for c in srcset.split(",") if c.strip()]
        if chunks:
            # آخر عنصر في srcset عادة الأكبر دقة
            part = chunks[-1].split()
            if part:
                candidate = urljoin(page_url, part[0])
                if not _is_weak_image_url(candidate):
                    return candidate
    src = (el.get("src") or "").strip()
    if src and not _is_weak_image_url(src):
        return urljoin(page_url, src)
    return ""


def _deep_collect_product_links(
    site_key: str,
    *,
    max_pages: int,
    start_urls: Optional[List[str]] = None,
) -> List[str]:
    cfg = DEEP_SCAN_SITES[site_key]
    base = cfg["base_url"].rstrip("/")
    visited_pages: Set[str] = set()
    product_links: Set[str] = set()
    seeds = list(start_urls) if start_urls else list(cfg["start_urls"])
    to_visit: List[str] = seeds
    selectors: List[str] = list(cfg.get("product_link_selectors") or [])
    pages_opened = 0

    while to_visit and pages_opened < max_pages:
        url = to_visit.pop(0)
        if url in visited_pages:
            continue
        visited_pages.add(url)
        pages_opened += 1
        try:
            soup = _deep_get_soup(url)
        except Exception as e:
            log.warning("deep_scan listing skip url=%s err=%s", url, e)
            continue

        for sel in selectors:
            for a in soup.select(sel):
                href = a.get("href")
                u = _deep_normalize_shop_url(base, href or "")
                if u:
                    product_links.add(u)

        if cfg.get("use_gtm_embed"):
            _deep_collect_gtm_links(base, soup, product_links)

        for a in soup.select(
            "a.next.page-numbers, a[rel='next'], .woocommerce-pagination a.next, "
            ".wd-pagination a.next, .wd-pagination a.next.page-numbers, "
            "a.page-numbers, a.pagination-next, "
            "a[href*='page='], nav a[href*='?page=']"
        ):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(base, href)
            if full not in visited_pages and full not in to_visit:
                if urlparse(full).netloc == urlparse(base).netloc:
                    to_visit.append(full)

    return sorted(product_links)


def _deep_parse_product_row(site_key: str, url: str, target_brand: str) -> Optional[Dict[str, str]]:
    cfg = DEEP_SCAN_SITES[site_key]
    try:
        soup = _deep_get_soup(url)
    except Exception as e:
        log.warning("deep_scan product skip url=%s err=%s", url, e)
        return None

    title = _deep_product_title_from_soup(soup, cfg)

    brand: Optional[str] = None
    if cfg.get("brand_selector"):
        brand = _deep_extract_text(soup.select_one(cfg["brand_selector"])) or None

    if not brand:
        if _deep_title_matches_brand(title, target_brand) or _deep_url_matches_brand(url, target_brand):
            brand = target_brand.strip()
        else:
            return None
    else:
        if brand.lower() != target_brand.strip().lower() and not _deep_title_matches_brand(title, target_brand):
            return None

    price = ""
    for sel in (cfg["price_selector"] or "p.price").split(","):
        el = soup.select_one(sel.strip())
        if el:
            price = _deep_extract_text(el)
            if price:
                break

    img_el = None
    for sel in (cfg["image_selector"] or "img").split(","):
        img_el = soup.select_one(sel.strip())
        if img_el:
            break
    image_url = _deep_extract_image_url(img_el, url) if img_el else ""

    description = ""
    for sel in (cfg["description_selector"] or "div").split(","):
        el = soup.select_one(sel.strip())
        if el:
            description = _deep_extract_text(el)
            if description:
                break

    meta_text = ""
    for sel in (
        ".product-card-year, .product-box-year, .year, .origin, .country, .product_meta, "
        ".woocommerce-product-attributes, .shop_attributes, .summary"
    ).split(","):
        el = soup.select_one(sel.strip())
        if el:
            meta_text = _deep_extract_text(el)
            if meta_text:
                break

    page_text = _deep_extract_text(soup.body or soup)
    combined = " ".join(x for x in [meta_text, description, page_text] if x)

    country, year = extract_country_year_from_card(combined)
    if not country and not year:
        country, year = _extract_country_year_from_text(combined)

    warranty = extract_warranty_from_text(combined)

    return {
        "url": url,
        "title": title,
        "brand": brand or target_brand,
        "price": price,
        "image": image_url,
        "description": description,
        "year": year,
        "country": country,
        "warranty": warranty,
    }


# =========================
# 3) أدوات مساعدة عامة
# =========================

def build_page_url(base_url: str, page: int, page_param: str) -> str:
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs[page_param] = [str(page)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _kafaratplus_brand_slug(url: str) -> str:
    """مثل /Sailun أو /continental من رابط صفحة الماركة."""
    path = (urlparse(url).path or "").strip("/")
    if not path or "/" in path:
        return ""
    slug = path.split("/")[0].strip().lower()
    if slug in _KAFARATPLUS_SKIP_PATHS:
        return ""
    return slug


def _kafaratplus_skip_limit_page_url(base_url: str, page: int) -> Optional[str]:
    """
    صفحات الماركة الحديثة: ?skip=16&limit=16
    الصفحة 1 = الرابط كما هو، الصفحة 2 = skip+limit، ...
    """
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    if "limit" not in qs and "skip" not in qs:
        return None
    per_page = int((qs.get("limit") or ["16"])[0] or 16)
    base_skip = int((qs.get("skip") or ["0"])[0] or 0)
    skip = base_skip + max(0, page - 1) * per_page
    new_qs = {k: list(v) for k, v in qs.items()}
    new_qs["skip"] = [str(skip)]
    new_qs["limit"] = [str(per_page)]
    return urlunparse(parsed._replace(query=urlencode(new_qs, doseq=True)))


def _listing_page_candidates(base_url: str, page: int, page_param: str = "page") -> List[str]:
    """روابط ترقيم محتملة — كفرات بلس يستخدم skip/limit لصفحات الماركة."""
    skip_url = _kafaratplus_skip_limit_page_url(base_url, page)
    if skip_url:
        return [skip_url]

    if page <= 1:
        return [base_url]
    out: List[str] = []
    seen: Set[str] = set()

    def _add(u: str) -> None:
        u = (u or "").strip()
        if u and u not in seen:
            seen.add(u)
            out.append(u)

    parsed = urlparse(base_url)
    path = (parsed.path or "").rstrip("/")
    _add(build_page_url(base_url, page, page_param))
    _add(build_page_url(base_url, page, "paged"))
    base_path = re.sub(r"/page/\d+/?$", "", path, flags=re.IGNORECASE)
    page_path = f"{base_path}/page/{page}/" if base_path else f"/page/{page}/"
    _add(urlunparse(parsed._replace(path=page_path)))
    return out


def _is_probable_product_detail(url: str, soup: BeautifulSoup, cfg: Dict[str, Any]) -> bool:
    path = (urlparse(url).path or "").lower()
    if "/categories/" in path or path.rstrip("/") in {"/products", "/shop"}:
        return False
    if "/product/" in path or re.search(r"/products/[^/]+/?$", path):
        return True
    has_cards = bool(soup.select(cfg.get("product_selector", "")))
    has_pdp_title = bool(
        soup.select_one(
            "h1.product_title, h1.product-title, .product_title, .summary .product_title"
        )
    )
    return has_pdp_title and not has_cards


def _scrape_product_detail_as_one(url: str, cfg: Dict[str, Any]) -> List[RawProduct]:
    """صفحة منتج واحد (ليس قائمة) — نستخرج منتجاً واحداً بدل البحث عن كروت."""
    if cfg.get("platform") == "almurad_salla":
        row = _raw_product_from_salla_detail(url)
        return [row] if row else []

    try:
        resp = _http_session().get(url, timeout=22)
    except Exception as e:
        log.warning("product_detail_failed url=%s err=%s", url, e)
        return []
    if resp.status_code != 200:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    title = _deep_product_title_from_soup(soup, cfg)
    if not title:
        return []

    price_raw = ""
    for sel in (cfg.get("price_selector") or "p.price, .price").split(","):
        el = soup.select_one(sel.strip())
        if el:
            price_raw = _deep_extract_text(el)
            if price_raw:
                break

    img_el = soup.select_one(cfg.get("image_selector", "img") or "img")
    image_url = _deep_extract_image_url(img_el, url) if img_el else ""

    country, year, warranty = "", "", ""
    if "kafaratplus.com" in url or cfg.get("base_url", "").endswith("kafaratplus.com"):
        country, year, warranty = _extract_card_meta_for_kafaratplus(soup.body or soup)
        if not country and not year:
            country, year, warranty = _enrich_kafaratplus_from_product_page(url)
    else:
        combined = _deep_extract_text(soup.body or soup)
        country, year = extract_country_year_from_card(combined)
        if not country and not year:
            country, year = _extract_country_year_from_text(combined)
        warranty = extract_warranty_from_text(combined)

    description = _extract_woo_description(soup)
    if not description:
        ld = _parse_json_ld_product(soup)
        description = (ld.get("description") or "").strip()

    return [
        RawProduct(
            name=title,
            price_raw=price_raw,
            image_url=image_url,
            product_url=url,
            year=year,
            country=country,
            warranty=warranty,
            description=description,
        )
    ]


def normalize_price(raw: str) -> str:
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


def normalize_brand(value: str) -> str:
    v = str(value or "").strip().lower()
    if v in {"ألفا", "الفا", "alpha"}:
        return "alpha"
    if v in {"لاوفين", "laufenn"}:
        return "laufenn"
    return v


# =========================
# 4) تحليل اسم الكفر
# =========================

@dataclass
class ParsedTire:
    brand: str
    model: str
    size: str
    width: str
    profile: str
    rim: str
    load_speed: str


def parse_tire_name(name: str) -> ParsedTire:
    s = str(name or "").strip()
    parts = s.split()
    brand = ""
    # أسماء كفرات بلس غالباً تبدأ بالمقاس وتنتهي بالماركة العربية
    for ar, en in (
        ("سايلون", "Sailun"),
        ("سايلن", "Sailun"),
        ("كونتيننتال", "Continental"),
        ("اكسيليرا", "Accelera"),
        ("أكسيليرا", "Accelera"),
        ("ميشلان", "Michelin"),
        ("هانكوك", "Hankook"),
        ("جوديير", "Goodyear"),
        ("بريدجستون", "Bridgestone"),
    ):
        if ar in s:
            brand = en
            break
    if not brand:
        brand = parts[0] if parts else ""
    model = parts[1] if len(parts) > 1 else ""

    size_match = re.search(r"(\d{3})\s*/\s*(\d{2})\s*R\s*(\d{2})", s, flags=re.IGNORECASE)
    size = ""
    width = ""
    profile = ""
    rim = ""
    if size_match:
        width = size_match.group(1)
        profile = size_match.group(2)
        rim = size_match.group(3)
        size = f"{width}/{profile}R{rim}"

    load_speed_match = re.search(r"\b(\d{2,3}[A-Z])\b", s)
    load_speed = load_speed_match.group(1) if load_speed_match else ""

    return ParsedTire(
        brand=brand,
        model=model,
        size=size,
        width=width,
        profile=profile,
        rim=rim,
        load_speed=load_speed,
    )


# =========================
# 5) SEO ذكي لكل إطار
# =========================

def build_seo_fields(parsed: ParsedTire, year: str = "", country: str = "", pattern: str = "") -> Dict[str, str]:
    brand = parsed.brand or ""
    model = parsed.model or ""
    size = parsed.size or ""
    load_speed = parsed.load_speed or ""
    year_txt = year or "غير محددة"
    country_txt = country or "غير محدد"
    pattern_txt = pattern or "غير محددة"

    seo_title = (
        f"كفر {brand} {model} مقاس {size} {load_speed} "
        f"- سنة {year_txt} - أداء عالي وثبات ممتاز"
    )

    meta_description = (
        f"كفر {brand} {model} مقاس {size} {load_speed} يتميز بثبات ممتاز وتماسك عالي "
        f"على الطرق المختلفة. سنة الصنع: {year_txt}، بلد المنشأ: {country_txt}، "
        f"نقشة الإطار: {pattern_txt}. خيار مثالي للاستخدام اليومي والرحلات."
    )

    keywords = (
        f"كفر {brand}, اطارات {brand}, {brand} {model}, كفر {model}, "
        f"مقاس {size}, اطارات سيارات {size}, كفرات {brand}, "
        f"افضل كفرات {brand}, سعر كفر {brand} {model}, "
        f"شراء كفر {brand}, اطارات {country_txt}, كفر نقشة {pattern_txt}"
    )

    image_alt = f"صورة كفر {brand} {model} مقاس {size} نقشة {pattern_txt}"

    return {
        "seo_title": seo_title,
        "meta_description": meta_description,
        "keywords": keywords,
        "image_alt_text": image_alt,
    }


# =========================
# 6) نموذج المنتج الخام + Scraper عام
# =========================

@dataclass
class RawProduct:
    name: str
    price_raw: str
    image_url: str
    product_url: str
    year: str = ""
    warranty: str = ""
    country: str = ""
    pattern: str = ""
    description: str = ""


def _extract_card_meta_for_kafaratplus(card) -> tuple[str, str, str]:
    """
    يقرأ نص الكرت كامل، ويستخرج:
    - country/year من سطر مثل: رومانيا / تاريخ 2025
    - warranty من سطر منفصل مثل: الضمان: خمس سنوات
    بدون خلط بينهما.
    """
    country = ""
    year = ""
    warranty = ""

    for sel in (
        ".product-box-year",
        ".product-box-origin",
        ".product-card-year",
        ".product-card-origin",
        "[class*='year']",
        "[class*='origin']",
        "small",
        ".meta",
    ):
        el = card.select_one(sel)
        if not el:
            continue
        chunk = _deep_extract_text(el)
        c, y = extract_country_year_from_card(chunk)
        if not c and not y:
            c, y = _extract_country_year_from_text(chunk)
        if c and not country:
            country = c
        if y and not year:
            year = y

    card_text = _deep_extract_text(card)
    lines = [l.strip() for l in re.split(r"[\n\r]+", card_text) if l.strip()]

    for line in lines:
        c, y = extract_country_year_from_card(line)
        if not c and not y:
            c, y = _extract_country_year_from_text(line)
        if c and not country:
            country = c
        if y and not year:
            year = y
        if not warranty:
            w = extract_warranty_from_text(line)
            if w:
                warranty = w

    if not country and not year:
        country, year = extract_country_year_from_card(card_text)
    if not country and not year:
        country, year = _extract_country_year_from_text(card_text)
    if not warranty:
        warranty = extract_warranty_from_text(card_text)

    return country, year, warranty


def _kafaratplus_listing_meta_cache(
    max_pages: int,
    start_urls: Optional[List[str]] = None,
) -> Dict[str, tuple[str, str, str]]:
    """يجمع بلد/سنة/ضمان من كروت القائمة (حيث تظهر فعلياً على كفرات بلس)."""
    cfg = SITES_CONFIG["kafaratplus"]
    bases = [u for u in (start_urls or [cfg["base_url"] + "/shop/"]) if u]
    cache: Dict[str, tuple[str, str, str]] = {}
    for base in bases:
        for page in range(1, max(1, int(max_pages or 1)) + 1):
            page_url = build_page_url(base, page, cfg.get("pagination_param", "page"))
            page_items = scrape_single_page(page_url, cfg, enrich_product_pages=False)
            if not page_items:
                break
            for it in page_items:
                key = (it.product_url or "").strip().rstrip("/").lower()
                if key and (it.country or it.year or it.warranty):
                    cache[key] = (it.country, it.year, it.warranty)
    log.info("kafaratplus_listing_meta_cache size=%s", len(cache))
    return cache


def _apply_listing_meta(row: Dict[str, str], listing_meta: Dict[str, tuple[str, str, str]]) -> None:
    key = (row.get("url") or "").strip().rstrip("/").lower()
    if not key or key not in listing_meta:
        return
    c, y, w = listing_meta[key]
    if c and not (row.get("country") or "").strip():
        row["country"] = c
    if y and not (row.get("year") or "").strip():
        row["year"] = y
    if w and not (row.get("warranty") or "").strip():
        row["warranty"] = w


def _enrich_kafaratplus_from_product_page(product_url: str) -> tuple[str, str, str]:
    """إذا الكرت لا يحتوي البلد/السنة، نحاول من صفحة المنتج."""
    if not product_url:
        return "", "", ""
    try:
        soup = _deep_get_soup(product_url)
    except Exception as e:
        log.warning("kafaratplus_enrich_failed url=%s err=%s", product_url, e)
        return "", "", ""

    meta_text = ""
    for sel in (
        ".product-box-year, .product-box-origin, .product-card-year, .year, .origin, "
        ".country, .product_meta, .woocommerce-product-attributes, .shop_attributes, .summary"
    ).split(","):
        el = soup.select_one(sel.strip())
        if el:
            meta_text = _deep_extract_text(el)
            if meta_text:
                break

    page_text = _deep_extract_text(soup.body or soup)
    combined = " ".join(x for x in [meta_text, page_text] if x)
    country, year = extract_country_year_from_card(combined)
    if not country and not year:
        country, year = _extract_country_year_from_text(combined)
    warranty = extract_warranty_from_text(combined)
    return country, year, warranty


def _card_select_one(card, selector_csv: str):
    for sel in str(selector_csv or "").split(","):
        el = card.select_one(sel.strip())
        if el:
            return el
    return None


def _extract_price_from_card_text(card_text: str) -> str:
    """أول سعر منطقي في نص الكرت (يتجاهل أقساط tabby)."""
    for m in re.finditer(r"\b(\d{2,4}(?:\.\d{1,2})?)\b", card_text or ""):
        try:
            v = float(m.group(1))
        except Exception:
            continue
        if 80 <= v <= 15000:
            return m.group(1)
    return ""


def _find_product_link_in_card(card, page_url: str) -> str:
    for a in card.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        low = href.lower()
        if any(x in low for x in ("cart", "checkout", "login", "javascript:")):
            continue
        full = urljoin(page_url, href)
        path = (urlparse(full).path or "").lower()
        if path in {"", "/"}:
            continue
        return full
    return ""


_ALMURAD_PRODUCT_PATH_RE = re.compile(r"^/products/[^/]+/?$", re.IGNORECASE)
_ALMURAD_CATEGORY_PATH_RE = re.compile(r"^/categories/\d+", re.IGNORECASE)
_ALMURAD_PRODUCT_URL_IN_HTML_RE = re.compile(
    r"https?://(?:www\.)?almuradstore\.com/products/[^\s\"'<>]+",
    re.IGNORECASE,
)
_ALMURAD_TOTAL_PRODUCTS_RE = re.compile(
    r"إجمالي\s*(\d+)\s*منتج",
    re.IGNORECASE,
)
_ALMURAD_NOISE_NAMES = frozenset(
    {
        "إضافة للسلة",
        "اضافة للسلة",
        "أضف للسلة",
        "أضف إلى السلة",
        "المزيد",
        "تصفية",
        "الأحدث",
    }
)


def _almurad_is_product_url(full: str) -> bool:
    path = (urlparse(full).path or "").rstrip("/")
    return bool(_ALMURAD_PRODUCT_PATH_RE.match(path))


def _almurad_is_category_url(url: str) -> bool:
    path = (urlparse(url).path or "").rstrip("/")
    return bool(_ALMURAD_CATEGORY_PATH_RE.match(path))


def _almurad_listing_base_url(url: str) -> str:
    """رابط القائمة بدون ?page= لتكرار الترقيم بشكل صحيح."""
    parsed = urlparse((url or "").strip())
    qs = parse_qs(parsed.query)
    for key in ("page", "paged"):
        qs.pop(key, None)
    q = urlencode(qs, doeq=True)
    return urlunparse(parsed._replace(query=q))


def _almurad_parse_total_products(soup: BeautifulSoup) -> int:
    text = _deep_extract_text(soup.body or soup)
    m = _ALMURAD_TOTAL_PRODUCTS_RE.search(text)
    if m:
        try:
            return max(0, int(m.group(1)))
        except ValueError:
            pass
    return 0


def _scrape_almurad_from_page_scripts(html: str, page_url: str) -> List[RawProduct]:
    """استخراج روابط المنتجات من JSON مضمّن (عند فشل الروابط الظاهرة في HTML)."""
    items: List[RawProduct] = []
    seen: Set[str] = set()
    if not html:
        return items

    for block in re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(block)
        except (json.JSONDecodeError, TypeError):
            continue
        for node in _json_ld_product_nodes(data):
            url = str(node.get("url") or "").strip()
            if not url:
                continue
            full = urljoin(page_url, url.split("?")[0])
            if not _almurad_is_product_url(full):
                continue
            key = full.rstrip("/").lower()
            if key in seen:
                continue
            seen.add(key)
            offers = node.get("offers")
            price = ""
            if isinstance(offers, dict):
                price = str(offers.get("price") or "")
            items.append(
                RawProduct(
                    name=str(node.get("name") or "").strip(),
                    price_raw=price,
                    image_url=str(node.get("image") or ""),
                    product_url=full.rstrip("/"),
                )
            )

    for full in _almurad_product_urls_in_html(html, page_url):
        key = full.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(
            RawProduct(name="", price_raw="", image_url="", product_url=full.rstrip("/"))
        )
    return items


def _almurad_product_urls_in_html(html: str, page_url: str) -> Set[str]:
    found: Set[str] = set()
    for raw in _ALMURAD_PRODUCT_URL_IN_HTML_RE.findall(html or ""):
        full = urljoin(page_url, raw.split("?")[0].rstrip("/") + "/")
        if _almurad_is_product_url(full):
            found.add(full.rstrip("/"))
    return found


def _almurad_effective_max_pages(total_products: int, per_page: int, max_pages: int) -> int:
    cap = max(1, min(int(max_pages or 1), 40))
    if total_products <= 0 or per_page <= 0:
        return cap
    needed = (total_products + per_page - 1) // per_page
    return min(cap, max(1, needed + 1))


def _scrape_almurad_catalog(
    category_url: str,
    *,
    max_pages: int = 10,
    limit: int = 0,
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> List[RawProduct]:
    """سحب كل منتجات فئة/قائمة المراد (مثل /categories/1125208/جنوط) مع ترقيم ?page=."""
    base = _almurad_listing_base_url(category_url)
    all_items: List[RawProduct] = []
    seen_urls: Set[str] = set()
    total_expected = 0
    per_page = 0
    pages_cap = max(1, min(int(max_pages or 1), 40))

    for page in range(1, pages_cap + 1):
        if progress_cb:
            _universal_report(
                progress_cb,
                5 + int((page - 1) / max(pages_cap, 1) * 30),
                f"قائمة المراد — صفحة {page}/{pages_cap}",
            )
        page_url = build_page_url(base, page, "page") if page > 1 else base
        log.info("almurad_catalog page=%s url=%s", page, page_url)
        try:
            soup = _deep_get_soup(page_url)
        except Exception as e:
            log.warning("almurad_catalog_page_failed url=%s err=%s", page_url, e)
            if page == 1:
                break
            continue

        if page == 1:
            total_expected = _almurad_parse_total_products(soup)

        page_items = _scrape_almurad_listing(soup, page_url)
        if not page_items:
            page_items = _scrape_almurad_from_page_scripts(str(soup), page_url)
        if not page_items:
            if page == 1:
                log.warning("almurad_catalog_empty url=%s", page_url)
            break

        if per_page == 0:
            per_page = max(1, len(page_items))
        if page == 1 and total_expected > 0:
            pages_cap = _almurad_effective_max_pages(total_expected, per_page, pages_cap)

        new_on_page = 0
        for it in page_items:
            key = (it.product_url or "").strip().rstrip("/").lower()
            if not key or key in seen_urls:
                continue
            seen_urls.add(key)
            all_items.append(it)
            new_on_page += 1

        if new_on_page == 0:
            break
        if total_expected and len(all_items) >= total_expected:
            break
        if limit > 0 and len(all_items) >= limit:
            all_items = all_items[:limit]
            break

    _universal_report(
        progress_cb,
        36,
        f"تم جمع {len(all_items)} منتج من القائمة"
        + (f" (متوقع {total_expected})" if total_expected else ""),
    )
    log.info(
        "almurad_catalog_done url=%s count=%s expected=%s pages_used=%s",
        base,
        len(all_items),
        total_expected,
        pages_cap,
    )
    return all_items


def _is_almurad_site(url: str, cfg: Dict[str, Any]) -> bool:
    base = (cfg.get("base_url") or "").lower()
    return cfg.get("platform") == "almurad_salla" or "almuradstore.com" in (url or "").lower() or "almuradstore.com" in base


def _scrape_almurad_listing(soup: BeautifulSoup, page_url: str) -> List[RawProduct]:
    """قائمة منتجات Salla (المراد): العناوين غالباً h2/h3 وروابط /products/{slug}."""
    items: List[RawProduct] = []
    seen_urls: Set[str] = set()
    by_url: Dict[str, RawProduct] = {}

    html = str(soup) if soup else ""
    for extra_url in _almurad_product_urls_in_html(html, page_url):
        if extra_url not in seen_urls:
            seen_urls.add(extra_url)
            by_url[extra_url] = RawProduct(
                name="",
                price_raw="",
                image_url="",
                product_url=extra_url,
            )

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        full = urljoin(page_url, href.split("?")[0])
        if not _almurad_is_product_url(full):
            continue
        if full in seen_urls:
            continue

        card = a
        for _ in range(10):
            parent = card.parent
            if parent is None:
                break
            card = parent
            txt = _deep_extract_text(card)
            if len(txt) > 30 and ("ر.س" in txt or "ريال" in txt or "SAR" in txt.upper()):
                break

        name = ""
        for heading in card.select("h2, h3, h4"):
            t = _deep_extract_text(heading)
            if t and len(t) > 3:
                name = t
                break
        if not name:
            name = _deep_extract_text(a)
        if not name or len(name) < 3 or name.strip() in _ALMURAD_NOISE_NAMES:
            continue

        card_text = _deep_extract_text(card)
        price_raw = _extract_price_from_card_text(card_text)
        image_url = ""
        for img_el in card.select("img"):
            image_url = _deep_extract_image_url(img_el, page_url)
            if image_url:
                break
        # لا نفتح صفحة المنتج هنا — التفاصيل تُجلب لاحقاً دفعة واحدة (أسرع).

        key = full.rstrip("/")
        seen_urls.add(key)
        by_url[key] = RawProduct(
            name=name,
            price_raw=price_raw,
            image_url=image_url,
            product_url=key,
            year="",
            country="",
            warranty="",
        )

    items.extend(by_url.values())
    return items


def _scrape_kafaratplus_modern_listing(soup: BeautifulSoup, page_url: str) -> List[RawProduct]:
    """
    صفحات مثل /Sailun و /continental — المنتجات في h3 وليس product-box دائماً.
  """
    items: List[RawProduct] = []
    seen_names: Set[str] = set()

    for h3 in soup.find_all("h3"):
        name = _deep_extract_text(h3)
        if not name or not _KAFARATPLUS_TIRE_TITLE_RE.search(name):
            continue
        if name in seen_names:
            continue

        card = h3.parent
        for _ in range(6):
            if card is None:
                break
            text = _deep_extract_text(card)
            if len(text) > len(name) + 10:
                break
            card = card.parent
        card = card or h3

        card_text = _deep_extract_text(card)
        country, year, warranty = _extract_card_meta_for_kafaratplus(card)
        price_raw = _extract_price_from_card_text(card_text)
        product_url = _find_product_link_in_card(card, page_url)
        img_el = card.select_one("img")
        image_url = _deep_extract_image_url(img_el, page_url) if img_el else ""

        seen_names.add(name)
        items.append(
            RawProduct(
                name=name,
                price_raw=price_raw,
                image_url=image_url,
                product_url=product_url,
                year=year,
                country=country,
                warranty=warranty,
            )
        )

    return items


def scrape_single_page(url: str, cfg: Dict[str, Any], *, enrich_product_pages: bool = True) -> List[RawProduct]:
    items: List[RawProduct] = []

    try:
        resp = _http_session().get(url, timeout=22)
    except Exception as e:
        log.warning("request_failed url=%s err=%s", url, e)
        return items

    if resp.status_code != 200:
        log.warning("bad_status url=%s status=%s", url, resp.status_code)
        return items

    soup = BeautifulSoup(resp.text, "html.parser")

    if _is_almurad_site(url, cfg) and _almurad_is_category_url(url):
        return _scrape_almurad_listing(soup, url)

    if _is_probable_product_detail(url, soup, cfg):
        return _scrape_product_detail_as_one(url, cfg)

    if _is_almurad_site(url, cfg):
        almurad_items = _scrape_almurad_listing(soup, url)
        if almurad_items:
            return almurad_items

    is_kafaratplus = "kafaratplus.com" in url or cfg.get("base_url", "").endswith("kafaratplus.com")
    if is_kafaratplus and (_kafaratplus_brand_slug(url) or "skip=" in url.lower()):
        modern = _scrape_kafaratplus_modern_listing(soup, url)
        if modern:
            return modern

    for card in soup.select(cfg["product_selector"]):
        title_el = _card_select_one(card, cfg["title_selector"])
        price_el = _card_select_one(card, cfg["price_selector"])
        img_el = card.select_one(cfg["image_selector"]) or card.select_one("img")
        link_el = _card_select_one(card, cfg.get("link_selector", "a")) or card.select_one("a")

        name = title_el.get_text(strip=True) if title_el else ""
        price_raw = price_el.get_text(strip=True) if price_el else ""
        image_url = img_el.get("src", "") if img_el else ""
        product_url = link_el.get("href", "") if link_el else ""
        image_url = urljoin(url, image_url) if image_url else ""
        product_url = urljoin(url, product_url) if product_url else ""
        if img_el and not _is_weak_image_url(image_url):
            better = _deep_extract_image_url(img_el, url)
            if better:
                image_url = better
        image_url = resolve_product_image_url(image_url, product_url)

        country = ""
        year = ""
        warranty = ""

        if is_kafaratplus:
            country, year, warranty = _extract_card_meta_for_kafaratplus(card)
            if enrich_product_pages and (not country or not year) and product_url:
                ec, ey, ew = _enrich_kafaratplus_from_product_page(product_url)
                if not country and ec:
                    country = ec
                if not year and ey:
                    year = ey
                if not warranty and ew:
                    warranty = ew
        else:
            # مواقع أخرى: نحاول من عناصر meta داخل الكرت
            meta_text = ""
            for sel in (
                ".product-card-year, .product-box-year, .year, .origin, .country, [class*='year'], [class*='origin']"
            ).split(","):
                el = card.select_one(sel.strip())
                if el:
                    meta_text = _deep_extract_text(el)
                    if meta_text:
                        break
            card_text = _deep_extract_text(card)
            if not meta_text:
                meta_text = card_text
            country, year = extract_country_year_from_card(card_text)
            if not country and not year:
                country, year = _extract_country_year_from_text(meta_text)
            warranty = extract_warranty_from_text(card_text)

        if not name:
            continue

        items.append(
            RawProduct(
                name=name,
                price_raw=price_raw,
                image_url=image_url,
                product_url=product_url,
                year=year,
                country=country,
                warranty=warranty,
            )
        )

    if is_kafaratplus and not items:
        items = _scrape_kafaratplus_modern_listing(soup, url)

    return items


def scrape_products(
    site_key: str,
    category_url: str,
    *,
    max_pages: int = 10,
    limit: int = 0,
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> List[RawProduct]:
    if site_key not in SITES_CONFIG:
        raise ValueError(f"Unknown site_key={site_key}")

    category_url = (category_url or "").strip()
    if _is_pdp_url(category_url, site_key):
        cfg = SITES_CONFIG[site_key]
        if cfg.get("platform") == "almurad_salla":
            one = _raw_product_from_salla_detail(category_url)
            return [one] if one else []
        pdp = _scrape_product_detail_as_one(category_url, cfg)
        if pdp:
            out = pdp[:1] if limit == 1 else pdp
            return out[:limit] if limit > 0 else out

    cfg = SITES_CONFIG[site_key]
    if cfg.get("platform") == "almurad_salla":
        eff_limit = limit if limit > 0 else 0
        return _scrape_almurad_catalog(
            category_url,
            max_pages=max_pages,
            limit=eff_limit,
            progress_cb=progress_cb,
        )

    page_param = cfg.get("pagination_param", "page")

    all_items: List[RawProduct] = []
    seen_urls: Set[str] = set()

    for page in range(1, max_pages + 1):
        page_items: List[RawProduct] = []
        for page_url in _listing_page_candidates(category_url, page, page_param):
            log.info("scraping site=%s page=%s url=%s", site_key, page, page_url)
            page_items = scrape_single_page(page_url, cfg)
            if page_items:
                break

        if not page_items:
            if page == 1:
                log.warning("listing_empty site=%s url=%s", site_key, category_url)
            break

        for it in page_items:
            key = (it.product_url or it.name or "").strip().lower()
            if key and key in seen_urls:
                continue
            if key:
                seen_urls.add(key)
            all_items.append(it)

        if limit > 0 and len(all_items) >= limit:
            all_items = all_items[:limit]
            break

    return all_items


# =========================
# 7) تحويل Brand Deep Scan إلى Universal Product
# =========================

def _deep_row_to_universal_product(row: Dict[str, str], target_brand: str) -> Dict[str, Any]:
    name = row.get("title") or ""
    parsed = parse_tire_name(name)
    price = normalize_price(row.get("price") or "")
    product_url = (row.get("url") or "").strip()
    image_url = (row.get("image") or "").strip()
    description = (row.get("description") or "").strip() or (
        f"كفر {parsed.brand} {parsed.model} مقاس {parsed.size} — مطابقة Brand Deep Scan للماركة {target_brand}."
    )
    year = (row.get("year") or "").strip()
    country = (row.get("country") or "").strip()
    pattern = (row.get("pattern") or "").strip()
    seo = build_seo_fields(parsed, year, country, pattern)
    product_title = " ".join(
        x for x in [parsed.brand, parsed.model, parsed.size, parsed.load_speed] if x
    ).strip()
    return {
        "name": name,
        "product_title": product_title,
        "brand": parsed.brand,
        "model": parsed.model,
        "size": parsed.size,
        "width": parsed.width,
        "profile": parsed.profile,
        "rim": parsed.rim,
        "load_speed": parsed.load_speed,
        "price": price,
        "product_url": product_url,
        "image_url": image_url,
        "year": year,
        "country": country,
        "pattern": pattern,
        "description": description,
        "seo_title": seo["seo_title"],
        "meta_description": seo["meta_description"],
        "keywords": seo["keywords"],
        "image_alt_text": seo["image_alt_text"],
        "warranty": (row.get("warranty") or "").strip(),
        "status": "needs_review" if (not parsed.size or not price) else "ok",
    }


def _universal_report(
    progress_cb: Optional[Callable[[int, str], None]], pct: int, message: str
) -> None:
    if not progress_cb:
        return
    try:
        progress_cb(max(1, min(99, int(pct))), message)
    except Exception:
        pass


def _universal_effective_limit(limit: int) -> int:
    try:
        v = int(limit or 0)
    except Exception:
        return 0
    return max(0, v)


def run_universal_import(
    site_key: str,
    category_url: str,
    *,
    max_pages: int = 10,
    limit: int = 0,
    brand: str = "",
    exports_root: Path = Path("exports"),
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> Dict[str, Any]:
    eff_limit = _universal_effective_limit(limit)
    pages = max(1, min(int(max_pages or 10), 40))
    _universal_report(progress_cb, 3, "جاري جلب قائمة المنتجات...")
    try:
        raw_items = scrape_products(
            site_key,
            category_url,
            max_pages=pages,
            limit=eff_limit,
            progress_cb=progress_cb,
        )
    except requests.RequestException as e:
        raise ValueError(f"فشل الاتصال بمتجر {site_key}: {e}") from e
    if raw_items and not _is_pdp_url(category_url, site_key):
        enrich_raw_products_from_detail_pages(raw_items, site_key, progress_cb=progress_cb)

    _universal_report(progress_cb, 88, f"تجهيز {len(raw_items)} منتج للتصدير...")
    products: List[Dict[str, Any]] = []
    rim_site = site_key in ("almurad", "brwx")
    seen: Set[str] = set()
    selected_brand = normalize_brand(brand)
    url_brand = _kafaratplus_brand_slug(category_url) if site_key == "kafaratplus" else ""

    for item in raw_items:
        parsed = parse_tire_name(item.name)
        price = normalize_price(item.price_raw)
        product_brand = normalize_brand(parsed.brand)
        if not product_brand or re.match(r"^\d", product_brand):
            if url_brand:
                product_brand = normalize_brand(url_brand)
        if selected_brand:
            if rim_site:
                if not (
                    _deep_title_matches_brand(item.name, brand)
                    or _deep_url_matches_brand(item.product_url, brand)
                ):
                    continue
            elif product_brand != selected_brand and not _deep_title_matches_brand(item.name, brand):
                if not (url_brand and url_brand == selected_brand):
                    continue

        if rim_site:
            desc_src = (item.description or item.name or "").strip()
            seo = {
                "seo_title": (item.name or "")[:160],
                "meta_description": desc_src[:240],
                "keywords": (item.name or "")[:200],
                "image_alt_text": (item.name or "منتج")[:120],
            }
            product_title = (item.name or "").strip()
        else:
            seo = build_seo_fields(parsed, item.year, item.country, item.pattern)
            product_title = " ".join(
                x for x in [parsed.brand, parsed.model, parsed.size, parsed.load_speed] if x
            ).strip()
            if not product_title:
                product_title = (item.name or "").strip()

        key = (item.product_url or "").strip().lower() or (item.name or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)

        description = (item.description or "").strip()
        if not description:
            if parsed.size:
                description = (
                    f"كفر {parsed.brand} {parsed.model} مقاس {parsed.size} يوفر ثباتاً ممتازاً "
                    f"وأداءً عملياً للاستخدام اليومي. بلد المنشأ: {item.country or 'غير محدد'}، "
                    f"سنة الصنع: {item.year or 'غير محددة'}، نقشة: {item.pattern or 'غير محددة'}."
                )
            else:
                description = (item.name or "").strip()

        if rim_site:
            review_status = "needs_review" if not price else "ok"
        else:
            review_status = "needs_review" if (not parsed.size or not price) else "ok"

        products.append(
            {
                "name": item.name,
                "product_title": product_title,
                "brand": parsed.brand,
                "model": parsed.model,
                "size": parsed.size,
                "width": parsed.width,
                "profile": parsed.profile,
                "rim": parsed.rim,
                "load_speed": parsed.load_speed,
                "price": price,
                "product_url": item.product_url,
                "image_url": (
                    item.image_url
                    if (item.image_url or "").startswith(("http://", "https://"))
                    and not _is_weak_image_url(item.image_url)
                    else resolve_product_image_url(item.image_url, item.product_url)
                ),
                "year": item.year,
                "country": item.country,
                "pattern": item.pattern,
                "description": description,
                "seo_title": seo["seo_title"],
                "meta_description": seo["meta_description"],
                "keywords": seo["keywords"],
                "image_alt_text": seo["image_alt_text"],
                "warranty": item.warranty,
                "status": review_status,
            }
        )

    _universal_report(progress_cb, 94, "تصدير CSV...")
    exports_root = Path(exports_root)
    try:
        exports_root.mkdir(parents=True, exist_ok=True)
    except OSError:
        import tempfile

        exports_root = Path(tempfile.gettempdir()) / "auditflow_exports"
        exports_root.mkdir(parents=True, exist_ok=True)
    csv_path = exports_root / f"{site_key}_products_salla_like.csv"
    try:
        export_salla_like_csv(products, csv_path)
    except Exception as e:
        log.warning("export_salla_like_csv failed: %s", e)
        csv_path = Path("")
    _universal_report(progress_cb, 99, f"اكتمل — {len(products)} منتج")

    log.info(
        "done site=%s raw=%s out=%s csv=%s",
        site_key,
        len(raw_items),
        len(products),
        csv_path,
    )
    hint = ""
    if len(raw_items) > 0 and len(products) == 0 and selected_brand:
        hint = "تم جلب منتجات من الموقع لكن فلتر الماركة استبعد الكل — اترك خانة الماركة فارغة للجنوط."
    elif len(raw_items) == 0:
        hint = "لم يُعثر على منتجات في الرابط — تأكد من الرابط أو جرّب من سيرفر lghe بعد النشر."

    return {
        "count": len(products),
        "raw_scraped_count": len(raw_items),
        "csv_path": str(csv_path),
        "items": products,
        "hint": hint,
    }


def _kafaratplus_products_from_listing_brand_scan(
    brand: str,
    *,
    max_pages: int,
    start_urls: Optional[List[str]] = None,
    limit: int = 0,
) -> List[Dict[str, Any]]:
    """مسار بديل: سحب كروت القائمة مباشرة (أنسب لكفرات بلس)."""
    cfg = SITES_CONFIG["kafaratplus"]
    bases = [u for u in (start_urls or DEEP_SCAN_SITES["kafaratplus"].get("start_urls") or []) if u]
    selected = normalize_brand(brand)
    products: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for base in bases:
        for page in range(1, max(1, int(max_pages or 1)) + 1):
            page_url = build_page_url(base, page, cfg.get("pagination_param", "page"))
            page_items = scrape_single_page(page_url, cfg, enrich_product_pages=False)
            if not page_items:
                break
            for item in page_items:
                parsed = parse_tire_name(item.name)
                product_brand = normalize_brand(parsed.brand)
                if not (
                    product_brand == selected
                    or _deep_title_matches_brand(item.name, brand)
                    or _deep_url_matches_brand(item.product_url, brand)
                ):
                    continue

                price = normalize_price(item.price_raw)
                seo = build_seo_fields(parsed, item.year, item.country, item.pattern)
                product_title = " ".join(
                    x for x in [parsed.brand, parsed.model, parsed.size, parsed.load_speed] if x
                ).strip()
                key = (item.product_url or "").strip().lower() or (item.name or "").strip().lower()
                if not key or key in seen:
                    continue
                seen.add(key)

                products.append(
                    {
                        "name": item.name,
                        "product_title": product_title,
                        "brand": parsed.brand,
                        "model": parsed.model,
                        "size": parsed.size,
                        "width": parsed.width,
                        "profile": parsed.profile,
                        "rim": parsed.rim,
                        "load_speed": parsed.load_speed,
                        "price": price,
                        "product_url": item.product_url,
                        "image_url": item.image_url,
                        "year": item.year,
                        "country": item.country,
                        "pattern": item.pattern,
                        "description": (
                            f"كفر {parsed.brand} {parsed.model} مقاس {parsed.size}. "
                            f"بلد المنشأ: {item.country or 'غير محدد'}، سنة الصنع: {item.year or 'غير محددة'}."
                        ),
                        "seo_title": seo["seo_title"],
                        "meta_description": seo["meta_description"],
                        "keywords": seo["keywords"],
                        "image_alt_text": seo["image_alt_text"],
                        "warranty": item.warranty,
                        "status": "needs_review" if (not parsed.size or not price) else "ok",
                    }
                )
                if limit > 0 and len(products) >= limit:
                    return products
    return products


def brand_deep_scan(
    site_key: str,
    brand: str,
    *,
    max_pages: int = 200,
    limit: int = 0,
    exports_root: Path = Path("exports"),
    progress_cb: Optional[Callable[[int, str], None]] = None,
    start_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Brand Deep Scan: يجمع روابط من صفحات البداية/الترقيم، يفتح كل منتج،
    ويحتفظ فقط بما يطابق البراند في عنوان الصفحة (أو حقل brand إن وُجد).
    """
    if site_key not in DEEP_SCAN_SITES:
        raise ValueError(f"site_key غير مدعوم لـ Brand Deep Scan: {site_key}")
    b = (brand or "").strip()
    if not b:
        raise ValueError("Brand Deep Scan يتطلب brand (اسم الماركة)")

    eff_limit = _universal_effective_limit(limit)
    links = _deep_collect_product_links(site_key, max_pages=max(1, int(max_pages or 1)), start_urls=start_urls)
    log.info("brand_deep_scan site=%s brand=%s links=%s", site_key, b, len(links))

    listing_meta: Dict[str, tuple[str, str, str]] = {}
    if site_key == "kafaratplus":
        listing_meta = _kafaratplus_listing_meta_cache(
            max_pages=max(1, int(max_pages or 1)),
            start_urls=start_urls or DEEP_SCAN_SITES[site_key].get("start_urls"),
        )

    raw_rows: List[Dict[str, str]] = []
    total = len(links)
    for i, link in enumerate(links, start=1):
        if progress_cb and total:
            progress_cb(max(1, min(99, int(i / max(total, 1) * 90))), f"Brand Deep Scan {i}/{total}")
        row = _deep_parse_product_row(site_key, link, b)
        if row:
            if listing_meta:
                _apply_listing_meta(row, listing_meta)
            raw_rows.append(row)
        if eff_limit > 0 and len(raw_rows) >= eff_limit:
            break

    products: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for row in raw_rows:
        p = _deep_row_to_universal_product(row, b)
        key = (p.get("product_url") or "").strip().lower() or (p.get("name") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        products.append(p)

    if site_key == "kafaratplus" and not products:
        log.info("brand_deep_scan kafaratplus listing fallback brand=%s", b)
        if progress_cb:
            progress_cb(95, "تفعيل مسار القائمة لكفرات بلس...")
        products = _kafaratplus_products_from_listing_brand_scan(
            b,
            max_pages=max(1, int(max_pages or 1)),
            start_urls=start_urls,
            limit=eff_limit,
        )

    exports_root = Path(exports_root)
    exports_root.mkdir(parents=True, exist_ok=True)
    csv_path = exports_root / f"{site_key}_deep_scan_salla_like.csv"
    export_salla_like_csv(products, csv_path)
    if progress_cb:
        progress_cb(100, f"Brand Deep Scan اكتمل — {len(products)} منتج")

    log.info("brand_deep_scan done site=%s count=%s csv=%s", site_key, len(products), csv_path)
    return {
        "ok": True,
        "count": len(products),
        "items": products,
        "csv_path": str(csv_path),
    }


# =========================
# 8) تصدير CSV جاهز لسلة (مع promo_title صحيح)
# =========================

def export_salla_like_csv(products: List[Dict[str, Any]], csv_path: Path) -> None:
    from .csv_exporter import _to_public_image_value

    fieldnames = [
        "أسم المنتج",
        "صورة المنتج",
        "سعر المنتج",
        "الوصف",
        "الماركة",
        "المقاس",
        "العنوان الترويجي",
        "الكلمات المفتاحية",
        "رابط المنتج الأصلي",
    ]

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for p in products:
            year = str(p.get("year", "") or "").strip()
            country = str(p.get("country", "") or "").strip()
            warranty = str(p.get("warranty", "") or "").strip()

            promo_bits: List[str] = []
            if year and country:
                promo_bits.append(f"سنة الصنع {year} - بلد الصنع {country}")
            elif year:
                promo_bits.append(f"سنة الصنع {year}")
            elif country:
                promo_bits.append(f"بلد الصنع {country}")
            if warranty:
                promo_bits.append(f"الضمان {warranty}")

            promo_title = " - ".join(x for x in promo_bits if x).strip() or str(p.get("seo_title", "") or "").strip()

            writer.writerow(
                {
                    "أسم المنتج": p.get("product_title") or p.get("name") or "",
                    "صورة المنتج": _to_public_image_value(p),
                    "سعر المنتج": p.get("price", ""),
                    "الوصف": p.get("description", ""),
                    "الماركة": p.get("brand", ""),
                    "المقاس": p.get("size", ""),
                    "العنوان الترويجي": promo_title,
                    "الكلمات المفتاحية": p.get("keywords", ""),
                    "رابط المنتج الأصلي": p.get("product_url", ""),
                }
            )


# =========================
# 9) نقطة تشغيل بسيطة (اختيارية)
# =========================

if __name__ == "__main__":
    # مثال تشغيل سريع لـ KafaratPlus
    site = "kafaratplus"
    category = "https://kafaratplus.com/shop/"
    exports_root = Path("exports")

    log.info("Start scraping %s ...", site)
    raw_items = scrape_products(site, category_url=category, max_pages=5, limit=0)

    # تحويل RawProduct إلى dict متوافق مع export_salla_like_csv
    products: List[Dict[str, Any]] = []
    for r in raw_items:
        parsed = parse_tire_name(r.name)
        price = normalize_price(r.price_raw)
        seo = build_seo_fields(parsed, r.year, r.country, r.pattern)
        product_title = " ".join(
            x for x in [parsed.brand, parsed.model, parsed.size, parsed.load_speed] if x
        ).strip()
        products.append(
            {
                "name": r.name,
                "product_title": product_title,
                "brand": parsed.brand,
                "model": parsed.model,
                "size": parsed.size,
                "width": parsed.width,
                "profile": parsed.profile,
                "rim": parsed.rim,
                "load_speed": parsed.load_speed,
                "price": price,
                "product_url": r.product_url,
                "image_url": r.image_url,
                "year": r.year,
                "country": r.country,
                "pattern": r.pattern,
                "description": f"كفر {parsed.brand} {parsed.model} مقاس {parsed.size}.",
                "seo_title": seo["seo_title"],
                "meta_description": seo["meta_description"],
                "keywords": seo["keywords"],
                "image_alt_text": seo["image_alt_text"],
                "warranty": r.warranty,
            }
        )

    csv_path = exports_root / f"{site}_salla_like.csv"
    export_salla_like_csv(products, csv_path)
    log.info("Done. Exported %s products to %s", len(products), csv_path)
