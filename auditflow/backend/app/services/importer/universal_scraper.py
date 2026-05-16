from __future__ import annotations

import csv
import logging
import os
import re
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
}

_DEEP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
}

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


def _deep_get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=_DEEP_HEADERS, timeout=45)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def _deep_normalize_shop_url(base_url: str, href: str) -> str:
    if not href:
        return ""
    full = urljoin(base_url, href.split("?")[0])
    path = (urlparse(full).path or "").lower().rstrip("/")
    if not path or path in {"/", "/shop", "/cart", "/checkout", "/my-account"}:
        return ""
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


def _deep_extract_image_url(el, page_url: str) -> str:
    if not el:
        return ""
    for attr in ("data-large_image", "data-src", "data-lazy-src"):
        v = (el.get(attr) or "").strip()
        if v:
            return urljoin(page_url, v)
    srcset = el.get("srcset") or ""
    if srcset:
        part = srcset.split(",")[0].strip().split()
        if part:
            return urljoin(page_url, part[0])
    src = (el.get("src") or "").strip()
    return urljoin(page_url, src) if src else ""


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
            "a.page-numbers, a.pagination-next"
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
    try:
        resp = requests.get(url, timeout=20, headers=_DEEP_HEADERS)
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

    return [
        RawProduct(
            name=title,
            price_raw=price_raw,
            image_url=image_url,
            product_url=url,
            year=year,
            country=country,
            warranty=warranty,
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
        image_url = ""
        if img_el:
            image_url = img_el.get("src", "") or img_el.get("data-src", "") or ""
            image_url = urljoin(page_url, image_url) if image_url else ""

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
        resp = requests.get(url, timeout=20)
    except Exception as e:
        log.warning("request_failed url=%s err=%s", url, e)
        return items

    if resp.status_code != 200:
        log.warning("bad_status url=%s status=%s", url, resp.status_code)
        return items

    soup = BeautifulSoup(resp.text, "html.parser")

    if _is_probable_product_detail(url, soup, cfg):
        return _scrape_product_detail_as_one(url, cfg)

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
) -> List[RawProduct]:
    if site_key not in SITES_CONFIG:
        raise ValueError(f"Unknown site_key={site_key}")

    cfg = SITES_CONFIG[site_key]
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
) -> Dict[str, Any]:
    eff_limit = _universal_effective_limit(limit)
    raw_items = scrape_products(site_key, category_url, max_pages=max_pages, limit=eff_limit)

    products: List[Dict[str, Any]] = []
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
            if product_brand != selected_brand and not _deep_title_matches_brand(item.name, brand):
                if not (url_brand and url_brand == selected_brand):
                    continue

        seo = build_seo_fields(parsed, item.year, item.country, item.pattern)
        product_title = " ".join(
            x for x in [parsed.brand, parsed.model, parsed.size, parsed.load_speed] if x
        ).strip()

        key = (item.product_url or "").strip().lower() or (item.name or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)

        description = (
            f"كفر {parsed.brand} {parsed.model} مقاس {parsed.size} يوفر ثباتاً ممتازاً "
            f"وأداءً عملياً للاستخدام اليومي. بلد المنشأ: {item.country or 'غير محدد'}، "
            f"سنة الصنع: {item.year or 'غير محددة'}، نقشة: {item.pattern or 'غير محددة'}."
        )

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
                "description": description,
                "seo_title": seo["seo_title"],
                "meta_description": seo["meta_description"],
                "keywords": seo["keywords"],
                "image_alt_text": seo["image_alt_text"],
                "warranty": item.warranty,
                "status": "needs_review" if (not parsed.size or not price) else "ok",
            }
        )

    exports_root = Path(exports_root)
    exports_root.mkdir(parents=True, exist_ok=True)
    csv_path = exports_root / f"{site_key}_products_salla_like.csv"
    export_salla_like_csv(products, csv_path)

    log.info("done site=%s count=%s csv=%s", site_key, len(products), csv_path)
    return {
        "count": len(products),
        "csv_path": str(csv_path),
        "items": products,
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
                    "صورة المنتج": p.get("image_url", ""),
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
