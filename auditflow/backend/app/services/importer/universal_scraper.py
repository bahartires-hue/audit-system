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
# 1) إعداد المواقع
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
        "product_selector": "div.product-box",
        "title_selector": ".product-box-title",
        "price_selector": ".product-box-price",
        "image_selector": "img",
        "link_selector": "a",
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


# إعدادات Brand Deep Scan (قائمة + صفحة منتج + مطابقة الاسم مع البراند)
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
        "product_link_selectors": ["ul.products li.product a[href]"],
        "use_gtm_embed": False,
        "product_title_selector": "h1.product_title",
        "brand_selector": None,
        "price_selector": "p.price",
        "image_selector": "figure.woocommerce-product-gallery__wrapper img",
        "description_selector": "div.woocommerce-product-details__short-description",
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
}


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


def _deep_get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=_DEEP_HEADERS, timeout=45)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def _deep_normalize_shop_url(base_url: str, href: str) -> str:
    if not href:
        return ""
    full = urljoin(base_url, href.split("?")[0])
    path = (urlparse(full).path or "").lower()
    if "/product/" not in path and "/shop/" not in path:
        return ""
    return full


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
        # fallback لسطر بطاقة مثل "رومانيا / تاريخ 2024"
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

    title = ""
    for sel in (cfg["product_title_selector"] or "h1").split(","):
        el = soup.select_one(sel.strip())
        if el:
            title = _deep_extract_text(el)
            if title:
                break

    brand: Optional[str] = None
    if cfg.get("brand_selector"):
        brand = _deep_extract_text(soup.select_one(cfg["brand_selector"])) or None

    if not brand:
        if _deep_title_matches_brand(title, target_brand):
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
    country, year = _extract_country_year_from_text(" ".join(x for x in [meta_text, description, page_text] if x))

    return {
        "url": url,
        "title": title,
        "brand": brand or target_brand,
        "price": price,
        "image": image_url,
        "description": description,
        "year": year,
        "country": country,
    }


def _deep_row_to_universal_product(row: Dict[str, str], target_brand: str) -> Dict[str, Any]:
    name = row.get("title") or ""
    parsed = parse_tire_name(name)
    price = normalize_price(row.get("price") or "")
    product_url = (row.get("url") or "").strip()
    image_url = (row.get("image") or "").strip()
    description = (row.get("description") or "").strip() or (
        f"كفر {parsed.brand} {parsed.model} مقاس {parsed.size} — مطابقة Brand Deep Scan للماركة {target_brand}."
    )
    seo = build_seo_fields(parsed, "", "", "")
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
        "year": (row.get("year") or "").strip(),
        "country": (row.get("country") or "").strip(),
        "pattern": "",
        "description": description,
        "seo_title": seo["seo_title"],
        "meta_description": seo["meta_description"],
        "keywords": seo["keywords"],
        "image_alt_text": seo["image_alt_text"],
        "status": "needs_review" if (not parsed.size or not price) else "ok",
    }


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

    raw_rows: List[Dict[str, str]] = []
    total = len(links)
    for i, link in enumerate(links, start=1):
        if progress_cb and total:
            progress_cb(max(1, min(99, int(i / max(total, 1) * 90))), f"Brand Deep Scan {i}/{total}")
        row = _deep_parse_product_row(site_key, link, b)
        if row:
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

    exports_root = Path(exports_root)
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
# 2) أدوات مساعدة
# =========================

def build_page_url(base_url: str, page: int, page_param: str) -> str:
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs[page_param] = [str(page)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


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
# 3) تحليل اسم الكفر
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
# 4) SEO ذكي لكل إطار
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
# 5) السكربر العام
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


def scrape_single_page(url: str, cfg: Dict[str, Any]) -> List[RawProduct]:
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

    for card in soup.select(cfg["product_selector"]):
        title_el = card.select_one(cfg["title_selector"])
        price_el = card.select_one(cfg["price_selector"])
        img_el = card.select_one(cfg["image_selector"])
        link_el = card.select_one(cfg["link_selector"])

        name = title_el.get_text(strip=True) if title_el else ""
        price_raw = price_el.get_text(strip=True) if price_el else ""
        image_url = img_el.get("src", "") if img_el else ""
        product_url = link_el.get("href", "") if link_el else ""
        image_url = urljoin(url, image_url) if image_url else ""
        product_url = urljoin(url, product_url) if product_url else ""
        meta_text = ""
        for sel in (
            ".product-card-year, .product-box-year, .year, .origin, .country, [class*='year'], [class*='origin']"
        ).split(","):
            el = card.select_one(sel.strip())
            if el:
                meta_text = _deep_extract_text(el)
                if meta_text:
                    break
        if not meta_text:
            meta_text = _deep_extract_text(card)
        country, year = _extract_country_year_from_text(meta_text)

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
            )
        )

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

    for page in range(1, max_pages + 1):
        page_url = build_page_url(category_url, page, page_param)
        log.info("scraping site=%s page=%s url=%s", site_key, page, page_url)

        page_items = scrape_single_page(page_url, cfg)
        if not page_items:
            break

        all_items.extend(page_items)

        if limit > 0 and len(all_items) >= limit:
            all_items = all_items[:limit]
            break

    return all_items


# =========================
# 6) تصدير CSV جاهز لسلة
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
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
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
                    "أسم المنتج": p.get("product_title", ""),
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
# 7) البايبلاين الكامل
# =========================


def _universal_effective_limit(limit: int) -> int:
    """limit > 0: سقف صريح. limit <= 0: سقف أمان من AUDITFLOW_IMPORTER_MAX_ITEMS (نفس منطق Tireex)."""
    li = int(limit or 0)
    if li > 0:
        return min(li, 2_000_000)
    try:
        cap = int((os.getenv("AUDITFLOW_IMPORTER_MAX_ITEMS") or "200000").strip())
    except ValueError:
        cap = 200_000
    return max(10_000, min(cap, 2_000_000))


def run_universal_import(
    site_key: str,
    category_url: str,
    *,
    max_pages: int = 10,
    limit: int = 0,
    brand: str = "",
    exports_root: Path = Path("exports"),
) -> Dict[str, Any]:
    effective_limit = _universal_effective_limit(limit)
    raw_items = scrape_products(site_key, category_url, max_pages=max_pages, limit=effective_limit)

    products: List[Dict[str, Any]] = []
    seen: set[str] = set()
    selected_brand = normalize_brand(brand)

    for item in raw_items:
        parsed = parse_tire_name(item.name)
        price = normalize_price(item.price_raw)
        product_brand = normalize_brand(parsed.brand)
        if selected_brand and product_brand != selected_brand:
            continue

        seo = build_seo_fields(parsed, item.year, item.country, item.pattern)

        product_title = " ".join(
            x for x in [parsed.brand, parsed.model, parsed.size, parsed.load_speed] if x
        ).strip()

        key = (item.product_url or "").strip().lower() or (item.name or "").strip().lower()
        if key in seen:
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
                "status": "needs_review" if (not parsed.size or not price) else "ok",
            }
        )

    csv_path = exports_root / f"{site_key}_products_salla_like.csv"
    export_salla_like_csv(products, csv_path)

    log.info("done site=%s count=%s csv=%s", site_key, len(products), csv_path)
    return {
        "count": len(products),
        "csv_path": str(csv_path),
        "items": products,
    }


# =========================
# 8) أمثلة تشغيل
# =========================

if __name__ == "__main__":
    run_universal_import(
        site_key="tireex",
        category_url="https://tireex.com/product-category/accelera-tires/",
        max_pages=10,
        limit=0,
    )

    run_universal_import(
        site_key="lumitires",
        category_url="https://lumitiress.com/categories/1059345/%D8%A7%D8%B7%D8%A7%D8%B1%D8%A7%D8%AA-%D9%83%D9%88%D9%85%D9%87%D9%88",
        max_pages=10,
        limit=0,
    )

    run_universal_import(
        site_key="kafaratplus",
        category_url="https://kafaratplus.com/darbk",
        max_pages=10,
        limit=0,
    )

    run_universal_import(
        site_key="etar",
        category_url="https://etar.com/category/car-suv",
        max_pages=10,
        limit=0,
    )
