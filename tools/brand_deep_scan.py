#!/usr/bin/env python3
"""
Brand Deep Scan — جمع روابط من صفحات القائمة ثم فتح كل منتج والتحقق من البراند وحفظ CSV.

تشغيل:
  python tools/brand_deep_scan.py --site tireex --brand Accelera
  python tools/brand_deep_scan.py --site tireex --brand Accelera --start "https://tireex.com/product-category/accelera-tires/"
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import time
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
}

# كلمات عربية شائعة للماركات (مطابقة بسيطة مع العنوان)
BRAND_ALIASES: Dict[str, tuple[str, ...]] = {
    "accelera": ("accelera", "اكسيليرا", "أكسيليرا", "إطارات اكسيليرا"),
    "hankook": ("hankook", "هانكوك"),
    "michelin": ("michelin", "ميشلان"),
    "goodyear": ("goodyear", "جوديير"),
}


def _brand_tokens(brand_name: str) -> tuple[str, ...]:
    key = re.sub(r"\s+", " ", (brand_name or "").strip().lower())
    extra = BRAND_ALIASES.get(key, ())
    return (key,) + tuple(x.lower() for x in extra if x)


def get_soup(url: str, timeout: int = 45) -> BeautifulSoup:
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def extract_text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


def extract_image_url(el, base_url: str) -> str:
    if not el:
        return ""
    for attr in ("data-large_image", "data-src", "data-lazy-src"):
        v = (el.get(attr) or "").strip()
        if v:
            return urljoin(base_url, v)
    srcset = el.get("srcset") or ""
    if srcset:
        part = srcset.split(",")[0].strip().split()
        if part:
            return urljoin(base_url, part[0])
    src = (el.get("src") or "").strip()
    return urljoin(base_url, src) if src else ""


SITE_CONFIG: Dict[str, Dict[str, Any]] = {
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


def _normalize_shop_url(base_url: str, href: str) -> str:
    if not href:
        return ""
    full = urljoin(base_url, href.split("?")[0])
    path = (urlparse(full).path or "").lower()
    if "/product/" not in path and "/shop/" not in path:
        return ""
    return full


def _collect_gtm_links(base_url: str, soup: BeautifulSoup, out: Set[str]) -> None:
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
        u = _normalize_shop_url(base_url, link)
        if u:
            out.add(u)


def collect_product_links(
    site_key: str,
    max_pages: int = 80,
    *,
    start_urls: Optional[List[str]] = None,
) -> List[str]:
    cfg = SITE_CONFIG[site_key]
    base = cfg["base_url"].rstrip("/")
    visited_pages: Set[str] = set()
    product_links: Set[str] = set()
    seeds = list(start_urls) if start_urls else list(cfg["start_urls"])
    to_visit: List[str] = seeds
    pages_opened = 0

    selectors: List[str] = list(cfg.get("product_link_selectors") or [])
    if not selectors and cfg.get("product_link_selector"):
        selectors = [cfg["product_link_selector"]]

    while to_visit and pages_opened < max_pages:
        url = to_visit.pop(0)
        if url in visited_pages:
            continue
        visited_pages.add(url)
        pages_opened += 1

        try:
            soup = get_soup(url)
        except Exception as e:
            print(f"[WARN] فشل فتح الصفحة: {url} | {e}")
            continue

        for sel in selectors:
            for a in soup.select(sel):
                href = a.get("href")
                u = _normalize_shop_url(base, href or "")
                if u:
                    product_links.add(u)

        if cfg.get("use_gtm_embed"):
            _collect_gtm_links(base, soup, product_links)

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


def title_matches_brand(title: str, brand_name: str) -> bool:
    t = (title or "").lower()
    for tok in _brand_tokens(brand_name):
        if tok and tok in t:
            return True
    return False


def parse_product_page(site_key: str, url: str, target_brand: str) -> Optional[Dict[str, str]]:
    cfg = SITE_CONFIG[site_key]
    try:
        soup = get_soup(url)
    except Exception as e:
        print(f"[WARN] فشل فتح صفحة المنتج: {url} | {e}")
        return None

    title = ""
    for sel in (cfg["product_title_selector"] or "h1").split(","):
        el = soup.select_one(sel.strip())
        if el:
            title = extract_text(el)
            if title:
                break

    brand: Optional[str] = None
    if cfg.get("brand_selector"):
        brand = extract_text(soup.select_one(cfg["brand_selector"])) or None

    if not brand:
        if title_matches_brand(title, target_brand):
            brand = target_brand.strip()
        else:
            return None
    else:
        if brand.lower() != target_brand.strip().lower() and not title_matches_brand(title, target_brand):
            return None

    price = ""
    for sel in (cfg["price_selector"] or "p.price").split(","):
        el = soup.select_one(sel.strip())
        if el:
            price = extract_text(el)
            if price:
                break

    img_el = None
    for sel in (cfg["image_selector"] or "img").split(","):
        img_el = soup.select_one(sel.strip())
        if img_el:
            break
    image_url = extract_image_url(img_el, url) if img_el else ""

    description = ""
    for sel in (cfg["description_selector"] or "div").split(","):
        el = soup.select_one(sel.strip())
        if el:
            description = extract_text(el)
            if description:
                break

    return {
        "url": url,
        "title": title,
        "brand": brand or target_brand,
        "price": price,
        "image": image_url,
        "description": description,
    }


def brand_deep_scan(
    site_key: str,
    brand_name: str,
    csv_path: str = "output_brand_scan.csv",
    *,
    start_urls: Optional[List[str]] = None,
    max_pages: int = 80,
    delay_s: float = 0.35,
) -> List[Dict[str, str]]:
    if site_key not in SITE_CONFIG:
        raise ValueError(f"موقع غير معروف: {site_key}")

    print(f"بدء Brand Deep Scan | الموقع: {site_key} | البراند: {brand_name}")

    links = collect_product_links(site_key, max_pages=max_pages, start_urls=start_urls)
    print(f"تم جمع {len(links)} رابط منتج من صفحات القائمة")

    results: List[Dict[str, str]] = []
    total = len(links)
    for i, link in enumerate(links, start=1):
        print(f"[{i}/{total}] {link}")
        row = parse_product_page(site_key, link, brand_name)
        if row:
            results.append(row)
            print(f"   + {row['title'][:70]}")
        time.sleep(delay_s)

    fieldnames = ["title", "brand", "price", "image", "description", "url"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in results:
            w.writerow(row)

    print(f"اكتمل — مطابق للبراند: {len(results)} | الملف: {csv_path}")
    return results


def main() -> None:
    p = argparse.ArgumentParser(description="Brand Deep Scan (قائمة + صفحة منتج + فلترة براند)")
    p.add_argument("--site", default="tireex", choices=list(SITE_CONFIG.keys()))
    p.add_argument("--brand", required=True, help="مثال: Accelera")
    p.add_argument("--out", default="brand_deep_scan_output.csv")
    p.add_argument("--start", action="append", dest="starts", help="رابط بداية (يمكن تكرار --start)")
    p.add_argument("--max-pages", type=int, default=80)
    p.add_argument("--delay", type=float, default=0.35)
    args = p.parse_args()

    starts = args.starts if args.starts else None
    brand_deep_scan(
        args.site,
        args.brand,
        csv_path=args.out,
        start_urls=starts,
        max_pages=args.max_pages,
        delay_s=args.delay,
    )


if __name__ == "__main__":
    main()
