from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
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

        if not name:
            continue

        items.append(
            RawProduct(
                name=name,
                price_raw=price_raw,
                image_url=image_url,
                product_url=product_url,
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
            writer.writerow(
                {
                    "أسم المنتج": p.get("product_title", ""),
                    "صورة المنتج": p.get("image_url", ""),
                    "سعر المنتج": p.get("price", ""),
                    "الوصف": p.get("description", ""),
                    "الماركة": p.get("brand", ""),
                    "المقاس": p.get("size", ""),
                    "العنوان الترويجي": p.get("seo_title", ""),
                    "الكلمات المفتاحية": p.get("keywords", ""),
                    "رابط المنتج الأصلي": p.get("product_url", ""),
                }
            )


# =========================
# 7) البايبلاين الكامل
# =========================

def run_universal_import(
    site_key: str,
    category_url: str,
    *,
    max_pages: int = 10,
    limit: int = 0,
    exports_root: Path = Path("exports"),
) -> Dict[str, Any]:
    raw_items = scrape_products(site_key, category_url, max_pages=max_pages, limit=limit)

    products: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()

    for item in raw_items:
        parsed = parse_tire_name(item.name)
        price = normalize_price(item.price_raw)
        if not parsed.size or not price:
            continue

        seo = build_seo_fields(parsed, item.year, item.country, item.pattern)

        product_title = " ".join(
            x for x in [parsed.brand, parsed.model, parsed.size, parsed.load_speed] if x
        ).strip()

        key = (
            parsed.brand.lower(),
            parsed.model.lower(),
            parsed.size.lower(),
            parsed.load_speed.lower(),
        )
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
