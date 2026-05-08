from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("importer.tireex")

_UA = {"User-Agent": "Mozilla/5.0"}
_SIZE_RE = re.compile(r"(\d{3})\s*/\s*(\d{2})\s*(?:ZR|R)?\s*(\d{2})", re.IGNORECASE)
_GENERIC_TITLE_RE = re.compile(r"(تصنيف|عروض|منتجات|product category|category)", re.IGNORECASE)


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


def _pick_largest_srcset(srcset: str) -> str:
    best_url = ""
    best_w = -1
    for part in (srcset or "").split(","):
        seg = part.strip().split()
        if not seg:
            continue
        u = seg[0].strip()
        w = 0
        if len(seg) > 1 and seg[1].endswith("w"):
            try:
                w = int(seg[1][:-1])
            except Exception:
                w = 0
        if w >= best_w:
            best_w = w
            best_url = u
    return best_url


def _extract_price_value(text: str) -> str:
    raw = _clean(text)
    nums = re.findall(r"(\d+(?:\.\d+)?)\s*(?:ر\.س|SAR|ريال)?", raw, flags=re.IGNORECASE)
    vals: List[float] = []
    for n in nums:
        try:
            v = float(n)
            if 10 <= v <= 100000:
                vals.append(v)
        except Exception:
            continue
    if not vals:
        return raw
    # نرجع أول قيمة صالحة كنص للحفاظ على تنسيق الحقول الحالية.
    v = vals[0]
    return str(int(v)) if v.is_integer() else str(v)


def _has_tire_size(text: str) -> bool:
    return bool(_SIZE_RE.search(_clean(text).upper()))


def _find_detail(page_text: str, labels: List[str]) -> str:
    for label in labels:
        m = re.search(rf"{re.escape(label)}\s*[:：]?\s*([^\n\r|]+)", page_text, flags=re.IGNORECASE)
        if m:
            return _clean(m.group(1))
    return ""


def _is_product_url(url: str) -> bool:
    p = (urlparse(url).path or "").lower().strip("/")
    if not p:
        return False
    if p.startswith("product-category/") or p == "product-category":
        return False
    if p == "shop":
        return False
    if p.startswith("product/"):
        return True
    if p.startswith("products/"):
        return True
    if p.startswith("shop/"):
        # في Tireex صفحات المنتج غالبًا تحت /shop/<slug>/
        return len(p.split("/")) >= 2
    return False


def _extract_size_token(text: str) -> str:
    m = _SIZE_RE.search(text or "")
    if not m:
        return ""
    return f"{m.group(1)}/{m.group(2)}R{m.group(3)}"


def _extract_product_links(base_url: str, soup: BeautifulSoup) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        u = urljoin(base_url, href)
        if urlparse(u).netloc != urlparse(base_url).netloc:
            continue
        path = (urlparse(u).path or "").lower()
        if "/product/" not in path and "/shop/" not in path:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _extract_product_links_by_anchor_text(base_url: str, soup: BeautifulSoup) -> List[Dict[str, str]]:
    products: List[Dict[str, str]] = []
    seen: Set[str] = set()
    bad_words = ("add-to-cart", "cart", "checkout", "category", "tag")
    all_links = soup.select("a[href]")
    log.info("tireex total anchors=%s url=%s", len(all_links), base_url)
    for a in all_links:
        text = _clean(a.get_text(" ", strip=True))
        href = (a.get("href") or "").strip()
        if not text or not href:
            continue
        if not _has_tire_size(text):
            continue
        product_url = urljoin(base_url, href)
        lower_u = product_url.lower()
        if any(w in lower_u for w in bad_words):
            continue
        if "/product/" not in lower_u and "/shop/" not in lower_u:
            continue
        if urlparse(product_url).netloc != urlparse(base_url).netloc:
            continue
        if product_url in seen:
            continue
        seen.add(product_url)
        products.append({"name": text, "product_url": product_url})
    log.info("tireex anchors-with-size=%s url=%s", len(products), base_url)
    return products


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
    cards = soup.select(
        "li.product, .product, .products .product, .wc-block-grid__product, .woocommerce-LoopProduct-link, a.woocommerce-LoopProduct-link, .product-card"
    )
    # Tireex theme specific: cards can be represented primarily by title anchors.
    if not cards:
        cards = soup.select("a.product-card-content-title")
    log.info("tireex detected listing cards=%s url=%s", len(cards), base_url)
    for card in cards:
        if card.name == "a" and "product-card-content-title" in ((card.get("class") or [])):
            a = card
            card_root = card.find_parent(class_=re.compile(r"product-card", re.I)) or card.parent or card
        elif card.name == "a":
            a = card
            card_root = card.parent or card
        else:
            a = (
                card.select_one("a.product-card-content-title[href]")
                or card.select_one("a.woocommerce-LoopProduct-link[href]")
                or card.select_one("a[href]")
            )
            card_root = card
        if not a or not a.get("href"):
            log.info("tireex skip card reason=no_link")
            continue
        product_url = urljoin(base_url, a.get("href"))
        if product_url in seen:
            continue
        pth = (urlparse(product_url).path or "").lower()
        if "/product/" not in pth and "/shop/" not in pth:
            log.info("tireex skip card reason=not_product_url url=%s", product_url)
            continue
        seen.add(product_url)
        name_node = (
            card_root.select_one("a.product-card-content-title")
            or card_root.select_one("h2.woocommerce-loop-product__title")
            or card_root.select_one(".woocommerce-loop-product__title")
            or card_root.select_one(".product-title")
            or card_root.select_one("h2")
            or card_root.select_one("h3")
            or a
        )
        name = _clean(name_node.get_text(" ", strip=True) if name_node else "")
        if not name:
            log.info("tireex skip card reason=no_name")
            continue
        if _GENERIC_TITLE_RE.search(name):
            log.info("tireex skip card reason=generic_title name=%s", name)
            continue
        price_node = (
            card_root.select_one(".price")
            or card_root.select_one(".woocommerce-Price-amount")
            or card_root.select_one("bdi")
            or card_root.select_one(".product-card-price")
        )
        old_price_node = card_root.select_one(".price del .amount") or card_root.select_one(".old-price") or card_root.select_one(".was-price")
        price = _extract_price_value(price_node.get_text(" ", strip=True) if price_node else "")
        old_price = _extract_price_value(old_price_node.get_text(" ", strip=True) if old_price_node else "")
        img = card_root.select_one("img")
        image_url = ""
        if img:
            raw = _pick_largest_srcset(img.get("srcset") or "")
            if not raw:
                raw = img.get("data-src") or img.get("data-lazy-src") or img.get("src") or ""
            image_url = urljoin(base_url, raw) if raw else ""
        out.append(
            {
                "name": name,
                "price": price,
                "old_price": old_price,
                "product_url": product_url,
                "image_url": image_url,
                "year": _pick_text(card_root, [".product-card-year .content", ".product-card-year"]),
                "country": "",
                "warranty": "",
                "pattern": _pick_text(card_root, [".product-card-pattern"]),
                "description": "",
                "_size_token": _extract_size_token(name),
            }
        )
    log.info("tireex listing products after size filter=%s url=%s", len(out), base_url)
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
    name = _pick_text(doc, ["h1.product_title", ".product_title", ".product-title", "h1", "h2"])
    if not name:
        name = _pick_attr(doc, ["meta[property='og:title']", "meta[name='twitter:title']"], "content")
    size_token = _extract_size_token(name)
    if not size_token:
        size_token = _extract_size_token(
            " ".join(
                x
                for x in [
                    _pick_text(doc, [".product_meta", ".summary", ".woocommerce-product-details__short-description"]),
                    _pick_text(doc, ["table.variations", ".woocommerce-product-attributes", ".shop_attributes"]),
                    _pick_text(doc, [".entry-content", ".product-description"]),
                    _pick_attr(doc, ["meta[property='og:description']"], "content"),
                ]
                if x
            )
        )
    if not size_token:
        size_token = _extract_size_token(doc.get_text(" ", strip=True))
    page_text = _clean(doc.get_text(" ", strip=True))
    price = _extract_price_value(_pick_text(doc, [".price .amount", ".price", "[class*='price'] .amount", "bdi"]))
    if not price:
        price = _extract_price_value(page_text)
    old_price = _extract_price_value(_pick_text(doc, [".price del .amount", ".price .old", ".was-price"]))
    image = _pick_attr(doc, ["meta[property='og:image']"], "content")
    if not image:
        image = _pick_attr(doc, [".woocommerce-product-gallery img", "img.wp-post-image", ".product img", "img"], "data-src")
    if not image:
        image = _pick_attr(doc, [".woocommerce-product-gallery img", "img.wp-post-image", ".product img", "img"], "src")
    image = urljoin(product_url, image) if image else ""
    year = ""
    ym = re.search(r"(20[2-9][0-9])", page_text)
    if ym:
        year = ym.group(1)
    if not year:
        year = _pick_text(doc, [".year", "[data-year]", ".manufacture-year"])
    warranty = _pick_text(doc, [".warranty", "[class*='warranty']"]) or _find_detail(page_text, ["الضمان", "Warranty"])
    country = _pick_text(doc, [".country", "[class*='origin']", ".origin"]) or _find_detail(page_text, ["بلد المنشأ", "الصنع", "Origin", "Country"])
    pattern = _pick_text(doc, [".pattern", "[class*='pattern']"]) or _find_detail(page_text, ["النقشة", "Pattern", "Tread"])
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
        "_size_token": size_token,
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
            # fallback سريع: روابط a التي تحمل مقاسًا في النص.
            for p in _extract_product_links_by_anchor_text(current, doc):
                if p.get("product_url") and all(x.get("product_url") != p["product_url"] for x in listing_items):
                    listing_items.append(
                        {
                            "name": p.get("name", ""),
                            "price": "",
                            "old_price": "",
                            "product_url": p["product_url"],
                            "image_url": "",
                            "year": "",
                            "country": "",
                            "warranty": "",
                            "pattern": "",
                            "description": "",
                            "_size_token": _extract_size_token(p.get("name", "")),
                        }
                    )
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
            if not p.get("name"):
                log.info("tireex skip product reason=no_name url=%s", u)
                continue
            if not p.get("_size_token"):
                log.info("tireex skip product reason=no_size name=%s url=%s", p.get("name", ""), u)
                continue
            if not p.get("product_url"):
                log.info("tireex skip product reason=no_product_url url=%s", u)
                continue
            if not p.get("image_url"):
                log.info("tireex product has no image url=%s", u)
            products.append(p)
        except Exception as e:
            log.warning("skip product %s: %s", u, e)
    if products:
        return products[:max_items]
    # fallback إذا فشل parsing صفحات المنتج: نعيد منتجات الكروت من صفحة الماركة/البحث.
    if not listing_items:
        try:
            doc = _fetch(url)
            html = doc.prettify()[:5000]
            Path("debug_tireex.html").write_text(html, encoding="utf-8")
            log.warning("tireex no products found; wrote debug_tireex.html")
        except Exception as e:
            log.warning("tireex debug html write failed: %s", e)
        links = _extract_product_links(url, _fetch(url))
        for u in links[: max_items * 4]:
            if len(listing_items) >= max_items:
                break
            try:
                p = _parse_product_page(u)
                if p.get("name") and p.get("_size_token") and p.get("product_url"):
                    listing_items.append(p)
            except Exception as e:
                log.warning("skip fallback product %s: %s", u, e)
    # إن رجعت من الكروت فقط، نحاول ترقية البيانات بدخول صفحات المنتج.
    upgraded: List[Dict[str, Any]] = []
    for item in listing_items:
        if len(upgraded) >= max_items:
            break
        u = item.get("product_url", "")
        try:
            p = _parse_product_page(u) if u else {}
            merged = {**item, **p}
            if merged.get("name") and merged.get("_size_token") and merged.get("product_url"):
                upgraded.append(merged)
            else:
                log.info("tireex skip upgraded card reason=missing_required url=%s", u)
        except Exception as e:
            log.warning("skip upgraded card %s: %s", u, e)
    return upgraded[:max_items]

