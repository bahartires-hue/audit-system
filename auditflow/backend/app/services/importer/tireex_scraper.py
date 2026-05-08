from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from .parser import normalize_brand_name

log = logging.getLogger("importer.tireex")

_UA = {"User-Agent": "Mozilla/5.0"}
_SIZE_RE = re.compile(r"(\d{3})\s*/\s*(\d{2})\s*(?:ZR|R)?\s*(\d{2})", re.IGNORECASE)
_SIZE_URL_RE = re.compile(r"(\d{3})[-_/](\d{2,3})[-_]?r(\d{2})", re.IGNORECASE)
_GENERIC_TITLE_RE = re.compile(r"(تصنيف|عروض|منتجات|product category|category)", re.IGNORECASE)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _fetch(url: str) -> BeautifulSoup:
    r = requests.get(url, timeout=15, headers=_UA)
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
    compact = re.sub(r"[^\d,\.]", " ", raw)
    nums = re.findall(r"\d[\d,\.]*", compact)
    vals: List[float] = []
    for n in nums:
        try:
            token = n.strip()
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
            v = float(token)
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


def _is_valid_product_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").strip().lower()
    bad_parts = [
        "/cdn-cgi/",
        "email-protection",
        "add-to-cart",
        "product-category",
        "wp-content",
        "wp-json",
        "mailto:",
        "tel:",
        "?per_page=",
        "shortcode=",
    ]
    if any(x in u for x in bad_parts):
        return False
    return "tireex.com/product/" in u or "/product/" in (urlparse(u).path or "")


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


def _extract_product_links(base_url: str, soup: BeautifulSoup) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    scope = soup.select_one("main") or soup.select_one(".woocommerce") or soup
    anchors = scope.select("ul.products a[href], .products a[href], li.product a[href], .product-card a[href], a.product-card-content-title[href]")
    if not anchors:
        anchors = scope.select("a[href]")
    for a in anchors:
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


def _category_page_candidates(category_url: str, page: int) -> List[str]:
    base = (category_url or "").rstrip("/")
    if page <= 1:
        return [base + "/"]
    return [
        f"{base}/page/{page}/",
        f"{base}/?product-page={page}",
        f"{base}/?paged={page}",
    ]


def _extract_list_products(base_url: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    main_scope = soup.select_one("main .woocommerce") or soup.select_one("main") or soup.select_one(".woocommerce") or soup
    cards = main_scope.select(
        "ul.products li.product, .products .product, .woocommerce ul.products li.product"
    )
    if not cards:
        # theme fallback still inside main/woocommerce scope only
        cards = main_scope.select(".products .product-card, .product-card, a.product-card-content-title")
    log.info("tireex detected listing cards=%s url=%s", len(cards), base_url)
    if not cards:
        log.warning("tireex detected listing cards=0 url=%s", base_url)
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
        if not _is_valid_product_url(product_url):
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
        size_token = _extract_size_token(name) or _extract_size_from_url(product_url)
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
                "_size_token": size_token,
            }
        )
    log.info("tireex listing products after size filter=%s url=%s", len(out), base_url)
    return out


def _is_brand_match(name: str, selected_brand: str) -> bool:
    sb = normalize_brand_name(selected_brand or "").lower().strip()
    if not sb:
        return True
    candidate = normalize_brand_name(name or "").lower()
    return sb in candidate or sb in (name or "").lower()


def _infer_brand_from_url(url: str) -> str:
    path = (urlparse(url).path or "").strip("/")
    if not path:
        return ""
    parts = [p for p in path.split("/") if p]
    if not parts:
        return ""
    tail = parts[-1]
    tail = re.sub(r"^(product-category|category|brand|brands)-?", "", tail, flags=re.IGNORECASE)
    tail = re.sub(r"-tires?$|-tyres?$|-كفرات$|-اطارات$|-إطارات$", "", tail, flags=re.IGNORECASE)
    tail = tail.replace("-", " ").replace("_", " ").strip()
    return normalize_brand_name(tail) if tail else ""


def _infer_brand_from_name(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    first = s.split(" ")[0]
    return normalize_brand_name(first)


def _name_contains_brand(name: str, selected_brand: str) -> bool:
    n = normalize_brand_name(name or "").lower()
    b = normalize_brand_name(selected_brand or "").lower()
    return bool(b and b in n)


def _is_explicit_other_brand(name: str, selected_brand: str) -> bool:
    b = normalize_brand_name(selected_brand or "").lower()
    g = _infer_brand_from_name(name).lower()
    return bool(b and g and g != b and not _name_contains_brand(name, selected_brand))


def _infer_brand_from_text(text: str) -> str:
    # keep backward compatibility for callers; generic inference now
    return _infer_brand_from_url(text) or _infer_brand_from_name(text)


def _has_explicit_other_brand(name: str, selected_brand: str) -> bool:
    # Keep scraper permissive to avoid false zero-results.
    # Final strict brand enforcement happens in pipeline after parsing.
    return False


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
        size_token = _extract_size_from_url(product_url)
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


def scrape_tireex(
    url: str,
    *,
    multi_pages: bool = False,
    max_pages: int = 5,
    limit: int = 20,
    selected_brand: str = "",
) -> List[Dict[str, Any]]:
    started_at = time.perf_counter()
    links: List[str] = []
    listing_items: List[Dict[str, Any]] = []
    max_items = max(1, int(limit or 20))
    ignored_links = 0
    accepted_cards = 0
    visited_product_urls: Set[str] = set()
    seen_card_keys: Set[str] = set()
    inferred_brand = selected_brand
    if not inferred_brand:
        inferred_brand = _infer_brand_from_url(url)
    if _is_product_url(url):
        links = [url]
    else:
        visited_pages: Set[str] = set()
        page_count = 0
        while page_count < max_pages:
            page_count += 1
            page_candidates = _category_page_candidates(url, page_count)
            current = ""
            doc = None
            for candidate in page_candidates:
                if candidate in visited_pages:
                    continue
                try:
                    doc = _fetch(candidate)
                    current = candidate
                    visited_pages.add(candidate)
                    break
                except Exception:
                    continue
            if not current or doc is None:
                break
            raw_cards = _extract_list_products(current, doc)
            log.warning("tireex product cards count=%s page=%s", len(raw_cards), current)
            for c in raw_cards:
                if len(listing_items) >= max_items:
                    break
                product_url = str(c.get("product_url") or "").strip()
                name = str(c.get("name") or "").strip()
                if not product_url or not name:
                    ignored_links += 1
                    continue
                # pre-filter before entering product page
                if urlparse(product_url).netloc != urlparse(url).netloc:
                    ignored_links += 1
                    continue
                if product_url in visited_product_urls:
                    ignored_links += 1
                    continue
                if inferred_brand:
                    on_brand_page = bool(_infer_brand_from_text(url))
                    # On brand/category page, allow neutral names to avoid zero results.
                    if not _is_brand_match(name, inferred_brand) and not on_brand_page:
                        log.info("SKIPPED_WRONG_BRAND card=%s selected=%s", name, inferred_brand)
                        ignored_links += 1
                        continue
                dedup_key = f"{normalize_brand_name(inferred_brand or '').lower()}::{c.get('_size_token','')}::{name.lower()}"
                if dedup_key in seen_card_keys:
                    ignored_links += 1
                    continue
                seen_card_keys.add(dedup_key)
                visited_product_urls.add(product_url)
                listing_items.append(c)
                links.append(product_url)
                accepted_cards += 1
            if len(listing_items) >= max_items:
                break
            if not multi_pages:
                break
            # continue to next page number candidates
    listing_by_url = {str(x.get("product_url") or "").strip(): x for x in listing_items if (x.get("product_url") or "").strip()}
    products: List[Dict[str, Any]] = []
    for u in links[: max_items * 2]:
        if len(products) >= max_items:
            break
        try:
            time.sleep(0.2)
            p = _parse_product_page(u)
            # preserve listing values when product page misses fields.
            base = listing_by_url.get(u, {})
            merged = {**base, **p}
            if not merged.get("name"):
                merged["name"] = base.get("name", "")
            if not merged.get("_size_token"):
                merged["_size_token"] = base.get("_size_token", "")
            if not merged.get("price"):
                merged["price"] = base.get("price", "")
            if not merged.get("old_price"):
                merged["old_price"] = base.get("old_price", "")
            if not merged.get("image_url"):
                merged["image_url"] = base.get("image_url", "")
            if not merged.get("year"):
                merged["year"] = base.get("year", "")
            if not merged.get("pattern"):
                merged["pattern"] = base.get("pattern", "")
            p = merged
            if not p.get("name"):
                log.info("tireex skip product reason=no_name url=%s", u)
                continue
            if inferred_brand:
                pname = p.get("name", "")
                if not _is_brand_match(pname, inferred_brand):
                    base_name = str(base.get("name", "")).strip()
                    # if card already accepted from brand page, don't drop neutral titles here
                    if not base_name:
                        log.info("SKIPPED_WRONG_BRAND product=%s selected=%s", pname, inferred_brand)
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
            base = listing_by_url.get(u, {})
            if base.get("name") and base.get("product_url"):
                if not base.get("_size_token"):
                    base["_size_token"] = _extract_size_token(base.get("name", "")) or _extract_size_from_url(base.get("product_url", ""))
                if base.get("_size_token"):
                    products.append(base)
                    continue
            log.warning("skip product %s: %s", u, e)
    elapsed = time.perf_counter() - started_at
    log.info(
        "tireex scrape done cards=%s accepted=%s ignored=%s total_seconds=%.2f",
        len(listing_items),
        len(products),
        ignored_links,
        elapsed,
    )
    return products[:max_items]

