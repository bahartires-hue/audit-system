from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("importer.tireex")

_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
}

_API_HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


def _http_get(url: str, *, timeout: int = 45, headers: Optional[Dict[str, str]] = None) -> requests.Response:
    hdrs = headers or _REQUEST_HEADERS
    try:
        return requests.get(url, timeout=timeout, headers=hdrs)
    except requests.exceptions.SSLError:
        log.warning("tireex ssl verify failed; retrying without verification url=%s", url)
        return requests.get(url, timeout=timeout, headers=hdrs, verify=False)


def _effective_max_items(limit: int, max_pages: int) -> int:
    """
    limit > 0: سقف صريح بعدد المنتجات.
    limit <= 0: سحب كامل للكتالوج ضمن سقف أمان (بيئة AUDITFLOW_IMPORTER_MAX_ITEMS، افتراضي 200000).
    """
    if int(limit or 0) > 0:
        return int(limit)
    try:
        cap = int((os.getenv("AUDITFLOW_IMPORTER_MAX_ITEMS") or "200000").strip())
    except ValueError:
        cap = 200_000
    cap = max(10_000, min(cap, 2_000_000))
    return max(cap, int(max_pages or 1) * 1000)
_SIZE_RE = re.compile(r"(\d{3})\s*/\s*(\d{2,3})\s*(?:ZR|R)?\s*(\d{2})", re.IGNORECASE)
_GENERIC_TITLE_RE = re.compile(r"(تصنيف|عروض|منتجات|product category|category)", re.IGNORECASE)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _fetch(url: str) -> BeautifulSoup:
    r = _http_get(url, timeout=45)
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
    raw = _clean(text).replace("٬", ",")
    nums = re.findall(r"(\d[\d\.,]*)\s*(?:ر\.س|SAR|ريال)?", raw, flags=re.IGNORECASE)
    vals: List[float] = []
    for n in nums:
        try:
            t = n.strip()
            if "," in t and "." in t:
                if t.rfind(",") > t.rfind("."):
                    t = t.replace(".", "").replace(",", ".")
                else:
                    t = t.replace(",", "")
            elif "," in t and "." not in t:
                # غالبا فاصلة آلاف
                if t.count(",") > 1:
                    t = t.replace(",", "")
                else:
                    left, right = t.split(",", 1)
                    if len(right) == 3:
                        t = left + right
                    else:
                        t = left + "." + right
            elif "." in t and "," not in t and t.count(".") > 1:
                t = t.replace(".", "")
            v = float(t)
            if 10 <= v <= 100000:
                vals.append(v)
        except Exception:
            continue
    if not vals:
        return raw
    # نرجع أول قيمة صالحة كنص للحفاظ على تنسيق الحقول الحالية.
    v = vals[0]
    return str(int(v)) if v.is_integer() else str(v)


def _format_price(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else str(v)


def _extract_price_pair(price_container) -> tuple[str, str]:
    """
    يرجّع (current_price, old_price) مع أولوية:
    - ins/new/current => السعر الحقيقي الحالي
    - del/old/was => السعر المشطوب
    """
    if not price_container:
        return "", ""

    current_text = ""
    old_text = ""

    ins_node = price_container.select_one("ins .amount, ins bdi, ins, .new-price, .current-price")
    del_node = price_container.select_one("del .amount, del bdi, del, .old-price, .was-price")

    if ins_node:
        current_text = ins_node.get_text(" ", strip=True)
    if del_node:
        old_text = del_node.get_text(" ", strip=True)

    current_price = _extract_price_value(current_text) if current_text else ""
    old_price = _extract_price_value(old_text) if old_text else ""

    # fallback: إذا لم نجد ins/del لكن يوجد أكثر من رقم داخل السعر
    if not current_price:
        all_vals: List[float] = []
        text = price_container.get_text(" ", strip=True)
        nums = re.findall(r"(\d[\d\.,]*)", text.replace("٬", ","))
        for n in nums:
            parsed = _extract_price_value(n)
            try:
                all_vals.append(float(parsed))
            except Exception:
                continue
        if all_vals:
            # في أغلب قوالب WooCommerce السعر الأخير هو الحالي
            current_price = _format_price(all_vals[-1])
            if len(all_vals) > 1:
                old_price = _format_price(all_vals[0])

    return current_price, old_price


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


def _store_api_category_slug(seed_url: str) -> str:
    path = (urlparse(seed_url).path or "").strip("/").lower()
    m = re.match(r"^product-category/([^/]+)/?$", path)
    return (m.group(1) or "").strip() if m else ""


def _store_api_price_string(prices: Dict[str, Any]) -> str:
    if not isinstance(prices, dict):
        return ""
    raw = str(prices.get("price") or "").strip()
    try:
        minor = int(prices.get("currency_minor_unit") or 2)
    except (TypeError, ValueError):
        minor = 2
    if not raw or not raw.isdigit():
        return ""
    value = int(raw) / (10**minor)
    return str(int(value)) if float(value).is_integer() else f"{value:.{minor}f}".rstrip("0").rstrip(".")


def _store_api_listing_rows(seed_url: str, max_pages: int, max_items: int) -> List[Dict[str, Any]]:
    """
    WooCommerce Store API fallback for Tireex category pages.
    يتجاوز مشاكل HTML الناقص/الحماية ويعيد منتجات التصنيف مباشرة من الـ API.
    """
    slug = _store_api_category_slug(seed_url)
    if not slug:
        return []

    base = f"{urlparse(seed_url).scheme}://{urlparse(seed_url).netloc}"
    cat_api = f"{base}/wp-json/wp/v2/product_cat?slug={slug}"
    try:
        r = _http_get(cat_api, timeout=30, headers=_API_HEADERS)
        r.raise_for_status()
        cats = r.json()
    except Exception as e:
        log.info("tireex store_api category lookup failed slug=%s err=%s", slug, e)
        return []

    if not isinstance(cats, list) or not cats:
        log.info("tireex store_api category not found slug=%s", slug)
        return []

    cat = next((x for x in cats if str(x.get("slug") or "").strip().lower() == slug), cats[0])
    cat_id = cat.get("id")
    if not cat_id:
        return []

    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    per_page = min(100, max(20, min(max_items, 100))) if int(max_items or 0) > 0 else 100
    for page in range(1, max(1, int(max_pages or 1)) + 1):
        api_url = f"{base}/wp-json/wc/store/v1/products?category={cat_id}&page={page}&per_page={per_page}"
        try:
            r = _http_get(api_url, timeout=30, headers=_API_HEADERS)
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            log.info("tireex store_api products failed page=%s cat_id=%s err=%s", page, cat_id, e)
            break
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if not isinstance(row, dict):
                continue
            product_url = _clean(str(row.get("permalink") or ""))
            if not product_url or product_url in seen:
                continue
            seen.add(product_url)
            images = row.get("images") or []
            image_url = ""
            if isinstance(images, list) and images:
                first = images[0] or {}
                if isinstance(first, dict):
                    image_url = _clean(str(first.get("src") or first.get("thumbnail") or ""))
            name = _clean(str(row.get("name") or ""))
            out.append(
                {
                    "name": name,
                    "price": _store_api_price_string(row.get("prices") or {}),
                    "old_price": "",
                    "product_url": product_url,
                    "image_url": image_url,
                    "year": "",
                    "country": "",
                    "warranty": "",
                    "pattern": "",
                    "description": "",
                    "_size_token": _extract_size_token(name),
                }
            )
            if len(out) >= max_items:
                break
        if len(out) >= max_items or len(rows) < per_page:
            break

    log.info("tireex store_api products=%s slug=%s seed_url=%s", len(out), slug, seed_url)
    return out


def _extract_gtm_embedded_listings(base_url: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Tireex/WoodMart + GTM4WP: كل بطاقة غالبًا تحوي JSON في data-gtm4wp_product_data
    (item_name, productlink, price, image) — يبقى مكتملًا حتى لو DOM الكروت ناقص على بعض الاستضافات.
    """
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
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
        name = _clean(str(data.get("item_name") or ""))
        product_url = _clean(str(data.get("productlink") or ""))
        if not product_url:
            continue
        if not product_url.startswith("http"):
            product_url = urljoin(base_url, product_url)
        if urlparse(product_url).netloc != urlparse(base_url).netloc:
            continue
        pth = (urlparse(product_url).path or "").lower()
        if "/product/" not in pth and "/shop/" not in pth:
            continue
        if product_url in seen:
            continue
        seen.add(product_url)
        price = ""
        try:
            pv = float(data.get("price"))
            if 0 < pv <= 1_000_000:
                price = str(int(pv)) if pv == int(pv) else str(pv)
        except (TypeError, ValueError):
            pass
        img = str(data.get("image") or "").strip()
        if img and not img.startswith("http"):
            img = urljoin(base_url, img)
        out.append(
            {
                "name": name,
                "price": price,
                "old_price": "",
                "product_url": product_url,
                "image_url": img,
                "year": "",
                "country": "",
                "warranty": "",
                "pattern": "",
                "description": "",
                "_size_token": _extract_size_token(name),
            }
        )
    log.info("tireex gtm_embedded products=%s url=%s", len(out), base_url)
    return out


def _merge_listing_rows_unique(listing_items: List[Dict[str, Any]], rows: List[Dict[str, Any]]) -> int:
    added = 0
    for row in rows:
        pu = (row.get("product_url") or "").strip()
        if not pu:
            continue
        if any((x.get("product_url") or "") == pu for x in listing_items):
            continue
        listing_items.append(row)
        added += 1
    return added


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
    bad_words = ("add-to-cart", "cart", "checkout", "tag")
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
        ".wd-pagination a.next.page-numbers",
        ".wd-pagination a.next",
    ]
    for sel in candidates:
        a = soup.select_one(sel)
        if a and a.get("href"):
            u = urljoin(base_url, a.get("href"))
            if urlparse(u).netloc == urlparse(base_url).netloc:
                return u
    return ""


def _with_page_path(url: str, page: int) -> str:
    p = urlparse(url)
    parts = [x for x in (p.path or "").split("/") if x]
    if "page" in parts:
        i = parts.index("page")
        parts = parts[:i]
    if parts and re.fullmatch(r"\d+", parts[-1]) and len(parts) >= 2 and parts[-2] == "page":
        parts = parts[:-2]
    parts += ["page", str(page)]
    path = "/" + "/".join(parts) + "/"
    return urlunparse((p.scheme, p.netloc, path, p.params, p.query, p.fragment))


def _with_paged_query(url: str, page: int) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q["paged"] = str(page)
    query = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, query, p.fragment))


def _with_page_num_query(url: str, page: int) -> str:
    """بعض القوالب تستخدم ?page=N بدل ?paged=N."""
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q["page"] = str(page)
    query = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, query, p.fragment))


def _implicit_next_listing_urls(seed_url: str, current_page_one_based: int) -> List[str]:
    """صفحة القائمة التالية بأشكال URL شائعة دون الاعتماد على رابط «التالي» في HTML."""
    nxt = current_page_one_based + 1
    out: List[str] = []
    for factory in (_with_page_path, _with_paged_query, _with_page_num_query):
        u = factory(seed_url, nxt)
        if u and u not in out:
            out.append(u)
    return out


def _pagination_candidates(seed_url: str, current_url: str, current_page: int, soup: BeautifulSoup) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    explicit_next = _next_page_url(current_url, soup)
    for candidate in [
        explicit_next,
        _with_page_path(seed_url, current_page + 1),
        _with_paged_query(seed_url, current_page + 1),
        _with_page_num_query(seed_url, current_page + 1),
    ]:
        if not candidate:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        out.append(candidate)
    # روابط أرقام الصفحات في ووكومرس (أحياناً لا يوجد class next واضح)
    for a in soup.select(
        ".woocommerce-pagination a.page-numbers, ul.page-numbers a.page-numbers, .wd-pagination a.page-numbers"
    ):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        u = urljoin(current_url, href)
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _bruteforce_listing_pages(
    seed_url: str,
    *,
    visited_pages: Set[str],
    max_pages: int,
    max_items: int,
    links: List[str],
    listing_items: List[Dict[str, Any]],
) -> None:
    """
    يزور صفحات 2..max_pages بأشكال URL شائعة حتى لو فشل الترقيم عبر DOM.
    لا يتوقف مبكراً بعد صفحات فارغة — يكمّل حتى max_pages (أو حتى يكتمل max_items).
    """
    for pnum in range(2, max_pages + 1):
        if len(links) >= max_items:
            break
        probes: List[str] = []
        for factory in (_with_page_path, _with_paged_query, _with_page_num_query):
            probe = factory(seed_url, pnum)
            if probe and probe not in visited_pages and probe not in probes:
                probes.append(probe)
        page_added = 0
        for probe in probes:
            if not _in_same_scope(seed_url, probe):
                continue
            try:
                docx = _fetch(probe)
            except Exception as e:
                log.info("tireex bruteforce skip fetch p=%s url=%s err=%s", pnum, probe, e)
                continue
            visited_pages.add(probe)
            for u in _extract_product_links(probe, docx):
                if u not in links:
                    links.append(u)
                    page_added += 1
            for row in _extract_list_products(probe, docx):
                pu = row.get("product_url") or ""
                if pu and not any((x.get("product_url") or "") == pu for x in listing_items):
                    listing_items.append(row)
            _merge_listing_rows_unique(listing_items, _extract_gtm_embedded_listings(probe, docx))
        log.info("tireex bruteforce page=%s new_product_links=%s total_links=%s", pnum, page_added, len(links))


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
        price, old_price = _extract_price_pair(price_node)
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
    seed_path = (seed.path or "").strip("/").lower()
    cand_path = (cand.path or "").strip("/").lower()
    if not seed_path:
        # للرابط الجذري: نسمح فقط بروابط pagination المعروفة.
        return ("page/" in cand_path) or ("paged=" in cand.query) or (cand_path == "")
    if cand_path.startswith(seed_path):
        return True
    seed_parts = seed_path.split("/")
    cand_parts = cand_path.split("/")
    # WooCommerce: نفس مسار التصنيف ثم page/N
    if len(cand_parts) >= len(seed_parts) + 2 and cand_parts[len(seed_parts)] == "page":
        if cand_parts[: len(seed_parts)] == seed_parts:
            return True
    cand_q = {k.lower(): v for k, v in parse_qsl(cand.query, keep_blank_values=True)}
    cat_slug = seed_parts[-1] if seed_parts else ""
    # شكل شائع: /page/2/?product_cat=sailun-tires
    if cand_parts and cand_parts[0] == "page" and len(cand_parts) >= 2 and re.fullmatch(r"\d+", cand_parts[1] or ""):
        pc = (cand_q.get("product_cat") or cand_q.get("category_name") or "").strip().lower().replace(" ", "-")
        if cat_slug and pc and (pc == cat_slug or pc in cat_slug or cat_slug in pc):
            return True
    base_prefix = seed_parts[0]
    return cand_path.startswith(base_prefix)


def _parse_product_page(product_url: str) -> Dict[str, Any]:
    doc = _fetch(product_url)
    name = _pick_text(doc, ["h1.product_title", ".product_title", ".product-title", "h1", "h2"])
    if not name:
        name = _pick_attr(doc, ["meta[property='og:title']", "meta[name='twitter:title']"], "content")
    size_token = _extract_size_token(name)
    if not size_token:
        # لا نستخدم نص الوصف أو body أو og:description لاستخراج المقاس — مناطق المنتج المنظمة فقط.
        size_token = _extract_size_token(
            " ".join(
                x
                for x in [
                    name,
                    _pick_text(doc, [".product_meta", ".summary"]),
                    _pick_text(doc, ["table.variations", ".woocommerce-product-attributes", ".shop_attributes"]),
                ]
                if x
            )
        )
    if not size_token:
        summary_el = doc.select_one(".summary")
        if summary_el:
            size_token = _extract_size_token(_clean(summary_el.get_text(" ", strip=True)))
    page_text = _clean(doc.get_text(" ", strip=True))
    price_node = doc.select_one(".summary .price, .product .price, .woocommerce-variation-price .price, .price")
    price, old_price = _extract_price_pair(price_node)
    if not price:
        price = _extract_price_value(_pick_text(doc, [".price .amount", ".price", "[class*='price'] .amount", "bdi"]))
    if not price:
        price = _extract_price_value(page_text)
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
    country = _pick_text(
        doc,
        [
            ".country",
            "[class*='origin']",
            ".origin",
            ".origin-country",
            ".product-country",
            ".manufacture-country",
            "[data-country]",
            ".woocommerce-product-attributes-item--attribute_pa_country",
        ],
    ) or _find_detail(
        page_text,
        [
            "بلد المنشأ",
            "بلد الصنع",
            "بلد الإنتاج",
            "صنع في",
            "Origin",
            "Country",
            "Country of Origin",
            "Country of Manufacture",
            "Manufactured in",
            "Made in",
        ],
    )
    pattern = _pick_text(doc, [".pattern", "[class*='pattern']"]) or _find_detail(page_text, ["النقشة", "Pattern", "Tread"])
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
        "description": "",
        "_size_token": size_token,
    }


def _tireex_progress(progress_cb: Optional[Callable[[int, str], None]], pct: int, msg: str) -> None:
    if not progress_cb:
        return
    try:
        progress_cb(max(0, min(100, int(pct))), msg)
    except Exception:
        pass


def scrape_tireex(
    url: str,
    *,
    multi_pages: bool = False,
    max_pages: int = 10,
    limit: int = 100,
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> List[Dict[str, Any]]:
    links: List[str] = []
    listing_items: List[Dict[str, Any]] = []
    max_items = _effective_max_items(limit, max_pages)
    if _is_product_url(url):
        links = [url]
        _tireex_progress(progress_cb, 15, "جاري تحليل صفحة المنتج...")
    else:
        # WooCommerce Store API هو fallback أقوى من HTML على بعض البيئات (مثل Render)
        # عندما تُرجع الصفحة HTML ناقصًا أو بلا كروت منتجات.
        store_rows = _store_api_listing_rows(url, max_pages=max_pages, max_items=max_items)
        if store_rows:
            _merge_listing_rows_unique(listing_items, store_rows)
            for row in store_rows:
                pu = (row.get("product_url") or "").strip()
                if pu and pu not in links:
                    links.append(pu)
            _tireex_progress(progress_cb, 12, f"Store API: تم جمع {len(store_rows)} منتجًا")
        current = url
        visited_pages: Set[str] = set()
        page_count = 0
        while current and current not in visited_pages and page_count < max_pages:
            visited_pages.add(current)
            page_count += 1
            _tireex_progress(
                progress_cb,
                max(4, int(20 * (page_count - 1) / max(max_pages, 1))),
                f"جاري فتح صفحة القائمة {page_count}/{max_pages}...",
            )
            try:
                doc = _fetch(current)
            except Exception as e:
                log.warning("skip listing page %s: %s", current, e)
                break
            page_products = _extract_list_products(current, doc)
            listing_items.extend(page_products)
            _merge_listing_rows_unique(listing_items, _extract_gtm_embedded_listings(current, doc))
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
            next_page_url = ""
            candidates = _pagination_candidates(url, current, page_count, doc)
            for nxt in candidates:
                if not _in_same_scope(url, nxt):
                    continue
                if nxt in visited_pages:
                    continue
                next_page_url = nxt
                break
            log.info(
                "pagination current_page=%s products_found_on_page=%s total_products_collected=%s listing_items=%s next_page_url=%s",
                page_count,
                len(page_products),
                len(links),
                len(listing_items),
                next_page_url,
            )
            _tireex_progress(
                progress_cb,
                int(22 * page_count / max(max_pages, 1)),
                f"صفحات القائمة {page_count}/{max_pages} — روابط {len(links)} / عناصر {len(listing_items)}",
            )
            if len(links) >= max_items:
                break
            if not multi_pages:
                break
            if not next_page_url:
                for fb in _implicit_next_listing_urls(url, page_count):
                    if fb and fb not in visited_pages and _in_same_scope(url, fb):
                        next_page_url = fb
                        break
                if not next_page_url:
                    break
            current = next_page_url
        if multi_pages:
            _bruteforce_listing_pages(
                url,
                visited_pages=visited_pages,
                max_pages=max_pages,
                max_items=max_items,
                links=links,
                listing_items=listing_items,
            )
    products: List[Dict[str, Any]] = []
    # كان التحليل يعتمد على links فقط؛ listing_items (كروت القائمة) قد تجمع كل المنتجات
    # بينما _extract_product_links يفوّت روابطاً (عرض شبكي، href داخل عنصر آخر، إلخ).
    parse_order: List[str] = []
    seen_u: Set[str] = set()
    for u in links:
        if u and u not in seen_u:
            seen_u.add(u)
            parse_order.append(u)
    for row in listing_items:
        u = (row.get("product_url") or "").strip()
        if u and u not in seen_u:
            seen_u.add(u)
            parse_order.append(u)
    cap_parse = min(500_000, max(len(parse_order), max_items * 10, 500))
    parse_urls = parse_order[:cap_parse]
    log.info(
        "tireex parse_queue links=%s listing_items=%s merged_unique=%s parse_cap=%s",
        len(links),
        len(listing_items),
        len(parse_order),
        cap_parse,
    )
    n_parse = len(parse_urls)
    for i, u in enumerate(parse_urls):
        if len(products) >= max_items:
            break
        try:
            p = _parse_product_page(u)
            if not p.get("name"):
                log.info("tireex skip product reason=no_name url=%s", u)
                continue
            if not p.get("_size_token"):
                log.info("tireex product no_size_token (keeping) name=%s url=%s", p.get("name", ""), u)
            if not p.get("product_url"):
                log.info("tireex skip product reason=no_product_url url=%s", u)
                continue
            if not p.get("image_url"):
                log.info("tireex product has no image url=%s", u)
            products.append(p)
        except Exception as e:
            log.warning("skip product %s: %s", u, e)
        if progress_cb and n_parse and (i % 2 == 0 or i + 1 == n_parse):
            _tireex_progress(
                progress_cb,
                25 + int(75 * (i + 1) / n_parse),
                f"تحليل صفحات المنتجات {i + 1}/{n_parse} — مكتمل {len(products)}",
            )
    # لا نُرجع المنتجات المحللة فقط؛ إذا فشل فتح بعض صفحات المنتج نبقي صفوف الكروت
    # كـ fallback حتى لا ينخفض العدد النهائي بسبب timeouts / blocking / parsing failures.
    merged_products: List[Dict[str, Any]] = []
    parsed_by_url: Dict[str, Dict[str, Any]] = {}
    for item in products:
        pu = (item.get("product_url") or "").strip()
        if pu and pu not in parsed_by_url:
            parsed_by_url[pu] = item

    seen_final: Set[str] = set()
    for item in listing_items:
        pu = (item.get("product_url") or "").strip()
        if not pu or pu in seen_final:
            continue
        seen_final.add(pu)
        merged = {**item, **parsed_by_url.get(pu, {})}
        if merged.get("name") and merged.get("product_url"):
            merged_products.append(merged)

    for item in products:
        pu = (item.get("product_url") or "").strip()
        if not pu or pu in seen_final:
            continue
        seen_final.add(pu)
        if item.get("name") and item.get("product_url"):
            merged_products.append(item)

    if merged_products:
        log.info(
            "tireex merged result parsed=%s listing_items=%s final=%s",
            len(products),
            len(listing_items),
            len(merged_products),
        )
        _tireex_progress(progress_cb, 100, f"اكتمل جمع {len(merged_products)} منتج من الموقع")
        return merged_products[:max_items]
    # fallback إذا فشل parsing صفحات المنتج: نعيد منتجات الكروت من صفحة الماركة/البحث.
    if not listing_items:
        log.warning("tireex no products found on listing; trying link extraction fallback")
        try:
            links = _extract_product_links(url, _fetch(url))
        except Exception as e:
            log.warning("tireex fallback link extraction failed: %s", e)
            links = []
        for u in links[: max_items * 4]:
            if len(listing_items) >= max_items:
                break
            try:
                p = _parse_product_page(u)
                if p.get("name") and p.get("product_url"):
                    listing_items.append(p)
            except Exception as e:
                log.warning("skip fallback product %s: %s", u, e)
    # إن رجعت من الكروت فقط، نحاول ترقية البيانات بدخول صفحات المنتج.
    upgraded: List[Dict[str, Any]] = []
    n_list = min(len(listing_items), max_items)
    for j, item in enumerate(listing_items):
        if len(upgraded) >= max_items:
            break
        u = item.get("product_url", "")
        try:
            p = _parse_product_page(u) if u else {}
            merged = {**item, **p}
            if merged.get("name") and merged.get("product_url"):
                upgraded.append(merged)
            else:
                log.info("tireex skip upgraded card reason=missing_required url=%s", u)
        except Exception as e:
            log.warning("skip upgraded card %s: %s", u, e)
        if progress_cb and n_list and (j % 2 == 0 or j + 1 == n_list):
            _tireex_progress(
                progress_cb,
                30 + int(70 * (j + 1) / max(n_list, 1)),
                f"ترقية بيانات المنتجات {j + 1}/{n_list} — مكتمل {len(upgraded)}",
            )
    _tireex_progress(progress_cb, 100, f"اكتمل جمع {len(upgraded)} منتج من الموقع")
    return upgraded[:max_items]

