import re
import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0"}
URLS = [
    "https://almuradstore.com/products",
    "https://almuradstore.com/",
]

for url in URLS:
    print("\n===", url)
    r = requests.get(url, headers=UA, timeout=30)
    print("status", r.status_code, "len", len(r.text), "final", r.url)
    soup = BeautifulSoup(r.text, "html.parser")
    for sel in [
        "ul.products li.product",
        "div.product-box",
        "article",
        "[class*='product-card']",
        "[class*='ProductCard']",
        "a[href*='/product']",
        "a[href*='almuradstore.com/']",
    ]:
        print(" ", sel, len(soup.select(sel)))
    h2 = soup.select("h2")
    h3 = soup.select("h3")
    print(" h2", len(h2), "h3", len(h3))
    links = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        if any(x in href.lower() for x in ("cart", "login", "categories", "faqs", "javascript")):
            continue
        text = a.get_text(" ", strip=True)
        if len(text) < 8:
            continue
        full = requests.compat.urljoin(url, href)
        if "almuradstore.com" in full and full not in links:
            links.append((text[:50], full))
        if len(links) >= 8:
            break
    print(" sample links:")
    for t, h in links:
        print("  ", t, "->", h)
    pag = sorted(set(re.findall(r'href="([^"]*(?:page|paged)=\d+[^"]*)"', r.text, re.I)))
    print(" pagination", pag[:5])
    print(" salla", bool(re.search(r"salla", r.text, re.I)))
    print(" json-ld", len(soup.select('script[type="application/ld+json"]')))
