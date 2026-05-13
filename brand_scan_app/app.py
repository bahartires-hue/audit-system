from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# =========================
# إعدادات عامة
# =========================

BASE_DIR = Path(__file__).resolve().parent
EXPORTS_DIR = BASE_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="مستورد الإطارات - Brand Deep Scan")

# لو حاب تضيف static لاحقًا
static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# =========================
# إعدادات المواقع
# عدّلها حسب المواقع اللي تشتغل عليها
# =========================

SITE_CONFIG: Dict[str, Dict[str, Any]] = {
    "tireex": {
        "label": "TireEX",
        "base_url": "https://tireex.com",
        "start_urls": ["https://tireex.com/shop/"],
        "product_link_selector": "ul.products li.product a.woocommerce-LoopProduct-link",
        "product_title_selector": "h1.product_title",
        "brand_selector": None,
        "price_selector": "p.price",
        "image_selector": "figure.woocommerce-product-gallery__wrapper img",
        "description_selector": "div.woocommerce-Tabs-panel--description",
    },
    "lumitires": {
        "label": "LumiTires",
        "base_url": "https://lumitiress.com",
        "start_urls": ["https://lumitiress.com/shop/"],
        "product_link_selector": "ul.products li.product a",
        "product_title_selector": "h1.product_title",
        "brand_selector": None,
        "price_selector": "p.price",
        "image_selector": "figure.woocommerce-product-gallery__wrapper img",
        "description_selector": "div.woocommerce-product-details__short-description",
    },
    "kafaratplus": {
        "label": "KafaratPlus",
        "base_url": "https://kafaratplus.com",
        "start_urls": ["https://kafaratplus.com/shop/"],
        "product_link_selector": "ul.products li.product a",
        "product_title_selector": "h1.product_title",
        "brand_selector": None,
        "price_selector": "p.price",
        "image_selector": "figure.woocommerce-product-gallery__wrapper img",
        "description_selector": "div.woocommerce-product-details__short-description",
    },
    "etar": {
        "label": "Etar",
        "base_url": "https://etar.com",
        "start_urls": ["https://etar.com/shop/"],
        "product_link_selector": "ul.products li.product a",
        "product_title_selector": "h1.product_title",
        "brand_selector": None,
        "price_selector": "p.price",
        "image_selector": "figure.woocommerce-product-gallery__wrapper img",
        "description_selector": "div.woocommerce-product-details__short-description",
    },
}


# =========================
# دوال مساعدة
# =========================

def get_soup(url: str, timeout: int = 15) -> BeautifulSoup:
    headers = {
        "User-Agent": "Mozilla/5.0 (TireImporterBot)"
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def extract_text(el) -> str:
    return el.get_text(strip=True) if el is not None else ""


def extract_attr(el, attr: str) -> str:
    if el is None:
        return ""
    return (el.get(attr) or "").strip()


def collect_product_links(
    site_key: str,
    start_urls: List[str],
    product_selector: str,
    base_url: str,
    max_pages: int = 200,
) -> List[str]:
    visited = set()
    product_links = set()
    to_visit = list(start_urls)

    while to_visit:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)

        try:
            soup = get_soup(url)
        except Exception:
            continue

        for a in soup.select(product_selector):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(base_url, href.split("?")[0])
            product_links.add(full)

        for a in soup.select("a.page-numbers, a.next, a.pagination-next"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(base_url, href)
            if full not in visited and len(visited) < max_pages:
                to_visit.append(full)

    return list(product_links)


# =========================
# Brand Deep Scan Mode
# =========================

def brand_deep_scan(
    site_key: str,
    brand: str,
    max_pages: int = 200,
    limit: int = 0,
) -> Dict[str, Any]:
    cfg = SITE_CONFIG.get(site_key)
    if not cfg:
        raise ValueError(f"site_key غير معروف: {site_key}")

    base_url = cfg["base_url"]
    start_urls = cfg["start_urls"]
    product_selector = cfg["product_link_selector"]
    title_selector = cfg["product_title_selector"]
    brand_selector = cfg.get("brand_selector")
    price_selector = cfg["price_selector"]
    image_selector = cfg["image_selector"]
    desc_selector = cfg["description_selector"]

    product_links = collect_product_links(
        site_key=site_key,
        start_urls=start_urls,
        product_selector=product_selector,
        base_url=base_url,
        max_pages=max_pages,
    )

    results: List[Dict[str, Any]] = []
    total = len(product_links) or 1

    for i, url in enumerate(product_links, start=1):
        try:
            soup = get_soup(url)
        except Exception:
            continue

        title = extract_text(soup.select_one(title_selector))

        detected_brand: Optional[str] = None
        if brand_selector:
            detected_brand = extract_text(soup.select_one(brand_selector))

        if not detected_brand:
            if brand.lower() in title.lower():
                detected_brand = brand
            else:
                continue

        price = extract_text(soup.select_one(price_selector))
        img_el = soup.select_one(image_selector)
        image_url = extract_attr(img_el, "src")
        description = extract_text(soup.select_one(desc_selector))

        results.append(
            {
                "url": url,
                "title": title,
                "brand": detected_brand,
                "price": price,
                "image": image_url,
                "description": description,
            }
        )

        if limit and len(results) >= limit:
            break

        time.sleep(0.15)

    return {
        "ok": True,
        "count": len(results),
        "items": results,
    }


# =========================
# سحب من رابط تصنيف عادي
# =========================

def scrape_category_simple(
    site_key: str,
    category_url: str,
    max_pages: int = 200,
    limit: int = 0,
    brand: str = "",
) -> Dict[str, Any]:
    cfg = SITE_CONFIG.get(site_key)
    if not cfg:
        raise ValueError(f"site_key غير معروف: {site_key}")

    base_url = cfg["base_url"]
    product_selector = cfg["product_link_selector"]
    title_selector = cfg["product_title_selector"]
    brand_selector = cfg.get("brand_selector")
    price_selector = cfg["price_selector"]
    image_selector = cfg["image_selector"]
    desc_selector = cfg["description_selector"]

    start_urls = [category_url]
    product_links = collect_product_links(
        site_key=site_key,
        start_urls=start_urls,
        product_selector=product_selector,
        base_url=base_url,
        max_pages=max_pages,
    )

    results: List[Dict[str, Any]] = []
    total = len(product_links) or 1

    for i, url in enumerate(product_links, start=1):
        try:
            soup = get_soup(url)
        except Exception:
            continue

        title = extract_text(soup.select_one(title_selector))

        detected_brand: Optional[str] = None
        if brand_selector:
            detected_brand = extract_text(soup.select_one(brand_selector))

        if brand:
            if detected_brand:
                if detected_brand.lower() != brand.lower():
                    continue
            else:
                if brand.lower() not in title.lower():
                    continue

        price = extract_text(soup.select_one(price_selector))
        img_el = soup.select_one(image_selector)
        image_url = extract_attr(img_el, "src")
        description = extract_text(soup.select_one(desc_selector))

        results.append(
            {
                "url": url,
                "title": title,
                "brand": detected_brand or brand or "",
                "price": price,
                "image": image_url,
                "description": description,
            }
        )

        if limit and len(results) >= limit:
            break

        time.sleep(0.15)

    return {
        "ok": True,
        "count": len(results),
        "items": results,
    }


# =========================
# دالة موحدة للسحب + حفظ CSV
# =========================

def run_universal_import(
    site_key: str,
    mode: str,
    brand: str = "",
    category_url: str = "",
    max_pages: int = 200,
    limit: int = 0,
) -> Dict[str, Any]:
    if mode == "deep":
        out = brand_deep_scan(
            site_key=site_key,
            brand=brand.strip(),
            max_pages=max_pages,
            limit=limit,
        )
    else:
        out = scrape_category_simple(
            site_key=site_key,
            category_url=category_url.strip(),
            max_pages=max_pages,
            limit=limit,
            brand=brand.strip(),
        )

    items = out.get("items") or []
    count = len(items)

    csv_path = EXPORTS_DIR / f"{site_key}_{mode}_products.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["title", "brand", "price", "image", "description", "url"],
        )
        writer.writeheader()
        for row in items:
            writer.writerow(
                {
                    "title": row.get("title", ""),
                    "brand": row.get("brand", ""),
                    "price": row.get("price", ""),
                    "image": row.get("image", ""),
                    "description": row.get("description", ""),
                    "url": row.get("url", ""),
                }
            )

    return {
        "ok": True,
        "count": count,
        "items": items,
        "csv_path": str(csv_path),
    }


# =========================
# API
# =========================

@app.post("/api/import")
async def api_import(request: Request):
    body = await request.json()
    site_key = (body.get("site_key") or "").strip()
    mode = (body.get("mode") or "deep").strip()  # deep أو category
    brand = (body.get("brand") or "").strip()
    category_url = (body.get("category_url") or "").strip()
    max_pages = int(body.get("max_pages") or 200)
    limit = int(body.get("limit") or 0)

    if site_key not in SITE_CONFIG:
        return JSONResponse({"ok": False, "error": "site_key غير معروف"}, status_code=400)

    if mode == "deep" and not brand:
        return JSONResponse({"ok": False, "error": "البراند مطلوب في وضع Deep Scan"}, status_code=400)

    if mode == "category" and not category_url:
        return JSONResponse({"ok": False, "error": "رابط التصنيف مطلوب في وضع Category"}, status_code=400)

    try:
        out = run_universal_import(
            site_key=site_key,
            mode=mode,
            brand=brand,
            category_url=category_url,
            max_pages=max_pages,
            limit=limit,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    return JSONResponse(out)


# =========================
# واجهة Dashboard بسيطة
# =========================

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="UTF-8" />
  <title>مستورد الإطارات</title>
  <style>
    body { margin:0; font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:#050816; color:#e5e7eb; }
    .layout { display:flex; min-height:100vh; }
    .sidebar { width:260px; background:#020617; border-left:1px solid #1f2937; padding:20px; box-sizing:border-box; }
    .sidebar h1 { font-size:20px; margin-bottom:20px; }
    .sidebar .section-title { font-size:13px; color:#9ca3af; margin-top:20px; margin-bottom:8px; }
    .sidebar ul { list-style:none; padding:0; margin:0; }
    .sidebar li { padding:6px 0; font-size:14px; color:#e5e7eb; opacity:0.8; }
    .main { flex:1; padding:24px 28px; box-sizing:border-box; }
    .card { background:#020617; border:1px solid #1f2937; border-radius:16px; padding:20px; margin-bottom:20px; }
    .card h2 { margin:0 0 12px 0; font-size:18px; }
    .row { display:flex; gap:12px; flex-wrap:wrap; }
    .field { flex:1; min-width:180px; }
    label { display:block; font-size:13px; color:#9ca3af; margin-bottom:4px; }
    select, input { width:100%; padding:8px 10px; border-radius:8px; border:1px solid #374151; background:#020617; color:#e5e7eb; font-size:14px; box-sizing:border-box; }
    select:focus, input:focus { outline:none; border-color:#6366f1; }
    button { padding:10px 18px; border-radius:999px; border:none; cursor:pointer; font-size:14px; }
    .btn-primary { background:linear-gradient(90deg,#6366f1,#8b5cf6); color:white; }
    .btn-secondary { background:#111827; color:#e5e7eb; border:1px solid #374151; }
    .top-actions { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-top:12px; flex-wrap:wrap; }
    .progress-wrap { margin-top:12px; }
    .progress-bar-bg { width:100%; height:8px; border-radius:999px; background:#111827; overflow:hidden; }
    .progress-bar-fill { height:100%; width:0%; background:linear-gradient(90deg,#22c55e,#a3e635); transition:width 0.2s; }
    .progress-text { font-size:13px; color:#9ca3af; margin-top:4px; display:flex; justify-content:space-between; }
    table { width:100%; border-collapse:collapse; margin-top:12px; font-size:13px; }
    th, td { padding:8px 6px; border-bottom:1px solid #111827; text-align:right; }
    th { color:#9ca3af; font-weight:500; }
    tr:hover td { background:#020617; }
    .badge { display:inline-flex; align-items:center; gap:4px; font-size:11px; padding:3px 8px; border-radius:999px; background:#111827; color:#9ca3af; }
    .badge-dot { width:6px; height:6px; border-radius:999px; background:#22c55e; }
    .muted { color:#6b7280; font-size:12px; }
    .pill { padding:4px 10px; border-radius:999px; border:1px solid #374151; font-size:11px; color:#9ca3af; display:inline-flex; align-items:center; gap:6px; }
    .pill-dot { width:6px; height:6px; border-radius:999px; background:#6366f1; }
    .header-row { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:10px; flex-wrap:wrap; }
  </style>
</head>
<body>
<div class="layout">
  <aside class="sidebar">
    <h1>مستورد الإطارات</h1>
    <div class="section-title">جلسات سابقة</div>
    <ul>
      <li>آخر استيراد: <span id="last-count">0 منتج</span></li>
    </ul>
    <div class="section-title">التصدير</div>
    <ul>
      <li>ملف CSV: <span id="last-csv" class="muted">لم يتم بعد</span></li>
    </ul>
    <div class="section-title">الوضع</div>
    <ul>
      <li>Deep Scan بالماركة</li>
      <li>أو من رابط تصنيف</li>
    </ul>
  </aside>
  <main class="main">
    <div class="card">
      <div class="header-row">
        <div>
          <h2>استيراد المنتجات</h2>
          <div class="muted">اختر الموقع وطريقة السحب، ثم ابدأ الاستيراد.</div>
        </div>
        <div class="pill">
          <span class="pill-dot"></span>
          وضع Brand Deep Scan
        </div>
      </div>
      <div class="row">
        <div class="field">
          <label>الموقع</label>
          <select id="site_key">
            <option value="tireex">TireEX</option>
            <option value="lumitires">LumiTires</option>
            <option value="kafaratplus">KafaratPlus</option>
            <option value="etar">Etar</option>
          </select>
        </div>
        <div class="field">
          <label>الوضع</label>
          <select id="mode">
            <option value="deep">Deep Scan بالماركة</option>
            <option value="category">من رابط تصنيف</option>
          </select>
        </div>
        <div class="field">
          <label>اسم الماركة (Brand)</label>
          <input id="brand" placeholder="مثال: Accelera" />
        </div>
        <div class="field">
          <label>رابط التصنيف (لو اخترت وضع Category)</label>
          <input id="category_url" placeholder="https://example.com/shop/accelera/" />
        </div>
      </div>
      <div class="row" style="margin-top:10px;">
        <div class="field">
          <label>أقصى عدد صفحات</label>
          <input id="max_pages" type="number" value="200" />
        </div>
        <div class="field">
          <label>حد أقصى للمنتجات (0 = بدون حد)</label>
          <input id="limit" type="number" value="0" />
        </div>
      </div>
      <div class="top-actions">
        <button class="btn-primary" id="start_btn">بدء الاستيراد</button>
        <span class="badge">
          <span class="badge-dot"></span>
          <span id="status_text">جاهز للاستيراد</span>
        </span>
      </div>
      <div class="progress-wrap">
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="progress_fill"></div>
        </div>
        <div class="progress-text">
          <span id="progress_label">0% جاري الاستيراد</span>
          <span id="progress_extra">0 منتج</span>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="header-row">
        <div>
          <h2>المنتجات المستوردة</h2>
          <div class="muted">سيتم عرض المنتجات بعد انتهاء عملية السحب.</div>
        </div>
      </div>
      <div style="overflow:auto; max-height:420px;">
        <table>
          <thead>
            <tr>
              <th>المنتج</th>
              <th>الماركة</th>
              <th>السعر</th>
              <th>الرابط</th>
            </tr>
          </thead>
          <tbody id="items_tbody">
          </tbody>
        </table>
      </div>
    </div>
  </main>
</div>

<script>
const startBtn = document.getElementById("start_btn");
const siteKeyEl = document.getElementById("site_key");
const modeEl = document.getElementById("mode");
const brandEl = document.getElementById("brand");
const categoryUrlEl = document.getElementById("category_url");
const maxPagesEl = document.getElementById("max_pages");
const limitEl = document.getElementById("limit");
const statusText = document.getElementById("status_text");
const progressFill = document.getElementById("progress_fill");
const progressLabel = document.getElementById("progress_label");
const progressExtra = document.getElementById("progress_extra");
const itemsTbody = document.getElementById("items_tbody");
const lastCountEl = document.getElementById("last-count");
const lastCsvEl = document.getElementById("last-csv");

function setProgress(pct, text, extra) {
  progressFill.style.width = pct + "%";
  progressLabel.textContent = pct + "% " + text;
  progressExtra.textContent = extra || "";
}

startBtn.addEventListener("click", async () => {
  const site_key = siteKeyEl.value;
  const mode = modeEl.value;
  const brand = brandEl.value.trim();
  const category_url = categoryUrlEl.value.trim();
  const max_pages = parseInt(maxPagesEl.value || "200", 10);
  const limit = parseInt(limitEl.value || "0", 10);

  if (mode === "deep" && !brand) {
    alert("الرجاء إدخال اسم الماركة في وضع Deep Scan");
    return;
  }
  if (mode === "category" && !category_url) {
    alert("الرجاء إدخال رابط التصنيف في وضع Category");
    return;
  }

  statusText.textContent = "جاري الاستيراد...";
  setProgress(10, "جاري الاتصال بالموقع", "");
  itemsTbody.innerHTML = "";

  try {
    const res = await fetch("/api/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        site_key,
        mode,
        brand,
        category_url,
        max_pages,
        limit
      })
    });

    const data = await res.json();
    if (!data.ok) {
      statusText.textContent = "فشل الاستيراد";
      setProgress(0, "فشل", "");
      alert(data.error || "خطأ غير معروف");
      return;
    }

    const items = data.items || [];
    setProgress(100, "اكتمل الاستيراد", items.length + " منتج");
    statusText.textContent = "اكتمل — " + items.length + " منتج";

    lastCountEl.textContent = items.length + " منتج";
    if (data.csv_path) {
      lastCsvEl.textContent = data.csv_path;
    }

    itemsTbody.innerHTML = "";
    for (const item of items) {
      const tr = document.createElement("tr");
      const tdTitle = document.createElement("td");
      const tdBrand = document.createElement("td");
      const tdPrice = document.createElement("td");
      const tdUrl = document.createElement("td");

      tdTitle.textContent = item.title || "";
      tdBrand.textContent = item.brand || "";
      tdPrice.textContent = item.price || "";

      const a = document.createElement("a");
      a.href = item.url || "#";
      a.textContent = "فتح";
      a.target = "_blank";
      a.style.color = "#60a5fa";
      tdUrl.appendChild(a);

      tr.appendChild(tdTitle);
      tr.appendChild(tdBrand);
      tr.appendChild(tdPrice);
      tr.appendChild(tdUrl);
      itemsTbody.appendChild(tr);
    }

  } catch (e) {
    console.error(e);
    statusText.textContent = "فشل الاستيراد";
    setProgress(0, "فشل", "");
    alert("حدث خطأ أثناء الاتصال بالخادم");
  }
});
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
