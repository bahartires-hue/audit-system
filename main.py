from __future__ import annotations

"""
AuditFlow (single-file version)
--------------------------------
Backend + Frontend in ONE Python file.

Run:
  pip install fastapi uvicorn[standard] sqlalchemy pandas pdfplumber openpyxl python-multipart
  uvicorn auditflow_single:app --host 127.0.0.1 --port 8001

Open:
  http://127.0.0.1:8001/
"""

import csv
import datetime as dt
import io
import os
import re
import uuid
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pdfplumber
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker


# =========================
# CONFIG / PATHS
# =========================
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("AUDITFLOW_DB", str(BASE_DIR / "auditflow.db")))
UPLOAD_DIR = BASE_DIR / "uploads"


# =========================
# DB
# =========================
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class AnalysisReport(Base):
    __tablename__ = "analysis_reports"

    id = Column(String, primary_key=True)  # uuid4 hex
    title = Column(String, nullable=True)

    branch1_name = Column(String, nullable=False)
    branch2_name = Column(String, nullable=False)

    file1_original = Column(String, nullable=True)
    file2_original = Column(String, nullable=True)
    file1_path = Column(String, nullable=True)
    file2_path = Column(String, nullable=True)

    status = Column(String, default="completed", nullable=False)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)

    total_ops = Column(Integer, nullable=False, default=0)
    matched_ops = Column(Integer, nullable=False, default=0)
    mismatch_ops = Column(Integer, nullable=False, default=0)
    errors_count = Column(Integer, nullable=False, default=0)
    warnings_count = Column(Integer, nullable=False, default=0)

    stats_json = Column(JSON, nullable=False, default=dict)
    analysis_json = Column(JSON, nullable=False, default=dict)


Base.metadata.create_all(bind=engine)


def db_session() -> Session:
    return SessionLocal()


# =========================
# UTILS (storage)
# =========================
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_upload_file(upload: UploadFile, dest_dir: Path) -> Tuple[str, str]:
    ensure_dir(dest_dir)
    original = upload.filename or "upload"
    suffix = Path(original).suffix
    saved_name = f"{uuid.uuid4().hex}{suffix}"
    saved_path = dest_dir / saved_name

    content = upload.file.read()
    with open(saved_path, "wb") as f:
        f.write(content)

    return str(saved_path), original


# =========================
# ANALYZER (local)
# =========================
def safe(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        v = str(v).replace(",", "").strip()
        if v == "":
            return None
        return round(float(v), 2)
    except Exception:
        return None


def detect_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    df.columns = df.columns.astype(str).str.strip()

    debit_col = None
    credit_col = None
    date_col = None

    for col in df.columns:
        name = str(col).lower()
        if any(x in name for x in ["مدين", "debit", "dr"]):
            debit_col = col
        if any(x in name for x in ["دائن", "credit", "cr"]):
            credit_col = col
        if any(x in name for x in ["تاريخ", "التاريخ", "التأريخ", "date"]):
            date_col = col

    numeric_cols: List[Tuple[str, float]] = []
    for col in df.columns:
        nums = pd.to_numeric(df[col], errors="coerce")
        valid = nums.dropna()
        if len(valid) < len(df) * 0.3:
            continue
        mean_val = valid.mean()
        if mean_val < 10:
            continue
        numeric_cols.append((col, float(mean_val)))

    numeric_cols.sort(key=lambda x: x[1], reverse=True)
    if not debit_col and len(numeric_cols) >= 1:
        debit_col = numeric_cols[0][0]
    if not credit_col and len(numeric_cols) >= 2:
        credit_col = numeric_cols[1][0]

    if not date_col:
        for col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().sum() > len(df) * 0.5:
                date_col = col
                break

    return debit_col, credit_col, date_col


def read_excel(file_path: str) -> Optional[pd.DataFrame]:
    df = pd.read_excel(file_path)
    if df is None or df.empty:
        return None
    return df.dropna(how="all")


def read_pdf(file_path: str) -> Optional[pd.DataFrame]:
    rows: List[List[Any]] = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue
            for table in tables:
                for row in table:
                    if row and any(cell is not None for cell in row):
                        rows.append(row)

    if not rows:
        return None

    df = pd.DataFrame(rows).dropna(how="all")
    if len(df) < 2:
        return None
    df.columns = df.iloc[0]
    df = df[1:].dropna(how="all")
    return df


def read_any(file_path: str, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        out = read_excel(file_path)
        if out is None:
            raise ValueError("Excel file has no readable data")
        return out
    if name.endswith(".pdf"):
        out = read_pdf(file_path)
        if out is None:
            raise ValueError("PDF file has no readable data")
        return out
    raise ValueError("نوع الملف غير مدعوم")


doc_map: Dict[str, str] = {
    "مردود مبيعات": "مردود مشتريات",
    "مردود مشتريات": "مردود مبيعات",
    "سند قبض": "سند صرف",
    "سند صرف": "سند قبض",
    "تحويل مخزني": "تحويل مخزني",
    "توريد مخزني": "صرف مخزني",
    "صرف مخزني": "توريد مخزني",
    "قيد يومية": "قيد يومية",
    "قيد افتتاحي": "قيد افتتاحي",
    "مبيعات": "مشتريات",
    "مشتريات": "مبيعات",
}


def clean(s: Any) -> str:
    if not s:
        return ""
    s = str(s).lower().strip()
    for w in ["رقم", "no", "doc", "ref"]:
        s = s.replace(w, "")
    s = re.sub(r"\d+", "", s)
    for ch in [" ", "-", "_", "/", "\\", ".", ","]:
        s = s.replace(ch, "")
    return s


def match_doc(d1: Any, d2: Any) -> bool:
    if not d1 and not d2:
        return True
    if not d1 or not d2:
        return False

    d1 = clean(d1)
    d2 = clean(d2)

    if not d1 and not d2:
        return True
    if not d1 or not d2:
        return False
    if d1 == d2:
        return True
    if len(d1) > 3 and len(d2) > 3:
        if d1 in d2 or d2 in d1:
            return True

    for key, val in doc_map.items():
        k = clean(key)
        v = clean(val)
        if (k in d1 and v in d2) or (v in d1 and k in d2):
            return True

    return SequenceMatcher(None, d1, d2).ratio() > 0.7


def date_diff_days(d1: Any, d2: Any) -> Optional[int]:
    try:
        dd1 = pd.to_datetime(d1, errors="coerce")
        dd2 = pd.to_datetime(d2, errors="coerce")
        if pd.isna(dd1) or pd.isna(dd2):
            return None
        return abs((dd1 - dd2).days)
    except Exception:
        return None


def process(file_path: str, filename: str, branch: str) -> List[Dict[str, Any]]:
    df = read_any(file_path, filename)
    if df is None or len(df) == 0:
        return []

    df.columns = df.columns.astype(str).str.strip()
    debit_col, credit_col, date_col = detect_columns(df)

    doc_col = None
    for col in df.columns:
        name = str(col).lower().strip()
        if any(
            x in name
            for x in ["مستند", "المستند", "نوع", "بيان", "وصف", "description", "desc", "document"]
        ):
            doc_col = col
            break

    if not debit_col and not credit_col:
        numeric_cols: List[Tuple[str, float]] = []
        for col in df.columns:
            nums = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(nums) < len(df) * 0.3:
                continue
            if nums.mean() < 10:
                continue
            numeric_cols.append((col, float(nums.mean())))
        numeric_cols.sort(key=lambda x: x[1], reverse=True)
        if len(numeric_cols) >= 1:
            debit_col = numeric_cols[0][0]
        if len(numeric_cols) >= 2:
            credit_col = numeric_cols[1][0]

    data: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        if row.isna().all():
            continue

        debit = safe(row[debit_col]) if debit_col and debit_col in df.columns else None
        credit = safe(row[credit_col]) if credit_col and credit_col in df.columns else None

        if debit is None and credit is None:
            continue

        if debit and credit and debit > 0 and credit > 0:
            amount = max(debit, credit)
            data.append(
                {
                    "amount": float(amount),
                    "type": "error",
                    "branch": branch,
                    "date": None,
                    "doc": "",
                    "reason": "خطأ: الصف يحتوي مدين ودائن",
                }
            )
            continue

        if credit and credit > 0:
            amount = credit
            t = "credit"
        elif debit and debit > 0:
            amount = debit
            t = "debit"
        else:
            continue

        date_out: Optional[str] = None
        if date_col and date_col in df.columns:
            try:
                val = row[date_col]
                if pd.isna(val):
                    date_out = None
                else:
                    d = pd.to_datetime(val, errors="coerce", dayfirst=False)
                    date_out = None if pd.isna(d) else d.strftime("%Y-%m-%d")
            except Exception:
                date_out = str(row[date_col])

        doc_out: Optional[str] = None
        if doc_col and doc_col in df.columns:
            val = row[doc_col]
            if pd.notna(val):
                doc_out = str(val).strip()

        data.append({"amount": float(amount), "type": t, "branch": branch, "date": date_out, "doc": doc_out})

    return data


def analyze(d1: List[Dict[str, Any]], d2: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    res: List[Dict[str, Any]] = []
    used = [False] * len(d2)
    counts: Dict[str, int] = {}

    def remove_reversals(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned: List[Dict[str, Any]] = []
        used_local = [False] * len(data)
        for i, x1 in enumerate(data):
            if used_local[i]:
                continue
            found = False
            for j, x2 in enumerate(data):
                if i == j or used_local[j]:
                    continue
                if x1["branch"] != x2["branch"]:
                    continue
                if x1["type"] == x2["type"]:
                    continue
                if abs(x1["amount"] - x2["amount"]) > 0.01:
                    continue
                days = date_diff_days(x1["date"], x2["date"])
                if days is None or days > 1:
                    continue
                if x1.get("doc") and x2.get("doc"):
                    if not match_doc(x1["doc"], x2["doc"]):
                        continue
                used_local[i] = True
                used_local[j] = True
                found = True
                break
            if not found:
                cleaned.append(x1)
        return cleaned

    d1 = remove_reversals(d1)
    d2 = remove_reversals(d2)

    if not d2:
        for x in d1:
            res.append({**x, "reason": "لا يوجد مقابل ❌ (الفرع الثاني فارغ)"})
            b = x.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
        return res, counts

    def match_score(x1: Dict[str, Any], x2: Dict[str, Any]) -> Tuple[int, List[str]]:
        score = 0
        reasons: List[str] = []

        diff = abs(x1["amount"] - x2["amount"])
        if diff < 0.01:
            score += 50
            reasons.append("نفس المبلغ")
        elif diff < 1:
            score += 30
            reasons.append("مبلغ قريب")
        else:
            return 0, ["فرق مبلغ كبير"]

        if (x1["type"] == "credit" and x2["type"] == "debit") or (x1["type"] == "debit" and x2["type"] == "credit"):
            score += 30
            reasons.append("اتجاه عكسي صحيح")
        else:
            return 0, ["نفس الاتجاه"]

        if x1.get("doc") and x2.get("doc"):
            if not match_doc(x1["doc"], x2["doc"]):
                return 0, ["اختلاف نوع المستند"]

        days = date_diff_days(x1["date"], x2["date"])
        if days is None:
            score -= 10
            reasons.append("تاريخ غير واضح")
        elif days == 0:
            score += 20
            reasons.append("نفس اليوم")
        elif days <= 2:
            score += 10
            reasons.append("تاريخ قريب")
        else:
            score -= 10
            reasons.append("تاريخ بعيد")

        if match_doc(x1.get("doc"), x2.get("doc")):
            score += 20
            reasons.append("نوع مستند مطابق")
        else:
            score -= 10
            reasons.append("اختلاف نوع المستند")

        return score, reasons

    for x1 in d1:
        if x1.get("type") == "error":
            res.append(x1)
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
            continue

        best_i = -1
        best_score = -1
        best_reason: List[str] = []

        for i, x2 in enumerate(d2):
            if used[i]:
                continue
            if x2.get("type") == "error":
                continue
            score, reasons = match_score(x1, x2)
            if score > best_score:
                best_score = score
                best_i = i
                best_reason = reasons

        if best_score >= 80 and best_i != -1:
            used[best_i] = True
        elif best_score >= 60 and best_i != -1:
            res.append({**x1, "reason": f"تطابق ضعيف ⚠️ | score={best_score} | {' , '.join(best_reason)}"})
            used[best_i] = True
        else:
            res.append({**x1, "reason": f"لا يوجد مقابل ❌ | score={best_score} | {' , '.join(best_reason)}"})
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1

    for i, x in enumerate(d2):
        if not used[i]:
            if x.get("type") == "error":
                res.append(x)
                b = x.get("branch") or "unknown"
                counts[b] = counts.get(b, 0) + 1
                continue
            res.append({**x, "reason": "لا يوجد مقابل ❌ (من الفرع الآخر)"})
            b = x.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1

    return res, counts


def compute_summary(d1: List[Dict[str, Any]], d2: List[Dict[str, Any]], mismatches: List[Dict[str, Any]]) -> Dict[str, int]:
    total_ops = len(d1) + len(d2)
    mismatch_ops = len(mismatches)
    matched_ops = max(0, total_ops - mismatch_ops)
    errors = 0
    warnings = 0
    for e in mismatches:
        reason = e.get("reason") or ""
        if e.get("type") == "error" or "❌" in reason or "لا يوجد مقابل" in reason:
            errors += 1
        elif "⚠️" in reason:
            warnings += 1
    return {
        "total_ops": total_ops,
        "matched_ops": matched_ops,
        "mismatch_ops": mismatch_ops,
        "errors_count": errors,
        "warnings_count": warnings,
    }


def mismatches_to_csv_bytes(entries: List[Dict[str, Any]]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["الفرع", "المبلغ", "نوع العملية", "التاريخ", "المستند", "السبب"])
    for e in entries:
        writer.writerow(
            [
                e.get("branch", ""),
                e.get("amount", ""),
                e.get("type", ""),
                e.get("date", "") or "",
                e.get("doc", "") or "",
                e.get("reason", "") or "",
            ]
        )
    return output.getvalue().encode("utf-8-sig")


# =========================
# FRONTEND (HTML + JS)
# =========================
APP_JS = r"""function qs(name) {
  return new URLSearchParams(window.location.search).get(name);
}

async function apiGet(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(text || `HTTP ${res.status}`);
  }
  return res.json();
}

async function apiPostForm(url, formData) {
  const res = await fetch(url, { method: "POST", body: formData, headers: { Accept: "application/json" } });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(text || `HTTP ${res.status}`);
  }
  return res.json();
}

function showToast(msg, color = "#10b981") {
  const t = document.getElementById("toast");
  if (!t) return;
  t.innerText = msg;
  t.style.background = color;
  t.classList.remove("hidden");
  setTimeout(() => t.classList.add("hidden"), 3000);
}

function setLoading(btn, loading, text) {
  if (!btn) return;
  if (loading) {
    btn.disabled = true;
    btn.dataset.oldText = btn.innerText;
    btn.innerHTML = text || "جارٍ التحليل ...";
  } else {
    btn.disabled = false;
    btn.innerText = btn.dataset.oldText || (text || "ابدأ التحليل");
  }
}

function renderReportRow(item) {
  const li = document.createElement("div");
  li.className = "bg-white rounded-xl border border-slate-200 p-4 flex flex-col gap-2";
  li.innerHTML = `
    <div class="flex items-start justify-between gap-4">
      <div class="min-w-0">
        <div class="font-extrabold text-slate-900 truncate">
          ${item.title ? item.title : "تقرير بدون عنوان"}
        </div>
        <div class="text-sm text-slate-600 mt-1">
          ${item.branch1_name} مقابل ${item.branch2_name}
        </div>
      </div>
      <a class="px-3 py-1.5 rounded-lg bg-slate-900 text-white text-sm font-extrabold" href="/report?id=${item.id}">عرض</a>
    </div>
    <div class="flex gap-3 flex-wrap">
      <div class="text-sm text-slate-700"><span class="font-extrabold">متطابق:</span> ${item.stats.matched_ops}</div>
      <div class="text-sm text-slate-700"><span class="font-extrabold">أخطاء:</span> ${item.stats.errors_count}</div>
      <div class="text-sm text-slate-700"><span class="font-extrabold">تحذيرات:</span> ${item.stats.warnings_count}</div>
    </div>
    <button class="self-end px-3 py-1.5 rounded-lg border border-rose-200 text-rose-600 text-sm font-extrabold hover:bg-rose-50" onclick="deleteReport('${item.id}')">حذف</button>
  `;
  return li;
}

async function deleteReport(id) {
  if (!confirm("هل تريد حذف هذا التقرير؟")) return;
  const res = await fetch(`/reports?id=${encodeURIComponent(id)}`, { method: "DELETE", headers: { Accept: "application/json" } });
  if (!res.ok) {
    showToast("فشل حذف التقرير", "#ef4444");
    return;
  }
  showToast("تم الحذف ✔️", "#10b981");
  await loadReports();
}

async function loadReports() {
  const host = document.getElementById("reportsHost");
  if (!host) return;
  host.innerHTML = `
    <div class="text-slate-600 text-center py-10">جارٍ تحميل التقارير ...</div>
  `;
  const data = await apiGet("/reports");
  const items = data.items || [];
  host.innerHTML = "";
  if (!items.length) {
    host.innerHTML = `<div class="text-slate-600 text-center py-10">لا توجد تقارير بعد.</div>`;
    return;
  }
  for (const item of items) {
    host.appendChild(renderReportRow(item));
  }
}

function renderMismatchTable(entries, host) {
  const rows = entries
    .map((e) => {
      const reason = e.reason || "";
      const severity = e.type === "error" || reason.includes("❌") ? "error" : reason.includes("⚠️") ? "warning" : "mismatch";
      const sevColor = severity === "error" ? "bg-rose-50 text-rose-700 border-rose-200" : severity === "warning" ? "bg-amber-50 text-amber-700 border-amber-200" : "bg-slate-50 text-slate-700 border-slate-200";
      const sevText = severity === "error" ? "خطأ" : severity === "warning" ? "تحذير" : "مخالفة";
      return `
        <tr class="border-b border-slate-200">
          <td class="px-3 py-3 text-sm text-slate-800">${e.branch || "-"}</td>
          <td class="px-3 py-3 text-sm text-slate-800">${e.amount ?? "-"}</td>
          <td class="px-3 py-3 text-sm text-slate-800">${e.type || "-"}</td>
          <td class="px-3 py-3 text-sm text-slate-800">${e.date || "-"}</td>
          <td class="px-3 py-3 text-sm text-slate-800">${e.doc || "-"}</td>
          <td class="px-3 py-3 text-sm">
            <span class="inline-flex items-center px-2 py-1 rounded-full border ${sevColor} text-xs font-extrabold">${sevText}</span>
          </td>
          <td class="px-3 py-3 text-sm text-slate-700">${reason || "-"}</td>
        </tr>
      `;
    })
    .join("");

  host.innerHTML = `
    <table class="w-full text-right table-fixed">
      <thead class="bg-slate-50">
        <tr>
          <th class="px-3 py-2 text-xs text-slate-600 font-extrabold w-[120px]">الفرع</th>
          <th class="px-3 py-2 text-xs text-slate-600 font-extrabold w-[110px]">المبلغ</th>
          <th class="px-3 py-2 text-xs text-slate-600 font-extrabold w-[90px]">نوع</th>
          <th class="px-3 py-2 text-xs text-slate-600 font-extrabold w-[110px]">التاريخ</th>
          <th class="px-3 py-2 text-xs text-slate-600 font-extrabold">المستند</th>
          <th class="px-3 py-2 text-xs text-slate-600 font-extrabold w-[110px]">الحالة</th>
          <th class="px-3 py-2 text-xs text-slate-600 font-extrabold">السبب</th>
        </tr>
      </thead>
      <tbody>
        ${rows || `<tr><td colspan="7" class="px-3 py-6 text-center text-slate-600">لا توجد بيانات</td></tr>`}
      </tbody>
    </table>
  `;
}

function applyTableFilters(entries) {
  const host = document.getElementById("mismatchTableHost");
  if (!host) return;

  const fDoc = (document.getElementById("filterDoc")?.value || "").toLowerCase().trim();
  const fAmount = (document.getElementById("filterAmount")?.value || "").trim();
  const fType = (document.getElementById("filterType")?.value || "").trim();

  let filtered = entries;
  if (fDoc) filtered = filtered.filter((x) => (x.doc || "").toLowerCase().includes(fDoc));
  if (fAmount) filtered = filtered.filter((x) => String(x.amount ?? "") === fAmount);
  if (fType) {
    filtered = filtered.filter((x) => (x.reason || "").includes(fType));
  }
  renderMismatchTable(filtered, host);
}

async function loadReportDetail() {
  const reportId = qs("id");
  if (!reportId) {
    showToast("معرّف التقرير غير موجود", "#ef4444");
    return;
  }
  const data = await apiGet(`/report?id=${encodeURIComponent(reportId)}`);

  document.getElementById("reportTitle").innerText = data.title || "تقرير بدون عنوان";
  document.getElementById("reportBranches").innerText = `${data.branch1_name} مقابل ${data.branch2_name}`;

  const stats = data.stats;
  document.getElementById("statTotal").innerText = String(stats.total_ops);
  document.getElementById("statMatched").innerText = String(stats.matched_ops);
  document.getElementById("statErrors").innerText = String(stats.errors_count);
  document.getElementById("statWarnings").innerText = String(stats.warnings_count);

  const analysis = data.analysis_json || {};
  const mismatches = analysis.mismatches || [];

  window.__MISMATCHES__ = mismatches;
  renderMismatchTable(mismatches, document.getElementById("mismatchTableHost"));
}

function downloadCSV(id) {
  window.location.href = `/download?id=${encodeURIComponent(id)}`;
}

async function startAnalyze() {
  const btn = document.getElementById("startBtn");
  setLoading(btn, true, "جارٍ التحليل ...");
  try {
    const file1 = document.getElementById("file1").files?.[0] || null;
    const file2 = document.getElementById("file2").files?.[0] || null;
    const b1 = document.getElementById("b1").value || "الفرع الأول";
    const b2 = document.getElementById("b2").value || "الفرع الثاني";
    const title = document.getElementById("title").value || null;

    if (!file1 || !file2) {
      showToast("اختَر الملفين أولاً", "#ef4444");
      return;
    }

    const fd = new FormData();
    fd.append("file1", file1);
    fd.append("file2", file2);
    fd.append("b1", b1);
    fd.append("b2", b2);
    if (title) fd.append("title", title);

    const data = await apiPostForm("/analyze", fd);
    const id = data.reportId;
    showToast("تم التحليل ✔️", "#10b981");
    window.location.href = `/report?id=${encodeURIComponent(id)}`;
  } catch (e) {
    showToast(e.message || "فشل التحليل", "#ef4444");
  } finally {
    setLoading(btn, false, "ابدأ التحليل");
  }
}

function initAnalyzePage() {
  document.getElementById("startBtn")?.addEventListener("click", () => startAnalyze());
}

window.deleteReport = deleteReport;
"""

INDEX_HTML = r"""<!doctype html>
<html lang="ar" dir="rtl">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>AuditFlow</title>
    <link
      href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;600;700;800&display=swap"
      rel="stylesheet"
    />
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
      body { font-family: "IBM Plex Sans Arabic", system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif; }
    </style>
  </head>
  <body class="bg-slate-50 text-slate-900">
    <div id="toast" class="hidden fixed bottom-5 left-5 z-50 text-white px-4 py-2 rounded-xl font-extrabold"></div>

    <header class="sticky top-0 bg-white/90 backdrop-blur border-b border-slate-200 z-40">
      <div class="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between gap-3">
        <div class="font-extrabold text-slate-900 text-lg">AuditFlow</div>
        <nav class="flex gap-3">
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-slate-900 text-white" href="/">لوحة التحكم</a>
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/analyze">تحليل</a>
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/reports">التقارير</a>
        </nav>
      </div>
    </header>

    <main class="max-w-6xl mx-auto px-4 py-8">
      <h1 class="text-3xl font-extrabold text-center">نظام المطابقة المالية</h1>
      <p class="text-center text-slate-600 mt-2">ارفع ملفي Excel / PDF وقارن العمليات تلقائياً.</p>

      <div class="grid md:grid-cols-3 gap-4 mt-8">
        <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
          <div class="text-slate-500 font-extrabold text-sm">آخر التقارير</div>
          <div id="dashTotalReports" class="text-3xl font-extrabold mt-2">0</div>
        </div>
        <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
          <div class="text-slate-500 font-extrabold text-sm">إجمالي الأخطاء (آخر تقرير)</div>
          <div id="dashErrors" class="text-3xl font-extrabold mt-2">0</div>
        </div>
        <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
          <div class="text-slate-500 font-extrabold text-sm">إجمالي التحذيرات (آخر تقرير)</div>
          <div id="dashWarnings" class="text-3xl font-extrabold mt-2">0</div>
        </div>
      </div>

      <section class="mt-8">
        <div class="flex items-center justify-between gap-3">
          <h2 class="text-xl font-extrabold">آخر التقارير</h2>
          <a class="text-sm font-extrabold text-slate-900 hover:underline" href="/reports">عرض الكل</a>
        </div>
        <div id="dashReportsHost" class="mt-4 grid gap-3 md:grid-cols-2"></div>
      </section>
    </main>

    <script src="/static/app.js"></script>
    <script>
      (async function () {
        try {
          const data = await apiGet("/reports");
          const items = data.items || [];
          document.getElementById("dashTotalReports").innerText = String(items.length);
          if (items.length) {
            document.getElementById("dashErrors").innerText = String(items[0].stats.errors_count);
            document.getElementById("dashWarnings").innerText = String(items[0].stats.warnings_count);
          }

          const host = document.getElementById("dashReportsHost");
          host.innerHTML = "";
          (items.slice(0, 6) || []).forEach((item) => {
            const card = document.createElement("div");
            card.className = "bg-white border border-slate-200 rounded-2xl p-5 shadow-sm flex flex-col gap-3";
            card.innerHTML = `
              <div class="flex items-start justify-between gap-4">
                <div class="min-w-0">
                  <div class="font-extrabold truncate">${item.title ? item.title : "تقرير بدون عنوان"}</div>
                  <div class="text-sm text-slate-600 mt-1">${item.branch1_name} مقابل ${item.branch2_name}</div>
                </div>
                <a class="px-3 py-1.5 rounded-lg bg-slate-900 text-white text-sm font-extrabold" href="/report?id=${item.id}">عرض</a>
              </div>
              <div class="flex gap-3 flex-wrap text-sm">
                <div class="font-extrabold text-slate-800">أخطاء: <span class="text-rose-600">${item.stats.errors_count}</span></div>
                <div class="font-extrabold text-slate-800">تحذيرات: <span class="text-amber-600">${item.stats.warnings_count}</span></div>
              </div>
            `;
            host.appendChild(card);
          });
        } catch (e) {
          console.error(e);
        }
      })();
    </script>
  </body>
</html>
"""

ANALYZE_HTML = r"""<!doctype html>
<html lang="ar" dir="rtl">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>تحليل - AuditFlow</title>
    <link
      href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;600;700;800&display=swap"
      rel="stylesheet"
    />
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
      body { font-family: "IBM Plex Sans Arabic", system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif; }
    </style>
  </head>
  <body class="bg-slate-50 text-slate-900">
    <div id="toast" class="hidden fixed bottom-5 left-5 z-50 text-white px-4 py-2 rounded-xl font-extrabold"></div>

    <header class="sticky top-0 bg-white/90 backdrop-blur border-b border-slate-200 z-40">
      <div class="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between gap-3">
        <div class="font-extrabold text-slate-900 text-lg">AuditFlow</div>
        <nav class="flex gap-3">
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/">لوحة التحكم</a>
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-slate-900 text-white" href="/analyze">تحليل</a>
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/reports">التقارير</a>
        </nav>
      </div>
    </header>

    <main class="max-w-6xl mx-auto px-4 py-8">
      <div class="bg-white border border-slate-200 rounded-3xl p-6 md:p-8 shadow-sm">
        <h1 class="text-2xl md:text-3xl font-extrabold text-center">تحليل المطابقة المالية</h1>
        <p class="text-center text-slate-600 mt-2">ارفع ملفي الفرع الأول والثاني (Excel/PDF) ثم شغّل التحليل.</p>

        <div class="grid lg:grid-cols-2 gap-4 mt-6">
          <section class="bg-slate-50 border border-slate-200 rounded-2xl p-4">
            <div class="font-extrabold text-slate-900 mb-3">الفرع الأول</div>

            <label class="block text-sm font-extrabold text-slate-700 mb-1">اسم الفرع</label>
            <input id="b1" class="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-slate-900/10" value="الفرع الأول" />

            <div class="flex gap-2 justify-center mt-3 mb-3">
              <button type="button" id="b1_excel" class="px-3 py-1.5 rounded-full border border-slate-200 text-slate-600 font-extrabold text-xs active:bg-emerald-50 active:border-emerald-200 active:text-emerald-700" onclick="setType(1,'excel')">Excel</button>
              <button type="button" id="b1_pdf" class="px-3 py-1.5 rounded-full border border-slate-200 text-slate-600 font-extrabold text-xs" onclick="setType(1,'pdf')">PDF</button>
            </div>

            <input type="file" id="file1" class="hidden" />
            <div id="dz1" class="dropzone border-2 border-dashed border-slate-300 rounded-2xl bg-white h-28 flex items-center justify-center flex-col gap-1 cursor-pointer hover:border-slate-400" draggable="false">
              <div class="text-sm font-extrabold text-slate-900">اسحب وأفلت</div>
              <div class="text-xs text-slate-500">أو اضغط للاختيار</div>
            </div>
            <div id="fileName1" class="text-xs text-slate-500 mt-2 min-h-4"></div>
          </section>

          <section class="bg-slate-50 border border-slate-200 rounded-2xl p-4">
            <div class="font-extrabold text-slate-900 mb-3">الفرع الثاني</div>

            <label class="block text-sm font-extrabold text-slate-700 mb-1">اسم الفرع</label>
            <input id="b2" class="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 outline-none focus:ring-2 focus:ring-slate-900/10" value="الفرع الثاني" />

            <div class="flex gap-2 justify-center mt-3 mb-3">
              <button type="button" id="b2_excel" class="px-3 py-1.5 rounded-full border border-slate-200 text-slate-600 font-extrabold text-xs" onclick="setType(2,'excel')">Excel</button>
              <button type="button" id="b2_pdf" class="px-3 py-1.5 rounded-full border border-slate-200 text-slate-600 font-extrabold text-xs" onclick="setType(2,'pdf')">PDF</button>
            </div>

            <input type="file" id="file2" class="hidden" />
            <div id="dz2" class="dropzone border-2 border-dashed border-slate-300 rounded-2xl bg-white h-28 flex items-center justify-center flex-col gap-1 cursor-pointer hover:border-slate-400" draggable="false">
              <div class="text-sm font-extrabold text-slate-900">اسحب وأفلت</div>
              <div class="text-xs text-slate-500">أو اضغط للاختيار</div>
            </div>
            <div id="fileName2" class="text-xs text-slate-500 mt-2 min-h-4"></div>
          </section>
        </div>

        <div class="mt-6 flex items-center justify-center gap-3 flex-wrap">
          <button id="startBtn" class="px-6 py-3 bg-slate-900 text-white rounded-2xl font-extrabold hover:bg-slate-800" onclick="startAnalyze()">ابدأ التحليل</button>
          <input id="title" class="w-72 max-w-full rounded-xl border border-slate-200 bg-white px-3 py-2 outline-none" placeholder="عنوان التقرير (اختياري)" />
        </div>
      </div>
    </main>

    <script src="/static/app.js"></script>
    <script>
      let type1 = "excel";
      let type2 = "pdf";

      function setType(branch, type) {
        if (branch === 1) type1 = type;
        if (branch === 2) type2 = type;

        const b1e = document.getElementById("b1_excel");
        const b1p = document.getElementById("b1_pdf");
        const b2e = document.getElementById("b2_excel");
        const b2p = document.getElementById("b2_pdf");

        if (b1e) b1e.className = "px-3 py-1.5 rounded-full border border-slate-200 font-extrabold text-xs" + (type1 === "excel" ? " bg-emerald-50 border-emerald-200 text-emerald-700" : " text-slate-600");
        if (b1p) b1p.className = "px-3 py-1.5 rounded-full border border-slate-200 font-extrabold text-xs" + (type1 === "pdf" ? " bg-blue-50 border-blue-200 text-blue-700" : " text-slate-600");
        if (b2e) b2e.className = "px-3 py-1.5 rounded-full border border-slate-200 font-extrabold text-xs" + (type2 === "excel" ? " bg-emerald-50 border-emerald-200 text-emerald-700" : " text-slate-600");
        if (b2p) b2p.className = "px-3 py-1.5 rounded-full border border-slate-200 font-extrabold text-xs" + (type2 === "pdf" ? " bg-blue-50 border-blue-200 text-blue-700" : " text-slate-600");
      }

      function validate(file, expected) {
        if (!file) return false;
        const n = (file.name || "").toLowerCase();
        if (expected === "excel") return /\.(xlsx|xls|xlsm|xlsb|csv)$/.test(n);
        if (expected === "pdf") return n.endsWith(".pdf");
        return true;
      }

      function bindDZ(dzId, inputId, expectedGetter, fileNameId) {
        const dz = document.getElementById(dzId);
        const inp = document.getElementById(inputId);
        const fileName = document.getElementById(fileNameId);
        if (!dz || !inp) return;

        dz.addEventListener("click", () => inp.click());
        dz.addEventListener("dragover", (e) => { e.preventDefault(); dz.classList.add("border-slate-500"); });
        dz.addEventListener("dragleave", () => dz.classList.remove("border-slate-500"));
        dz.addEventListener("drop", (e) => {
          e.preventDefault();
          dz.classList.remove("border-slate-500");
          const file = e.dataTransfer?.files?.[0];
          if (!file || !validate(file, expectedGetter())) {
            showToast("نوع الملف غير صحيح", "#ef4444");
            return;
          }
          inp.files = e.dataTransfer.files;
          if (fileName) fileName.innerText = "تم اختيار: " + file.name;
        });

        inp.addEventListener("change", () => {
          const file = inp.files?.[0] || null;
          if (file && !validate(file, expectedGetter())) {
            showToast("نوع الملف غير صحيح", "#ef4444");
            inp.value = "";
            if (fileName) fileName.innerText = "";
            return;
          }
          if (fileName) fileName.innerText = file ? ("تم اختيار: " + file.name) : "";
        });
      }

      bindDZ("dz1", "file1", () => type1, "fileName1");
      bindDZ("dz2", "file2", () => type2, "fileName2");

      setType(1, "excel");
      setType(2, "pdf");
    </script>
  </body>
</html>
"""

REPORTS_HTML = r"""<!doctype html>
<html lang="ar" dir="rtl">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>التقارير - AuditFlow</title>
    <link
      href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;600;700;800&display=swap"
      rel="stylesheet"
    />
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
      body { font-family: "IBM Plex Sans Arabic", system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif; }
    </style>
  </head>
  <body class="bg-slate-50 text-slate-900">
    <div id="toast" class="hidden fixed bottom-5 left-5 z-50 text-white px-4 py-2 rounded-xl font-extrabold"></div>

    <header class="sticky top-0 bg-white/90 backdrop-blur border-b border-slate-200 z-40">
      <div class="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between gap-3">
        <div class="font-extrabold text-slate-900 text-lg">AuditFlow</div>
        <nav class="flex gap-3">
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/">لوحة التحكم</a>
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/analyze">تحليل</a>
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-slate-900 text-white" href="/reports">التقارير</a>
        </nav>
      </div>
    </header>

    <main class="max-w-6xl mx-auto px-4 py-8">
      <div class="bg-white border border-slate-200 rounded-3xl p-6 md:p-8 shadow-sm">
        <div class="flex items-center justify-between gap-3">
          <h1 class="text-2xl font-extrabold">التقارير</h1>
          <a href="/analyze" class="px-4 py-2 rounded-2xl bg-slate-900 text-white font-extrabold hover:bg-slate-800">تحليل جديد</a>
        </div>

        <div id="reportsHost" class="mt-6 grid gap-4 md:grid-cols-2"></div>
      </div>
    </main>

    <script src="/static/app.js"></script>
    <script>
      loadReports().catch((e) => {
        console.error(e);
        showToast("فشل تحميل التقارير", "#ef4444");
      });
    </script>
  </body>
</html>
"""

REPORT_HTML = r"""<!doctype html>
<html lang="ar" dir="rtl">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>تقرير - AuditFlow</title>
    <link
      href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;600;700;800&display=swap"
      rel="stylesheet"
    />
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
      body { font-family: "IBM Plex Sans Arabic", system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif; }
    </style>
  </head>
  <body class="bg-slate-50 text-slate-900">
    <div id="toast" class="hidden fixed bottom-5 left-5 z-50 text-white px-4 py-2 rounded-xl font-extrabold"></div>

    <header class="sticky top-0 bg-white/90 backdrop-blur border-b border-slate-200 z-40">
      <div class="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between gap-3">
        <div class="font-extrabold text-slate-900 text-lg">AuditFlow</div>
        <nav class="flex gap-3">
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/">لوحة التحكم</a>
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/analyze">تحليل</a>
          <a class="px-3 py-2 rounded-xl font-extrabold text-sm bg-white border border-slate-200 hover:bg-slate-50" href="/reports">التقارير</a>
        </nav>
      </div>
    </header>

    <main class="max-w-6xl mx-auto px-4 py-8">
      <div class="bg-white border border-slate-200 rounded-3xl p-6 md:p-8 shadow-sm">
        <div class="flex items-start justify-between gap-4 flex-wrap">
          <div>
            <h1 id="reportTitle" class="text-2xl font-extrabold">تقرير</h1>
            <div id="reportBranches" class="text-slate-600 font-extrabold mt-2"></div>
          </div>
          <div class="flex gap-2">
            <button
              id="downloadBtn"
              class="px-4 py-2 rounded-2xl bg-emerald-500 text-white font-extrabold hover:bg-emerald-600"
              onclick="downloadCSV(qs('id'))"
            >
              تحميل CSV
            </button>
          </div>
        </div>

        <div class="grid md:grid-cols-4 gap-4 mt-6">
          <div class="bg-slate-50 border border-slate-200 rounded-2xl p-4">
            <div class="text-slate-500 font-extrabold text-sm">الإجمالي</div>
            <div id="statTotal" class="text-3xl font-extrabold mt-2">0</div>
          </div>
          <div class="bg-emerald-50 border border-emerald-200 rounded-2xl p-4">
            <div class="text-emerald-700 font-extrabold text-sm">متطابق</div>
            <div id="statMatched" class="text-3xl font-extrabold mt-2 text-emerald-800">0</div>
          </div>
          <div class="bg-rose-50 border border-rose-200 rounded-2xl p-4">
            <div class="text-rose-700 font-extrabold text-sm">أخطاء</div>
            <div id="statErrors" class="text-3xl font-extrabold mt-2 text-rose-800">0</div>
          </div>
          <div class="bg-amber-50 border border-amber-200 rounded-2xl p-4">
            <div class="text-amber-700 font-extrabold text-sm">تحذيرات</div>
            <div id="statWarnings" class="text-3xl font-extrabold mt-2 text-amber-800">0</div>
          </div>
        </div>

        <div class="mt-6 bg-slate-50 border border-slate-200 rounded-2xl p-4">
          <div class="flex items-center justify-between gap-3 flex-wrap">
            <h2 class="font-extrabold">فلترة الأخطاء</h2>
            <div class="flex gap-2 flex-wrap">
              <input id="filterDoc" placeholder="نوع المستند" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none" />
              <input id="filterAmount" placeholder="المبلغ" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none w-32" />
              <input id="filterType" placeholder="نوع الخطأ (❌ أو ⚠️)" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none w-44" />
              <button class="px-4 py-2 rounded-xl bg-slate-900 text-white font-extrabold text-sm" onclick="applyTableFilters(window.__MISMATCHES__ || [])">تطبيق</button>
            </div>
          </div>
        </div>

        <div id="mismatchTableHost" class="mt-4 overflow-auto rounded-2xl border border-slate-200"></div>
      </div>
    </main>

    <script src="/static/app.js"></script>
    <script>
      window.addEventListener("DOMContentLoaded", () => {
        loadReportDetail().catch((e) => {
          console.error(e);
          showToast("فشل تحميل التقرير", "#ef4444");
        });
      });
    </script>
  </body>
</html>
"""


def wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "").lower()
    if "application/json" in accept:
        return False
    return ("text/html" in accept) or (accept == "" or "*/*" in accept)


# =========================
# APP
# =========================
app = FastAPI(title="AuditFlow (single file)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
def ui_home():
    return HTMLResponse(INDEX_HTML)


@app.get("/analyze", response_class=HTMLResponse)
def ui_analyze():
    return HTMLResponse(ANALYZE_HTML)


@app.get("/static/app.js")
def ui_js():
    return Response(content=APP_JS.encode("utf-8"), media_type="application/javascript; charset=utf-8")


@app.post("/analyze")
def analyze_api(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    b1: str = Form(...),
    b2: str = Form(...),
    title: Optional[str] = Form(None),
):
    report_id = uuid.uuid4().hex
    saved1, original1 = save_upload_file(file1, UPLOAD_DIR / report_id / "file1")
    saved2, original2 = save_upload_file(file2, UPLOAD_DIR / report_id / "file2")

    try:
        d1 = process(saved1, original1, b1)
        d2 = process(saved2, original2, b2)
        mismatches, counts = analyze(d1, d2)
    except Exception as e:
        raise HTTPException(400, f"Failed to analyze files: {e}")

    summary = compute_summary(d1, d2, mismatches)

    created = AnalysisReport(
        id=report_id,
        title=title,
        branch1_name=b1,
        branch2_name=b2,
        file1_original=original1,
        file2_original=original2,
        file1_path=saved1,
        file2_path=saved2,
        total_ops=summary["total_ops"],
        matched_ops=summary["matched_ops"],
        mismatch_ops=summary["mismatch_ops"],
        errors_count=summary["errors_count"],
        warnings_count=summary["warnings_count"],
        stats_json={
            "counts": counts,
            "branch1_total": len(d1),
            "branch2_total": len(d2),
        },
        analysis_json={
            "extracted_branch1": d1,
            "extracted_branch2": d2,
            "mismatches": mismatches,
            "counts": counts,
        },
    )

    db = db_session()
    try:
        db.add(created)
        db.commit()
    finally:
        db.close()

    return {"reportId": report_id}


@app.get("/reports")
def reports(request: Request):
    if wants_html(request):
        return HTMLResponse(REPORTS_HTML)

    db = db_session()
    try:
        rows: List[AnalysisReport] = (
            db.query(AnalysisReport).order_by(AnalysisReport.created_at.desc()).limit(200).all()
        )
        items = []
        for r in rows:
            items.append(
                {
                    "id": r.id,
                    "title": r.title,
                    "branch1_name": r.branch1_name,
                    "branch2_name": r.branch2_name,
                    "status": r.status,
                    "created_at": r.created_at,
                    "stats": {
                        "total_ops": r.total_ops,
                        "matched_ops": r.matched_ops,
                        "mismatch_ops": r.mismatch_ops,
                        "errors_count": r.errors_count,
                        "warnings_count": r.warnings_count,
                    },
                }
            )
        return {"items": items}
    finally:
        db.close()


@app.get("/report")
def report(request: Request, id: str = Query(...)):
    if wants_html(request):
        return HTMLResponse(REPORT_HTML)

    db = db_session()
    try:
        r: AnalysisReport | None = db.query(AnalysisReport).filter(AnalysisReport.id == id).first()
        if not r:
            raise HTTPException(404, "Report not found")

        return {
            "id": r.id,
            "title": r.title,
            "branch1_name": r.branch1_name,
            "branch2_name": r.branch2_name,
            "status": r.status,
            "created_at": r.created_at,
            "stats": {
                "total_ops": r.total_ops,
                "matched_ops": r.matched_ops,
                "mismatch_ops": r.mismatch_ops,
                "errors_count": r.errors_count,
                "warnings_count": r.warnings_count,
            },
            "file1_original": r.file1_original,
            "file2_original": r.file2_original,
            "stats_json": r.stats_json,
            "analysis_json": r.analysis_json,
        }
    finally:
        db.close()


@app.get("/download")
def download(id: str = Query(...)):
    db = db_session()
    try:
        r: AnalysisReport | None = db.query(AnalysisReport).filter(AnalysisReport.id == id).first()
        if not r:
            raise HTTPException(404, "Report not found")
        mismatches = (r.analysis_json or {}).get("mismatches", []) or []
        csv_bytes = mismatches_to_csv_bytes(mismatches)
        filename = f"report_{r.id}.csv"
        return Response(
            content=csv_bytes,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
        )
    finally:
        db.close()


@app.delete("/reports")
def delete_report(id: str = Query(...)):
    db = db_session()
    try:
        r: AnalysisReport | None = db.query(AnalysisReport).filter(AnalysisReport.id == id).first()
        if not r:
            raise HTTPException(404, "Report not found")
        db.delete(r)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()

