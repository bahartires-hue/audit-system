try {
  const t = localStorage.getItem("auditflow-theme");
  const dark =
    t === "dark" || (t !== "light" && window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.classList.toggle("dark", dark);
} catch (e) {}

function syncCsrfFromCookie() {
  const key = "auditflow_csrf=";
  const i = document.cookie.indexOf(key);
  if (i === -1) return;
  let v = document.cookie.slice(i + key.length).split(";")[0] || "";
  try {
    v = decodeURIComponent(v);
  } catch (e) {}
  if (v) try { localStorage.setItem("csrf_token", v); } catch (e) {}
}
syncCsrfFromCookie();

async function readErrorMessage(res) {
  const raw = await res.text().catch(() => "");
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("application/json") && raw) {
    try {
      const j = JSON.parse(raw);
      const d = j.detail;
      if (typeof d === "string") return d;
      if (Array.isArray(d))
        return d
          .map((x) => (x && typeof x === "object" && x.msg ? String(x.msg) : JSON.stringify(x)))
          .join("; ");
    } catch (e) {}
  }
  return raw || `HTTP ${res.status}`;
}

function qs(name) {
  return new URLSearchParams(window.location.search).get(name);
}

async function apiGet(url) {
  syncCsrfFromCookie();
  const res = await fetch(url, { headers: { Accept: "application/json" }, credentials: "include" });
  if (!res.ok) throw new Error(await readErrorMessage(res));
  const data = await res.json();
  if (data && typeof data === "object" && data.csrf_token) {
    try { localStorage.setItem("csrf_token", data.csrf_token); } catch (e) {}
  }
  return data;
}

async function apiPostForm(url, formData) {
  syncCsrfFromCookie();
  const csrf = localStorage.getItem("csrf_token") || "";
  const res = await fetch(url, {
    method: "POST",
    body: formData,
    headers: { Accept: "application/json", "X-CSRF-Token": csrf },
    credentials: "include",
  });
  if (!res.ok) throw new Error(await readErrorMessage(res));
  return res.json();
}

async function apiPostJson(url, body) {
  syncCsrfFromCookie();
  const csrf = localStorage.getItem("csrf_token") || "";
  const res = await fetch(url, {
    method: "POST",
    credentials: "include",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-CSRF-Token": csrf,
    },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) throw new Error(await readErrorMessage(res));
  return res.json();
}

async function apiDelete(url) {
  syncCsrfFromCookie();
  const csrf = localStorage.getItem("csrf_token") || "";
  const res = await fetch(url, {
    method: "DELETE",
    credentials: "include",
    headers: { Accept: "application/json", "X-CSRF-Token": csrf },
  });
  if (!res.ok) throw new Error(await readErrorMessage(res));
  return res.json();
}

async function apiPatchJson(url, body) {
  syncCsrfFromCookie();
  const csrf = localStorage.getItem("csrf_token") || "";
  const res = await fetch(url, {
    method: "PATCH",
    credentials: "include",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-CSRF-Token": csrf,
    },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) throw new Error(await readErrorMessage(res));
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
    if (btn.dataset.oldText === undefined) btn.dataset.oldText = (btn.textContent || "").trim();
    const msg = text || "جارٍ التحليل ...";
    btn.innerHTML = `<span class="inline-flex items-center justify-center gap-2"><svg class="h-4 w-4 animate-spin shrink-0" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" aria-hidden="true"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg><span>${msg}</span></span>`;
  } else {
    btn.disabled = false;
    btn.textContent = btn.dataset.oldText !== undefined ? btn.dataset.oldText : text || "ابدأ التحليل";
    delete btn.dataset.oldText;
  }
}

function renderReportRow(item) {
  const li = document.createElement("div");
  li.className =
    "bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 p-4 flex flex-col gap-2 shadow-sm";
  const tags = Array.isArray(item.tags) ? item.tags : [];
  const tagsHtml = tags.length
    ? `<div class="flex flex-wrap gap-1 mt-2">${tags.map((t) => `<span class="text-[0.65rem] font-extrabold px-2 py-0.5 rounded-full bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300">${String(t)}</span>`).join("")}</div>`
    : "";
  const arch = item.archived ? "إرجاع من الأرشيف" : "أرشفة";
  li.innerHTML = `
    <div class="flex items-start justify-between gap-4">
      <div class="min-w-0">
        <div class="font-extrabold text-slate-900 dark:text-slate-50 truncate">
          ${item.title ? item.title : "تقرير بدون عنوان"}
        </div>
        <div class="text-sm text-slate-600 dark:text-slate-400 mt-1">
          ${item.branch1_name} مقابل ${item.branch2_name}
        </div>
        ${tagsHtml}
      </div>
      <a class="px-3 py-1.5 rounded-lg bg-slate-900 dark:bg-white text-white dark:text-slate-900 text-sm font-extrabold shrink-0" href="/report?id=${item.id}">عرض</a>
    </div>
    <div class="flex gap-3 flex-wrap">
      <div class="text-sm text-slate-700 dark:text-slate-300"><span class="font-extrabold">متطابق:</span> ${item.stats.matched_ops}</div>
      <div class="text-sm text-slate-700 dark:text-slate-300"><span class="font-extrabold">أخطاء:</span> ${item.stats.errors_count}</div>
      <div class="text-sm text-slate-700 dark:text-slate-300"><span class="font-extrabold">تحذيرات:</span> ${item.stats.warnings_count}</div>
    </div>
    <div class="flex flex-wrap gap-2 justify-end">
      <button type="button" class="px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-600 text-slate-700 dark:text-slate-200 text-sm font-extrabold hover:bg-slate-50 dark:hover:bg-slate-800" data-archive-toggle="${item.id}" data-archived="${item.archived ? "1" : "0"}">${arch}</button>
      <button type="button" class="px-3 py-1.5 rounded-lg border border-rose-200 dark:border-rose-800 text-rose-600 dark:text-rose-400 text-sm font-extrabold hover:bg-rose-50 dark:hover:bg-rose-950/50" onclick="deleteReport('${item.id}')">حذف</button>
    </div>
  `;
  const arBtn = li.querySelector("[data-archive-toggle]");
  if (arBtn) {
    arBtn.addEventListener("click", () => toggleReportArchive(item.id, !item.archived));
  }
  return li;
}

async function deleteReport(id) {
  if (!confirm("هل تريد حذف هذا التقرير؟")) return;
  try {
    await apiDelete(`/reports?id=${encodeURIComponent(id)}`);
    showToast("تم الحذف ✔️", "#10b981");
    await loadReports();
  } catch (e) {
    showToast(e.message || "فشل حذف التقرير", "#ef4444");
  }
}

async function toggleReportArchive(id, archived) {
  try {
    await apiPatchJson(`/report?id=${encodeURIComponent(id)}`, { archived: !!archived });
    showToast(archived ? "تمت الأرشفة" : "تمت إعادة التقرير للقائمة النشطة", "#10b981");
    await loadReports();
  } catch (e) {
    showToast(e.message || "فشل التحديث", "#ef4444");
  }
}

async function loadReports() {
  const host = document.getElementById("reportsHost");
  if (!host) return;
  host.innerHTML = `
    <div class="text-slate-600 dark:text-slate-400 text-center py-10">جارٍ تحميل التقارير ...</div>
  `;
  const archivedSel = document.getElementById("reportsArchivedFilter");
  const qInp = document.getElementById("reportsSearchInp");
  const archived = archivedSel ? archivedSel.value : "0";
  const q = qInp ? (qInp.value || "").trim() : "";
  const data = await apiGet(`/reports?archived=${encodeURIComponent(archived)}&q=${encodeURIComponent(q)}`);
  const items = data.items || [];
  host.innerHTML = "";
  if (!items.length) {
    host.innerHTML = `<div class="text-slate-600 dark:text-slate-400 text-center py-10">لا توجد تقارير مطابقة.</div>`;
    return;
  }
  for (const item of items) {
    host.appendChild(renderReportRow(item));
  }
}

function initReportsFilters() {
  const archivedSel = document.getElementById("reportsArchivedFilter");
  const qInp = document.getElementById("reportsSearchInp");
  const btn = document.getElementById("reportsFilterBtn");
  const run = () => loadReports().catch((e) => showToast(e.message || "فشل التحميل", "#ef4444"));
  archivedSel?.addEventListener("change", run);
  btn?.addEventListener("click", run);
  qInp?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") run();
  });
}

const MISMATCH_PAGE_SIZE = 40;

function goMismatchPage(delta) {
  const host = document.getElementById("mismatchTableHost");
  const entries = window.__MISMATCHES_FILTERED__ || [];
  if (!host) return;
  let p = Number(window.__MISMATCH_PAGE__ || 1);
  p += delta;
  renderMismatchTable(entries, host, { page: p });
}

function exportFilteredCsv() {
  const rows = window.__MISMATCHES_FILTERED__ || [];
  if (!rows.length) {
    showToast("لا صفوف للتصدير", "#ef4444");
    return;
  }
  const esc = (v) => {
    const s = String(v ?? "");
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = [["الفرع", "المبلغ", "نوع العملية", "التاريخ", "المستند", "السبب"].map(esc).join(",")];
  for (const e of rows) {
    lines.push(
      [e.branch, e.amount, e.type, e.date, e.doc, e.reason].map(esc).join(",")
    );
  }
  const blob = new Blob(["\ufeff" + lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `filtered_${qs("id") || "report"}.csv`;
  document.body.appendChild(a);
  a.click();
  URL.revokeObjectURL(a.href);
  a.remove();
  showToast("تم تصدير الصفوف المعروضة ✔️", "#10b981");
}

function renderMismatchTable(entries, host, opts) {
  const pageSize = (opts && opts.pageSize) || MISMATCH_PAGE_SIZE;
  let page = (opts && opts.page) || window.__MISMATCH_PAGE__ || 1;
  const total = entries.length;
  const pages = Math.max(1, Math.ceil(total / pageSize) || 1);
  if (page < 1) page = 1;
  if (page > pages) page = pages;
  window.__MISMATCH_PAGE__ = page;
  const start = (page - 1) * pageSize;
  const slice = entries.slice(start, start + pageSize);

  const rows = slice
    .map((e) => {
      const reason = e.reason || "";
      const severity = e.type === "error" || reason.includes("❌") ? "error" : reason.includes("⚠️") ? "warning" : "mismatch";
      const sevColor =
        severity === "error"
          ? "bg-rose-50 text-rose-700 border-rose-200 dark:bg-rose-950/40 dark:text-rose-300 dark:border-rose-800"
          : severity === "warning"
            ? "bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-950/40 dark:text-amber-300 dark:border-amber-800"
            : "bg-slate-50 text-slate-700 border-slate-200 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600";
      const sevText = severity === "error" ? "خطأ" : severity === "warning" ? "تحذير" : "مخالفة";
      return `
        <tr class="border-b border-slate-200 dark:border-slate-700">
          <td class="px-3 py-3 text-sm text-slate-800 dark:text-slate-200">${e.branch || "-"}</td>
          <td class="px-3 py-3 text-sm text-slate-800 dark:text-slate-200">${e.amount ?? "-"}</td>
          <td class="px-3 py-3 text-sm text-slate-800 dark:text-slate-200">${e.type || "-"}</td>
          <td class="px-3 py-3 text-sm text-slate-800 dark:text-slate-200">${e.date || "-"}</td>
          <td class="px-3 py-3 text-sm text-slate-800 dark:text-slate-200">${e.doc || "-"}</td>
          <td class="px-3 py-3 text-sm">
            <span class="inline-flex items-center px-2 py-1 rounded-full border ${sevColor} text-xs font-extrabold">${sevText}</span>
          </td>
          <td class="px-3 py-3 text-sm text-slate-700 dark:text-slate-300">${reason || "-"}</td>
        </tr>
      `;
    })
    .join("");

  const from = total ? start + 1 : 0;
  const to = total ? start + slice.length : 0;
  const pager =
    total > pageSize
      ? `<div class="flex flex-wrap items-center justify-between gap-2 mt-3 text-sm font-extrabold text-slate-600 dark:text-slate-400">
          <span>عرض ${from}–${to} من ${total}</span>
          <div class="flex gap-2">
            <button type="button" class="px-3 py-1.5 rounded-xl border border-slate-200 dark:border-slate-600 hover:bg-slate-50 dark:hover:bg-slate-800 disabled:opacity-40" ${page <= 1 ? "disabled" : ""} onclick="goMismatchPage(-1)">السابق</button>
            <button type="button" class="px-3 py-1.5 rounded-xl border border-slate-200 dark:border-slate-600 hover:bg-slate-50 dark:hover:bg-slate-800 disabled:opacity-40" ${page >= pages ? "disabled" : ""} onclick="goMismatchPage(1)">التالي</button>
          </div>
        </div>`
      : total
        ? `<div class="mt-3 text-sm font-extrabold text-slate-500 dark:text-slate-400">إجمالي ${total} صفاً</div>`
        : "";

  host.innerHTML = `
    <div class="overflow-auto rounded-2xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-950 print:border-0">
    <table class="w-full text-right table-fixed">
      <thead class="bg-slate-50 dark:bg-slate-800/80">
        <tr>
          <th class="px-3 py-2 text-xs text-slate-600 dark:text-slate-300 font-extrabold w-[120px]">الفرع</th>
          <th class="px-3 py-2 text-xs text-slate-600 dark:text-slate-300 font-extrabold w-[110px]">المبلغ</th>
          <th class="px-3 py-2 text-xs text-slate-600 dark:text-slate-300 font-extrabold w-[90px]">نوع</th>
          <th class="px-3 py-2 text-xs text-slate-600 dark:text-slate-300 font-extrabold w-[110px]">التاريخ</th>
          <th class="px-3 py-2 text-xs text-slate-600 dark:text-slate-300 font-extrabold">المستند</th>
          <th class="px-3 py-2 text-xs text-slate-600 dark:text-slate-300 font-extrabold w-[110px]">الحالة</th>
          <th class="px-3 py-2 text-xs text-slate-600 dark:text-slate-300 font-extrabold">السبب</th>
        </tr>
      </thead>
      <tbody>
        ${rows || `<tr><td colspan="7" class="px-3 py-6 text-center text-emerald-700 dark:text-emerald-300 font-extrabold">لا توجد فروقات مالية، سعدنا بخدمتك</td></tr>`}
      </tbody>
    </table>
    </div>
    ${pager}
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
  window.__MISMATCHES_FILTERED__ = filtered;
  window.__MISMATCH_PAGE__ = 1;
  renderMismatchTable(filtered, host, { page: 1 });
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
  const counts = analysis.counts || {};
  const statsJson = data.stats_json || {};
  const b1Name = data.branch1_name || "";
  const b2Name = data.branch2_name || "";
  const b1Total = Number(statsJson.branch1_total || 0);
  const b2Total = Number(statsJson.branch2_total || 0);
  const b1Err = Number(counts[b1Name] || 0);
  const b2Err = Number(counts[b2Name] || 0);
  const b1Rate = b1Total > 0 ? ((b1Err / b1Total) * 100).toFixed(1) : "0.0";
  const b2Rate = b2Total > 0 ? ((b2Err / b2Total) * 100).toFixed(1) : "0.0";

  const b1ErrEl = document.getElementById("branch1Errors");
  const b2ErrEl = document.getElementById("branch2Errors");
  const b1RateEl = document.getElementById("branch1Rate");
  const b2RateEl = document.getElementById("branch2Rate");
  const b1Label = document.getElementById("branch1Label");
  const b2Label = document.getElementById("branch2Label");
  if (b1ErrEl) b1ErrEl.innerText = String(b1Err);
  if (b2ErrEl) b2ErrEl.innerText = String(b2Err);
  if (b1RateEl) b1RateEl.innerText = `${b1Rate}%`;
  if (b2RateEl) b2RateEl.innerText = `${b2Rate}%`;
  if (b1Label) b1Label.innerText = b1Name || "الفرع الأول";
  if (b2Label) b2Label.innerText = b2Name || "الفرع الثاني";

  window.__MISMATCHES__ = mismatches;
  window.__MISMATCHES_FILTERED__ = mismatches;
  window.__MISMATCH_PAGE__ = 1;
  renderMismatchTable(mismatches, document.getElementById("mismatchTableHost"), { page: 1 });

  const tagsInp = document.getElementById("reportTagsInp");
  const notesTa = document.getElementById("reportNotesTa");
  const saveMetaBtn = document.getElementById("saveReportMetaBtn");
  if (tagsInp) tagsInp.value = (data.tags || []).join(", ");
  if (notesTa) notesTa.value = data.notes || "";
  if (saveMetaBtn) {
    saveMetaBtn.onclick = async () => {
      const tagsRaw = (tagsInp?.value || "").trim();
      const tags = tagsRaw
        ? tagsRaw
            .split(/[,،]/)
            .map((x) => x.trim())
            .filter(Boolean)
        : [];
      try {
        await apiPatchJson(`/report?id=${encodeURIComponent(reportId)}`, {
          tags,
          notes: (notesTa?.value || "").trim(),
        });
        showToast("تم حفظ الملاحظات والوسوم ✔️", "#10b981");
      } catch (e) {
        showToast(e.message || "فشل الحفظ", "#ef4444");
      }
    };
  }
  const archBtn = document.getElementById("reportArchiveBtn");
  if (archBtn) {
    archBtn.textContent = data.archived ? "إرجاع من الأرشيف" : "أرشفة التقرير";
    archBtn.onclick = async () => {
      try {
        await apiPatchJson(`/report?id=${encodeURIComponent(reportId)}`, { archived: !data.archived });
        showToast(!data.archived ? "تمت الأرشفة" : "أُعيد التقرير للقائمة النشطة", "#10b981");
        window.location.reload();
      } catch (e) {
        showToast(e.message || "فشل التحديث", "#ef4444");
      }
    };
  }

  const aiBtn = document.getElementById("aiInsightsBtn");
  const aiHost = document.getElementById("aiInsightsHost");
  if (aiBtn && aiHost) {
    aiBtn.onclick = async () => {
      try {
        aiBtn.disabled = true;
        aiBtn.textContent = "جارٍ التحليل الشامل...";
        const out = await apiPostJson(`/ai/full-analysis?id=${encodeURIComponent(reportId)}`, {});
        const causes = (out.root_causes || []).map((x) => `<li>${x}</li>`).join("");
        const actions = (out.recommended_actions || []).map((x) => `<li>${x}</li>`).join("");
        const msgs = (out.followup_messages || []).map((x) => `<li>${x}</li>`).join("");
        const risk = Number(out.risk_score || 0);
        aiHost.innerHTML = `
          <div class="rounded-xl border border-indigo-200 dark:border-indigo-800 bg-white dark:bg-slate-900 p-3">
            <div class="flex items-center justify-between gap-2">
              <div class="font-extrabold text-slate-900 dark:text-slate-100 mb-2">ملخص تنفيذي</div>
              <span class="px-2 py-1 rounded-lg text-xs font-extrabold ${risk >= 70 ? "bg-rose-100 text-rose-700" : risk >= 40 ? "bg-amber-100 text-amber-700" : "bg-emerald-100 text-emerald-700"}">درجة المخاطر: ${risk}/100</span>
            </div>
            <p class="leading-7">${out.executive_summary || "لا يوجد ملخص"}</p>
            <div class="mt-3 font-extrabold text-slate-900 dark:text-slate-100">الأسباب الجذرية المحتملة</div>
            <ul class="list-disc list-inside mt-1 space-y-1">${causes || "<li>لا توجد بيانات كافية</li>"}</ul>
            <div class="mt-3 font-extrabold text-slate-900 dark:text-slate-100">خطة العمل المقترحة</div>
            <ul class="list-disc list-inside mt-1 space-y-1">${actions || "<li>لا توجد توصيات حالياً</li>"}</ul>
            <div class="mt-3 font-extrabold text-slate-900 dark:text-slate-100">رسائل متابعة جاهزة</div>
            <ul class="list-disc list-inside mt-1 space-y-1">${msgs || "<li>لا توجد رسائل متابعة حالياً</li>"}</ul>
          </div>
        `;
        showToast("تم توليد التحليل الذكي الشامل ✔️", "#10b981");
      } catch (e) {
        aiHost.innerHTML = `<p class="text-rose-600">${e.message || "تعذر تشغيل التحليل الذكي"}</p>`;
        showToast(e.message || "تعذر تشغيل التحليل الذكي", "#ef4444");
      } finally {
        aiBtn.disabled = false;
        aiBtn.textContent = "توليد التحليل الذكي الشامل";
      }
    };
  }
}

const AUDITFLOW_REMEMBER_USER_KEY = "auditflow_remember_username";
const AUDITFLOW_LAST_USERNAME_KEY = "auditflow_last_username";

function parseDownloadFilename(contentDisposition, fallback) {
  const cd = contentDisposition || "";
  const utf = cd.match(/filename\*=UTF-8''([^;\s]+)/i);
  if (utf) {
    try {
      return decodeURIComponent(utf[1].trim());
    } catch (e) {
      return utf[1].trim();
    }
  }
  const m = cd.match(/filename\s*=\s*"?([^";\n]+)"?/i);
  if (m) return m[1].trim();
  return fallback;
}

async function downloadReportFile(id, format) {
  if (!id) {
    showToast("معرّف التقرير غير موجود", "#ef4444");
    return;
  }
  const fmt = (format || "excel").toLowerCase().trim();
  const url = `/download?id=${encodeURIComponent(id)}&format=${encodeURIComponent(fmt)}`;
  syncCsrfFromCookie();
  try {
    const res = await fetch(url, { method: "GET", credentials: "include" });
    if (!res.ok) throw new Error(await readErrorMessage(res));
    const blob = await res.blob();
    const ext = fmt === "pdf" ? "pdf" : fmt === "csv" ? "csv" : "xlsx";
    const fallbackName = `report_${id}.${ext}`;
    const filename = parseDownloadFilename(res.headers.get("Content-Disposition"), fallbackName);
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    URL.revokeObjectURL(a.href);
    a.remove();
    showToast("تم التحميل ✔️", "#10b981");
  } catch (e) {
    showToast(e.message || "فشل التحميل", "#ef4444");
  }
}

function downloadCSV(id) {
  return downloadReportFile(id, "csv");
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
    const strictEl = document.getElementById("strictMirror");
    if (strictEl && strictEl.checked) fd.append("strict_mirror_types", "true");

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

async function initAuthUI() {
  const host = document.getElementById("authArea");
  if (!host) return;

  function ensureAuthModal() {
    let modal = document.getElementById("authModal");
    if (modal) return modal;
    modal = document.createElement("div");
    modal.id = "authModal";
    modal.className = "hidden fixed inset-0 z-[70] bg-black/50 items-center justify-center p-4";
    modal.innerHTML = `
      <div class="w-full max-w-md rounded-2xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 shadow-2xl p-5">
        <div class="flex items-center justify-between mb-4">
          <h3 id="authModalTitle" class="text-xl font-extrabold text-slate-900 dark:text-slate-50"></h3>
          <button type="button" id="authCloseBtn" class="px-2 py-1 rounded-lg border border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-800">✕</button>
        </div>
        <div class="space-y-3">
          <div>
            <label class="block text-sm font-extrabold text-slate-700 dark:text-slate-300 mb-1">اسم المستخدم</label>
            <input id="authUsername" class="w-full rounded-xl border border-slate-200 dark:border-slate-600 bg-white dark:bg-slate-950 px-3 py-2 outline-none text-slate-900 dark:text-slate-100 focus:ring-2 focus:ring-slate-900/10 dark:focus:ring-white/20" />
          </div>
          <div id="authEmailWrap" class="hidden">
            <label class="block text-sm font-extrabold text-slate-700 dark:text-slate-300 mb-1">البريد الإلكتروني</label>
            <input id="authEmail" type="email" class="w-full rounded-xl border border-slate-200 dark:border-slate-600 bg-white dark:bg-slate-950 px-3 py-2 outline-none text-slate-900 dark:text-slate-100 focus:ring-2 focus:ring-slate-900/10 dark:focus:ring-white/20" />
          </div>
          <div id="authInviteWrap" class="hidden">
            <label class="block text-sm font-extrabold text-slate-700 dark:text-slate-300 mb-1">كود الدعوة</label>
            <input id="authInviteCode" class="w-full rounded-xl border border-slate-200 dark:border-slate-600 bg-white dark:bg-slate-950 px-3 py-2 outline-none text-slate-900 dark:text-slate-100 focus:ring-2 focus:ring-slate-900/10 dark:focus:ring-white/20" />
          </div>
          <div id="authPlanWrap" class="hidden">
            <label class="block text-sm font-extrabold text-slate-700 dark:text-slate-300 mb-1">الخطة</label>
            <select id="authPlanSelect" class="w-full rounded-xl border border-slate-200 dark:border-slate-600 bg-white dark:bg-slate-950 px-3 py-2 outline-none text-slate-900 dark:text-slate-100 focus:ring-2 focus:ring-slate-900/10 dark:focus:ring-white/20">
              <option value="free">مجاني</option>
              <option value="month">شهر</option>
              <option value="3months">3 شهور</option>
              <option value="6months">6 شهور</option>
              <option value="year">سنة</option>
              <option value="5years">5 سنوات</option>
            </select>
          </div>
          <div>
            <label class="block text-sm font-extrabold text-slate-700 dark:text-slate-300 mb-1">كلمة المرور</label>
            <input id="authPassword" type="password" class="w-full rounded-xl border border-slate-200 dark:border-slate-600 bg-white dark:bg-slate-950 px-3 py-2 outline-none text-slate-900 dark:text-slate-100 focus:ring-2 focus:ring-slate-900/10 dark:focus:ring-white/20" />
          </div>
          <label class="flex items-center gap-2 cursor-pointer select-none">
            <input id="authRememberUsername" type="checkbox" class="rounded border-slate-300 dark:border-slate-600" />
            <span class="text-sm font-extrabold text-slate-700 dark:text-slate-300">تذكير اسم المستخدم فقط (لا نحفظ كلمة المرور)</span>
          </label>
          <div id="authLegalWrap" class="hidden rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/40 p-3">
            <label class="flex items-start gap-2 cursor-pointer select-none">
              <input id="authAcceptLegal" type="checkbox" class="mt-1 rounded border-slate-300 dark:border-slate-600" />
              <span class="text-xs leading-6 font-bold text-slate-700 dark:text-slate-300">
                أوافق على
                <a href="/terms" target="_blank" class="text-emerald-700 dark:text-emerald-300 underline">شروط الاستخدام</a>
                و
                <a href="/privacy" target="_blank" class="text-emerald-700 dark:text-emerald-300 underline">سياسة الخصوصية</a>
                و
                <a href="/user-agreement" target="_blank" class="text-emerald-700 dark:text-emerald-300 underline">اتفاقية المستخدم</a>.
              </span>
            </label>
          </div>
        </div>
        <div class="mt-4 flex items-center justify-end gap-2">
          <button type="button" id="authCancelBtn" class="px-4 py-2 rounded-xl border border-slate-200 dark:border-slate-600 text-sm font-extrabold text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-800">إلغاء</button>
          <button type="button" id="authSubmitBtn" class="px-4 py-2 rounded-xl bg-slate-900 dark:bg-white text-white dark:text-slate-900 text-sm font-extrabold hover:bg-slate-800 dark:hover:bg-slate-200"></button>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    return modal;
  }

  function openAuthModal(mode) {
    const modal = ensureAuthModal();
    const title = document.getElementById("authModalTitle");
    const submit = document.getElementById("authSubmitBtn");
    const cancel = document.getElementById("authCancelBtn");
    const close = document.getElementById("authCloseBtn");
    const u = document.getElementById("authUsername");
    const eInp = document.getElementById("authEmail");
    const eWrap = document.getElementById("authEmailWrap");
    const invInp = document.getElementById("authInviteCode");
    const invWrap = document.getElementById("authInviteWrap");
    const planSel = document.getElementById("authPlanSelect");
    const planWrap = document.getElementById("authPlanWrap");
    const legalWrap = document.getElementById("authLegalWrap");
    const legalCb = document.getElementById("authAcceptLegal");
    const p = document.getElementById("authPassword");
    const rememberCb = document.getElementById("authRememberUsername");
    if (!title || !submit || !cancel || !close || !u || !p || !eInp || !eWrap || !invInp || !invWrap || !planSel || !planWrap || !legalWrap || !legalCb) return;

    title.innerText = mode === "register" ? "إنشاء حساب جديد" : "تسجيل الدخول";
    submit.innerText = mode === "register" ? "تسجيل" : "دخول";
    eWrap.classList.toggle("hidden", mode !== "register");
    invWrap.classList.toggle("hidden", mode !== "register");
    planWrap.classList.toggle("hidden", mode !== "register");
    legalWrap.classList.toggle("hidden", mode !== "register");
    let savedUser = "";
    try {
      savedUser = localStorage.getItem(AUDITFLOW_LAST_USERNAME_KEY) || "";
    } catch (e) {}
    let remember = false;
    try {
      remember = localStorage.getItem(AUDITFLOW_REMEMBER_USER_KEY) === "1";
    } catch (e) {}
    if (rememberCb) rememberCb.checked = remember;
    u.value = remember && savedUser ? savedUser : "";
    eInp.value = "";
    invInp.value = "";
    planSel.value = "free";
    legalCb.checked = false;
    p.value = "";
    modal.classList.remove("hidden");
    modal.classList.add("flex");
    u.focus();

    const closeModal = () => {
      modal.classList.add("hidden");
      modal.classList.remove("flex");
    };

    close.onclick = closeModal;
    cancel.onclick = closeModal;
    modal.onclick = (e) => {
      if (e.target === modal) closeModal();
    };
    submit.onclick = async () => {
      const username = (u.value || "").trim();
      const email = (eInp.value || "").trim();
      const invite_code = (invInp.value || "").trim();
      const plan = String(planSel.value || "free");
      const password = (p.value || "").trim();
      if (!username || !password) {
        showToast("أدخل اسم المستخدم وكلمة المرور", "#ef4444");
        return;
      }
      try {
        if (mode === "register") {
          if (!email) {
            showToast("أدخل البريد الإلكتروني", "#ef4444");
            return;
          }
          if (!legalCb.checked) {
            showToast("يجب الموافقة على الشروط والسياسات لإكمال التسجيل", "#ef4444");
            return;
          }
          await apiPostJson("/auth/register", {
            username,
            email,
            invite_code,
            plan,
            password,
            accepted_terms: true,
            accepted_privacy: true,
            accepted_agreement: true,
          });
          showToast("تم إنشاء الحساب وتسجيل الدخول ✔️");
        } else {
          await apiPostJson("/auth/login", { username, password });
          showToast("تم تسجيل الدخول ✔️");
        }
        try {
          if (rememberCb && rememberCb.checked) {
            localStorage.setItem(AUDITFLOW_REMEMBER_USER_KEY, "1");
            localStorage.setItem(AUDITFLOW_LAST_USERNAME_KEY, username);
          } else {
            localStorage.removeItem(AUDITFLOW_REMEMBER_USER_KEY);
            localStorage.removeItem(AUDITFLOW_LAST_USERNAME_KEY);
          }
        } catch (e) {}
        closeModal();
        window.location.reload();
      } catch (e) {
        showToast(e.message || "فشل العملية", "#ef4444");
      }
    };
  }

  async function render() {
    try {
      const me = await apiGet("/auth/me");
      if (me?.csrf_token) localStorage.setItem("csrf_token", me.csrf_token);
      const username = me?.username || "";
      if (username) {
        host.innerHTML = `
          <div class="flex items-center gap-2 flex-wrap justify-center">
            <span class="text-sm font-extrabold text-slate-700 dark:text-slate-300">مرحباً ${username}</span>
            <button type="button" id="logoutBtn" class="px-3 py-1.5 rounded-xl border border-slate-200 dark:border-slate-600 text-sm font-extrabold text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-800">خروج</button>
          </div>
        `;
        document.getElementById("logoutBtn")?.addEventListener("click", async () => {
          await apiPostJson("/auth/logout", {});
          showToast("تم تسجيل الخروج");
          window.location.reload();
        });
        return;
      }
    } catch (_) {
      // غير مسجّل
    }

    host.innerHTML = `
      <div class="flex items-center gap-2 flex-wrap justify-center">
        <button type="button" id="loginBtn" class="px-3 py-1.5 rounded-xl border border-slate-200 dark:border-slate-600 text-sm font-extrabold text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-800">تسجيل دخول</button>
        <button type="button" id="registerBtn" class="px-3 py-1.5 rounded-xl bg-slate-900 dark:bg-white text-white dark:text-slate-900 text-sm font-extrabold hover:bg-slate-800 dark:hover:bg-slate-200">إنشاء حساب جديد</button>
        <button type="button" id="forgotBtn" class="px-3 py-1.5 rounded-xl border border-amber-300 dark:border-amber-700 text-sm font-extrabold text-amber-700 dark:text-amber-300 hover:bg-amber-50 dark:hover:bg-amber-950/40">نسيت كلمة المرور</button>
      </div>
    `;

    document.getElementById("registerBtn")?.addEventListener("click", () => {
      openAuthModal("register");
    });

    document.getElementById("loginBtn")?.addEventListener("click", () => {
      openAuthModal("login");
    });
    document.getElementById("forgotBtn")?.addEventListener("click", async () => {
      const email = prompt("أدخل بريدك الإلكتروني لاستعادة كلمة المرور:");
      if (!email) return;
      try {
        const data = await apiPostJson("/auth/request-password-reset", { email: String(email).trim() });
        if (data?.reset_link) {
          prompt("هذا رابط استعادة كلمة المرور (انسخه وافتحه):", data.reset_link);
          showToast("تم إنشاء رابط الاستعادة ✔️", "#10b981");
        } else {
          showToast("تم إرسال رابط الاستعادة إلى بريدك ✔️", "#10b981");
        }
      } catch (e) {
        showToast(e.message || "تعذر إرسال رابط الاستعادة", "#ef4444");
      }
    });
  }

  await render();
}

function updateThemeToggleUi() {
  const btn = document.getElementById("themeToggle");
  if (!btn) return;
  const isDark = document.documentElement.classList.contains("dark");
  btn.textContent = isDark ? "☀️ نهار" : "🌙 ليل";
  btn.title = isDark ? "التبديل إلى الوضع النهاري" : "التبديل إلى الوضع الليلي";
  btn.setAttribute("aria-label", isDark ? "التبديل إلى الوضع النهاري" : "التبديل إلى الوضع الليلي");
}

function initNavAndTheme() {
  const path = window.location.pathname || "";
  let key = "home";
  if (path.startsWith("/analyze")) key = "analyze";
  else if (path.startsWith("/reports")) key = "reports";
  else if (path.startsWith("/report")) key = "reports";
  else if (path.startsWith("/settings")) key = "settings";
  else if (path.startsWith("/help")) key = "help";
  else if (path.startsWith("/about")) key = "about";
  else if (path.startsWith("/contact")) key = "contact";
  else if (path.startsWith("/social")) key = "social";
  else if (path.startsWith("/terms")) key = "terms";
  else if (path.startsWith("/privacy")) key = "privacy";
  else if (path.startsWith("/user-agreement")) key = "agreement";

  const sidebar = document.querySelector(".erp-sidebar__nav");
  const mobileDrawer = document.getElementById("mobileNavDrawer");
  const legalLinks = [
    { href: "/terms", nav: "terms", title: "شروط الاستخدام", text: "شروط الاستخدام" },
    { href: "/privacy", nav: "privacy", title: "سياسة الخصوصية", text: "سياسة الخصوصية" },
    { href: "/user-agreement", nav: "agreement", title: "اتفاقية المستخدم", text: "اتفاقية المستخدم" },
  ];
  if (sidebar) {
    legalLinks.forEach((x) => {
      if (!sidebar.querySelector(`a[data-nav="${x.nav}"]`)) {
        const a = document.createElement("a");
        a.href = x.href;
        a.className = "erp-side-link nav-link";
        a.setAttribute("data-nav", x.nav);
        a.setAttribute("title", x.title);
        sidebar.appendChild(a);
      }
    });
  }
  if (mobileDrawer) {
    legalLinks.forEach((x) => {
      if (!mobileDrawer.querySelector(`a[data-nav="${x.nav}"]`)) {
        const a = document.createElement("a");
        a.href = x.href;
        a.setAttribute("data-nav", x.nav);
        a.textContent = x.text;
        mobileDrawer.appendChild(a);
      }
    });
  }

  document.querySelectorAll("[data-nav]").forEach((el) => {
    el.classList.add("nav-link");
    const k = el.getAttribute("data-nav");
    if (k === "home") {
      if (el.getAttribute("title")) el.setAttribute("title", "الرئيسية");
      const txt = (el.textContent || "").trim();
      if (txt === "لوحة التحكم") el.textContent = "الرئيسية";
    } else if (k === "settings") {
      if (el.getAttribute("title")) el.setAttribute("title", "لوحة التحكم");
      const txt = (el.textContent || "").trim();
      if (txt === "الإعدادات") el.textContent = "لوحة التحكم";
    }
    const active = k === key;
    el.classList.toggle("nav-link--active", active);
    el.classList.toggle("nav-link--idle", !active);
  });

  const themeBtn = document.getElementById("themeToggle");
  if (themeBtn) {
    themeBtn.addEventListener("click", () => {
      const nextDark = !document.documentElement.classList.contains("dark");
      document.documentElement.classList.toggle("dark", nextDark);
      try {
        localStorage.setItem("auditflow-theme", nextDark ? "dark" : "light");
      } catch (e) {}
      updateThemeToggleUi();
    });
  }
  updateThemeToggleUi();

  const navMenuBtn = document.getElementById("navMenuBtn");
  const closeMobileNav = () => {
    if (!mobileDrawer || !navMenuBtn) return;
    mobileDrawer.classList.remove("is-open");
    navMenuBtn.setAttribute("aria-expanded", "false");
  };
  if (navMenuBtn && mobileDrawer) {
    navMenuBtn.addEventListener("click", () => {
      const open = mobileDrawer.classList.toggle("is-open");
      navMenuBtn.setAttribute("aria-expanded", open ? "true" : "false");
    });
    mobileDrawer.querySelectorAll("a[data-nav]").forEach((a) => {
      a.addEventListener("click", () => closeMobileNav());
    });
    window.addEventListener(
      "resize",
      () => {
        if (window.matchMedia("(min-width: 768px)").matches) closeMobileNav();
      },
      { passive: true }
    );
  }
  const topTitle = document.querySelector(".erp-topbar__title");
  if (topTitle) {
    if ((topTitle.textContent || "").trim() === "لوحة التحكم") topTitle.textContent = "الرئيسية";
    else if ((topTitle.textContent || "").trim() === "الإعدادات") topTitle.textContent = "لوحة التحكم";
  }

  // Normalize brand spelling across pages.
  document.querySelectorAll(".app-brand__text").forEach((el) => {
    el.textContent = "التطابق الأمثل";
  });
  document.querySelectorAll(".app-brand__en").forEach((el) => {
    el.textContent = "OptimalMatch";
  });
}

document.addEventListener("DOMContentLoaded", initNavAndTheme);

// deleteReport is global for inline onclick usage
window.deleteReport = deleteReport;
window.toggleReportArchive = toggleReportArchive;
window.initAuthUI = initAuthUI;
window.initReportsFilters = initReportsFilters;
window.qs = qs;
window.downloadReportFile = downloadReportFile;
window.downloadCSV = downloadCSV;
window.downloadErrors = downloadReportFile;
window.goMismatchPage = goMismatchPage;
window.exportFilteredCsv = exportFilteredCsv;

