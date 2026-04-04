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
  li.innerHTML = `
    <div class="flex items-start justify-between gap-4">
      <div class="min-w-0">
        <div class="font-extrabold text-slate-900 dark:text-slate-50 truncate">
          ${item.title ? item.title : "تقرير بدون عنوان"}
        </div>
        <div class="text-sm text-slate-600 dark:text-slate-400 mt-1">
          ${item.branch1_name} مقابل ${item.branch2_name}
        </div>
      </div>
      <a class="px-3 py-1.5 rounded-lg bg-slate-900 dark:bg-white text-white dark:text-slate-900 text-sm font-extrabold shrink-0" href="/report?id=${item.id}">عرض</a>
    </div>
    <div class="flex gap-3 flex-wrap">
      <div class="text-sm text-slate-700 dark:text-slate-300"><span class="font-extrabold">متطابق:</span> ${item.stats.matched_ops}</div>
      <div class="text-sm text-slate-700 dark:text-slate-300"><span class="font-extrabold">أخطاء:</span> ${item.stats.errors_count}</div>
      <div class="text-sm text-slate-700 dark:text-slate-300"><span class="font-extrabold">تحذيرات:</span> ${item.stats.warnings_count}</div>
    </div>
    <button class="self-end px-3 py-1.5 rounded-lg border border-rose-200 dark:border-rose-800 text-rose-600 dark:text-rose-400 text-sm font-extrabold hover:bg-rose-50 dark:hover:bg-rose-950/50" onclick="deleteReport('${item.id}')">حذف</button>
  `;
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

async function loadReports() {
  const host = document.getElementById("reportsHost");
  if (!host) return;
  host.innerHTML = `
    <div class="text-slate-600 dark:text-slate-400 text-center py-10">جارٍ تحميل التقارير ...</div>
  `;
  const data = await apiGet("/reports");
  const items = data.items || [];
  host.innerHTML = "";
  if (!items.length) {
    host.innerHTML = `<div class="text-slate-600 dark:text-slate-400 text-center py-10">لا توجد تقارير بعد.</div>`;
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

  host.innerHTML = `
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
        ${rows || `<tr><td colspan="7" class="px-3 py-6 text-center text-slate-600 dark:text-slate-400">لا توجد بيانات</td></tr>`}
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
  renderMismatchTable(mismatches, document.getElementById("mismatchTableHost"));
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
          <div>
            <label class="block text-sm font-extrabold text-slate-700 dark:text-slate-300 mb-1">كلمة المرور</label>
            <input id="authPassword" type="password" class="w-full rounded-xl border border-slate-200 dark:border-slate-600 bg-white dark:bg-slate-950 px-3 py-2 outline-none text-slate-900 dark:text-slate-100 focus:ring-2 focus:ring-slate-900/10 dark:focus:ring-white/20" />
          </div>
          <label class="flex items-center gap-2 cursor-pointer select-none">
            <input id="authRememberUsername" type="checkbox" class="rounded border-slate-300 dark:border-slate-600" />
            <span class="text-sm font-extrabold text-slate-700 dark:text-slate-300">تذكير اسم المستخدم فقط (لا نحفظ كلمة المرور)</span>
          </label>
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
    const p = document.getElementById("authPassword");
    const rememberCb = document.getElementById("authRememberUsername");
    if (!title || !submit || !cancel || !close || !u || !p) return;

    title.innerText = mode === "register" ? "إنشاء حساب جديد" : "تسجيل الدخول";
    submit.innerText = mode === "register" ? "تسجيل" : "دخول";
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
      const password = (p.value || "").trim();
      if (!username || !password) {
        showToast("أدخل اسم المستخدم وكلمة المرور", "#ef4444");
        return;
      }
      try {
        if (mode === "register") {
          await apiPostJson("/auth/register", { username, password });
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
        <button type="button" id="registerBtn" class="px-3 py-1.5 rounded-xl bg-slate-900 dark:bg-white text-white dark:text-slate-900 text-sm font-extrabold hover:bg-slate-800 dark:hover:bg-slate-200">إنشاء حساب</button>
      </div>
    `;

    document.getElementById("registerBtn")?.addEventListener("click", () => {
      openAuthModal("register");
    });

    document.getElementById("loginBtn")?.addEventListener("click", () => {
      openAuthModal("login");
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

  document.querySelectorAll("[data-nav]").forEach((el) => {
    el.classList.add("nav-link");
    const k = el.getAttribute("data-nav");
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
}

document.addEventListener("DOMContentLoaded", initNavAndTheme);

// deleteReport is global for inline onclick usage
window.deleteReport = deleteReport;
window.initAuthUI = initAuthUI;
window.qs = qs;
window.downloadReportFile = downloadReportFile;
window.downloadCSV = downloadCSV;
window.downloadErrors = downloadReportFile;

