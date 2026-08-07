// ============================================================
// admin_layout.js — لوحة تحكم المدير الجديدة (Sidebar احترافي بنمط
// Stripe/Linear/Notion) — ملف واحد مشترك تستخدمه كل صفحات /admin/*
// بدل تكرار شريط التنقل داخل كل صفحة.
//
// عقد الاستخدام (Contract):
//   <body data-admin-active="dashboard" data-admin-title="لوحة التحكم">
//     <div id="adminLayoutHost"></div>
//     <main id="adminContent" class="admin2-content">...</main>
//   بعد تحميل app.js ثم هذا الملف، ثم سكربت الصفحة الخاص.
// ============================================================
(function () {
  const NAV_GROUPS = [
    {
      label: "الرئيسية",
      items: [{ key: "dashboard", label: "لوحة التحكم", href: "/admin", icon: "grid" }],
    },
    {
      label: "الإدارة",
      items: [
        { key: "users", label: "المستخدمون", href: "/admin/manage-users", icon: "users" },
        { key: "subscriptions", label: "الاشتراكات", href: "/admin/subscriptions", icon: "card" },
        { key: "invites", label: "الدعوات", href: "/admin/invites", icon: "ticket" },
        { key: "documents", label: "المستندات", href: "/admin/documents", icon: "folder" },
      ],
    },
    {
      label: "التقارير",
      items: [
        { key: "reports", label: "التقارير", href: "/reports", icon: "chart" },
        { key: "activity", label: "سجل النشاط", href: "/admin/activity", icon: "list" },
      ],
    },
    {
      label: "الإعدادات",
      items: [
        { key: "settings", label: "إعدادات النظام", href: "/admin/settings", icon: "gear" },
        { key: "bank_accounts", label: "الحسابات البنكية", href: "/admin/manage-bank-accounts", icon: "card" },
        { key: "smtp", label: "إعدادات البريد", href: "/admin/smtp", icon: "mail" },
        { key: "backups", label: "النسخ الاحتياطية", href: "/admin/backups", icon: "save" },
        { key: "security", label: "الأمان", href: "/admin/security", icon: "lock" },
      ],
    },
    {
      label: "الحساب",
      items: [{ key: "profile", label: "الملف الشخصي", href: "/admin/profile", icon: "user" }],
    },
  ];

  const ICONS = {
    grid: '<path stroke-linecap="round" stroke-linejoin="round" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/>',
    users: '<path stroke-linecap="round" stroke-linejoin="round" d="M17 20h5v-2a4 4 0 00-3-3.87M9 20H4v-2a4 4 0 013-3.87m6-1.13a4 4 0 10-4-4 4 4 0 004 4zm6 0a4 4 0 10-4-4"/>',
    card: '<rect x="2" y="5" width="20" height="14" rx="2" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M2 10h20"/>',
    ticket: '<path stroke-linecap="round" stroke-linejoin="round" d="M4 7a2 2 0 012-2h12a2 2 0 012 2v2a2 2 0 100 4v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2a2 2 0 100-4V7z"/>',
    folder: '<path stroke-linecap="round" stroke-linejoin="round" d="M3 7a2 2 0 012-2h4l2 2h8a2 2 0 012 2v8a2 2 0 01-2 2H5a2 2 0 01-2-2V7z"/>',
    chart: '<path stroke-linecap="round" stroke-linejoin="round" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z"/><path stroke-linecap="round" stroke-linejoin="round" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z"/>',
    list: '<path stroke-linecap="round" stroke-linejoin="round" d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/>',
    gear: '<path stroke-linecap="round" stroke-linejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/><path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>',
    mail: '<path stroke-linecap="round" stroke-linejoin="round" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/>',
    save: '<path stroke-linecap="round" stroke-linejoin="round" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8l4 4v6a2 2 0 01-2 2h-2m-6 0v-4a1 1 0 011-1h4a1 1 0 011 1v4m-6 0h6"/>',
    lock: '<rect x="4" y="11" width="16" height="10" rx="2" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M8 11V7a4 4 0 118 0v4"/>',
    user: '<path stroke-linecap="round" stroke-linejoin="round" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>',
    logout: '<path stroke-linecap="round" stroke-linejoin="round" d="M17 16l4-4m0 0l-4-4m4 4H7m6 5v1a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h5a2 2 0 012 2v1"/>',
  };

  function svg(key, extra) {
    return `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true" class="${extra || ""}">${ICONS[key] || ""}</svg>`;
  }

  function buildSidebarHTML(activeKey, collapsed) {
    const groups = NAV_GROUPS.map((g) => {
      const items = g.items
        .map((it) => {
          const isActive = it.key === activeKey;
          return `
            <a href="${it.href}" class="admin2-nav-link${isActive ? " admin2-nav-link--active" : ""}" title="${it.label}">
              ${svg(it.icon, "admin2-nav-icon")}
              <span class="admin2-nav-label">${it.label}</span>
            </a>`;
        })
        .join("");
      return `
        <div class="admin2-nav-group">
          <div class="admin2-nav-group-label">${g.label}</div>
          ${items}
        </div>`;
    }).join("");

    return `
      <aside class="admin2-sidebar${collapsed ? " admin2-sidebar--collapsed" : ""}" id="admin2Sidebar">
        <div class="admin2-sidebar-head">
          <a href="/" class="admin2-brand">
            <svg width="20" height="20" viewBox="0 0 32 32" fill="none" aria-hidden="true"><path d="M16 3l4 4-9 9-4-4 9-9zM7 16l4 4-4 4-4-4 4-4zM21 16l4 4-9 9-4-4 9-9z" fill="#111827"/></svg>
            <span class="admin2-brand-text">لوحة المدير</span>
          </a>
          <button type="button" id="admin2CollapseBtn" class="admin2-icon-btn" title="طي/توسيع القائمة">
            ${svg("list")}
          </button>
        </div>
        <nav class="admin2-nav" aria-label="تنقل لوحة المدير">${groups}</nav>
        <div class="admin2-nav-group admin2-nav-group--footer">
          <button type="button" id="admin2LogoutBtn" class="admin2-nav-link admin2-nav-link--danger">
            ${svg("logout", "admin2-nav-icon")}
            <span class="admin2-nav-label">تسجيل الخروج</span>
          </button>
        </div>
      </aside>
      <div class="admin2-sidebar-backdrop" id="admin2Backdrop"></div>`;
  }

  function buildTopbarHTML(title) {
    return `
      <header class="admin2-topbar">
        <div class="admin2-topbar-left">
          <button type="button" id="admin2MobileMenuBtn" class="admin2-icon-btn admin2-only-mobile" title="القائمة">
            ${svg("list")}
          </button>
          <a href="/admin" class="admin2-back-home" title="عودة للوحة الرئيسية">${svg("grid")}</a>
          <h1 class="admin2-title">${title || ""}</h1>
        </div>
        <div class="admin2-topbar-right">
          <button type="button" id="admin2ThemeBtn" class="admin2-icon-btn" title="تبديل المظهر">🌓</button>
          <div class="admin2-profile">
            <div class="admin2-avatar" id="admin2Avatar">?</div>
            <div class="admin2-profile-text">
              <div class="admin2-profile-name" id="admin2ProfileName">—</div>
              <div class="admin2-profile-role" id="admin2ProfileRole">مدير النظام</div>
            </div>
          </div>
        </div>
      </header>`;
  }

  function applyCollapsedPref() {
    try {
      return localStorage.getItem("admin2_sidebar_collapsed") === "1";
    } catch (e) {
      return false;
    }
  }

  async function initAdminLayout() {
    const host = document.getElementById("adminLayoutHost");
    if (!host) return;
    const activeKey = document.body.getAttribute("data-admin-active") || "";
    const title = document.body.getAttribute("data-admin-title") || document.title || "";
    const collapsed = applyCollapsedPref();

    host.innerHTML = buildSidebarHTML(activeKey, collapsed) + buildTopbarHTML(title);
    document.body.classList.toggle("admin2-collapsed", collapsed);

    document.getElementById("admin2CollapseBtn")?.addEventListener("click", () => {
      const next = !document.body.classList.contains("admin2-collapsed");
      document.body.classList.toggle("admin2-collapsed", next);
      document.getElementById("admin2Sidebar")?.classList.toggle("admin2-sidebar--collapsed", next);
      try { localStorage.setItem("admin2_sidebar_collapsed", next ? "1" : "0"); } catch (e) {}
    });

    document.getElementById("admin2MobileMenuBtn")?.addEventListener("click", () => {
      document.getElementById("admin2Sidebar")?.classList.add("admin2-sidebar--open");
      document.getElementById("admin2Backdrop")?.classList.add("admin2-sidebar-backdrop--visible");
    });
    document.getElementById("admin2Backdrop")?.addEventListener("click", () => {
      document.getElementById("admin2Sidebar")?.classList.remove("admin2-sidebar--open");
      document.getElementById("admin2Backdrop")?.classList.remove("admin2-sidebar-backdrop--visible");
    });

    document.getElementById("admin2LogoutBtn")?.addEventListener("click", async () => {
      try {
        await apiPostJson("/auth/logout", {});
      } catch (e) {}
      window.location.href = "/login";
    });

    document.getElementById("admin2ThemeBtn")?.addEventListener("click", () => {
      const dark = !document.documentElement.classList.contains("dark");
      document.documentElement.classList.toggle("dark", dark);
      try { localStorage.setItem("auditflow-theme", dark ? "dark" : "light"); } catch (e) {}
    });

    try {
      const me = await apiGet("/auth/me");
      if (!me || !me.username) {
        window.location.href = "/login";
        return;
      }
      const nameEl = document.getElementById("admin2ProfileName");
      const avatarEl = document.getElementById("admin2Avatar");
      if (nameEl) nameEl.textContent = me.username;
      if (avatarEl) avatarEl.textContent = me.username.trim().charAt(0).toUpperCase();
    } catch (e) {
      window.location.href = "/login";
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAdminLayout);
  } else {
    initAdminLayout();
  }
})();
