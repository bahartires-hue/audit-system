## التطابق الأمثل (OptimalMatch) — Backend + Frontend

### التشغيل

#### 1) Backend
افتح PowerShell:

```powershell
cd "C:\Users\DELL\OneDrive\Desktop\audit-system\auditflow\backend"
.\venv\Scripts\python -m uvicorn app.main:app --host 127.0.0.1 --port 8001
```

افتح المتصفح:
- `http://127.0.0.1:8001/`

### ملاحظات
- الكود في:
  - `backend/app/main.py` (API + استضافة صفحات الواجهة)
  - `backend/app/services/analyzer.py` (التحليل المحلي)
  - `frontend/` (صفحات HTML + `app.js`)

### النشر على Railway

- Root Directory: اتركه على جذر المستودع `audit-system`
- Start Command:
  - `uvicorn auditflow.backend.app.main:app --host 0.0.0.0 --port $PORT`
- Healthcheck:
  - `/healthz`
- يعتمد Railway الآن على:
  - `railway.json`
  - `Procfile`
  - `requirements.txt` في الجذر والذي يثبت حزم `auditflow/backend/requirements.txt`

#### متغيرات بيئة مهمة

- `AUDITFLOW_COOKIE_SECURE=1`
- `AUDITFLOW_PUBLIC_BASE_URL=https://<your-railway-domain>`
- `DATABASE_URL` إذا ستستخدم Postgres
- إن لم تستخدم Postgres فالتطبيق سيعمل بـ SQLite محليًا داخل الحاوية، لكنه غير مناسب لتخزين دائم عبر إعادة النشر

