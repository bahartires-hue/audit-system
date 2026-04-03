## AuditFlow (Backend + Frontend)

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

