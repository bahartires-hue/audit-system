from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException, Header
from fastapi.responses import HTMLResponse, FileResponse

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base, Session

import pandas as pd
import uuid
import pdfplumber
import jwt

from passlib.hash import pbkdf2_sha256

app = FastAPI()

# ================= DB =================
engine = create_engine("sqlite:///new.db", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    password = Column(String)

Base.metadata.create_all(engine)

# ================= AUTH =================
SECRET = "SECRET_KEY"

def create_token(username):
    return jwt.encode({"user": username}, SECRET, algorithm="HS256")

def check_auth(token: str):
    try:
        jwt.decode(token, SECRET, algorithms=["HS256"])
    except:
        raise HTTPException(401, "غير مصرح")

# ================= UTILS =================
def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def safe(v):
    try:
        if v is None:
            return None

        v = str(v).replace(",", "").strip()

        if v == "":
            return None

        return round(float(v), 2)

    except:
        return None

# ================= SMART DETECTION =================
def detect_columns(df):
    df.columns = df.columns.astype(str).str.strip()

    debit_col = None
    credit_col = None
    date_col = None

    # =========================================
    # 1. تحديد بالأسماء (أقوى شيء)
    # =========================================
    for col in df.columns:
        name = str(col).lower()

        if any(x in name for x in ["مدين", "debit", "dr"]):
            debit_col = col

        if any(x in name for x in ["دائن", "credit", "cr"]):
            credit_col = col

        if any(x in name for x in ["تاريخ","التاريخ","التأريخ","date"]):
            date_col = col

    # =========================================
    # 2. fallback ذكي (مو أي رقم)
    # =========================================
    numeric_cols = []

    for col in df.columns:
        nums = pd.to_numeric(df[col], errors='coerce')

        valid = nums.dropna()

        if len(valid) < len(df) * 0.3:
            continue

        mean_val = valid.mean()

        # 🔥 استبعد الأعمدة اللي شكلها IDs
        if mean_val < 10:
            continue

        numeric_cols.append((col, mean_val))

    # رتب حسب متوسط القيمة
    numeric_cols.sort(key=lambda x: x[1], reverse=True)

    if not debit_col and len(numeric_cols) >= 1:
        debit_col = numeric_cols[0][0]

    if not credit_col and len(numeric_cols) >= 2:
        credit_col = numeric_cols[1][0]

    # =========================================
    # 3. التاريخ fallback
    # =========================================
    if not date_col:
        for col in df.columns:
            parsed = pd.to_datetime(df[col], errors='coerce')
            if parsed.notna().sum() > len(df) * 0.5:
                date_col = col
                break

    return debit_col, credit_col, date_col

# ================= READ =================

def read_excel(file):
    df = pd.read_excel(file)

    if df is None or df.empty:
        return None

    df = df.dropna(how="all")
    return df


def read_pdf(file):
    rows = []

    with pdfplumber.open(file) as pdf:
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

    df = pd.DataFrame(rows)

    df = df.dropna(how="all")

    # 🔥 حماية من crash
    if len(df) < 2:
        return None

    # أول صف = header
    df.columns = df.iloc[0]
    df = df[1:]

    df = df.dropna(how="all")

    return df


def read_any(file, filename):
    name = filename.lower()

    if name.endswith(".xlsx") or name.endswith(".xls"):
        return read_excel(file)

    elif name.endswith(".pdf"):
        return read_pdf(file)

    else:
        raise Exception("نوع الملف غير مدعوم")


def process(file, filename, branch):
    df = read_any(file, filename)

    if df is None or len(df) == 0:
        return []

    df.columns = df.columns.astype(str).str.strip()

    debit_col, credit_col, date_col = detect_columns(df)

    # =========================================
    # 🔥 كشف عمود المستند
    # =========================================
    doc_col = None

    for col in df.columns:
        name = str(col).lower().strip()

        if any(x in name for x in [
            "مستند","المستند","نوع","بيان","وصف",
            "description","desc","document"
        ]):
            doc_col = col
            break


    # =========================================
    # fallback الأعمدة
    # =========================================
    if not debit_col and not credit_col:
        numeric_cols = []

        for col in df.columns:
            nums = pd.to_numeric(df[col], errors='coerce').dropna()

            if len(nums) < len(df) * 0.3:
                continue

            if nums.mean() < 10:
                continue

            numeric_cols.append((col, nums.mean()))

        numeric_cols.sort(key=lambda x: x[1], reverse=True)

        if len(numeric_cols) >= 1:
            debit_col = numeric_cols[0][0]

        if len(numeric_cols) >= 2:
            credit_col = numeric_cols[1][0]

    data = []

    for _, row in df.iterrows():

        if row.isna().all():
            continue

        debit  = safe(row[debit_col]) if debit_col in df.columns else None
        credit = safe(row[credit_col]) if credit_col in df.columns else None

        if debit is None and credit is None:
            continue

        # خطأ مدين + دائن
        if debit and credit and debit > 0 and credit > 0:
            amount = max(debit, credit)

            data.append({
                "amount": float(amount),
                "type": "error",
                "branch": branch,
                "date": None,
                "doc": "",
                "reason": "خطأ: الصف يحتوي مدين ودائن"
            })
            continue

        # تحديد النوع
        if credit and credit > 0:
            amount = credit
            t = "credit"

        elif debit and debit > 0:
            amount = debit
            t = "debit"

        else:
            continue

        # التاريخ
        date = None

        if date_col and date_col in df.columns:
            try:
                val = row[date_col]

                if pd.isna(val):
                    date = None
                else:
                    d = pd.to_datetime(val, errors='coerce', dayfirst=False)
                    if not pd.isna(d):
                        date = d.strftime("%Y-%m-%d")
                    else:
                        date = None
            except:
                date = str(row[date_col])

        # المستند
        doc = None

        if doc_col and doc_col in df.columns:
            val = row[doc_col]
            if pd.notna(val):
                doc = str(val).strip()

        data.append({
            "amount": float(amount),
            "type": t,
            "branch": branch,
            "date": date,
            "doc": doc
        })

    return data
    
# ================= ANALYZE =================

doc_map = {
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
    "مشتريات": "مبيعات"
}

import re
from difflib import SequenceMatcher

def clean(s):
    if not s:
        return ""

    s = str(s).lower().strip()

    # إزالة كلمات مزعجة
    for w in ["رقم", "no", "doc", "ref"]:
        s = s.replace(w, "")

    # حذف الأرقام 🔥
    s = re.sub(r'\d+', '', s)

    # إزالة رموز
    for ch in [" ", "-", "_", "/", "\\", ".", ","]:
        s = s.replace(ch, "")

    return s


def match_doc(d1, d2):

    # 🔥 تصحيح منطقي
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

    # 1. تطابق مباشر
    if d1 == d2:
        return True

    # 2. تطابق جزئي مضبوط
    if len(d1) > 3 and len(d2) > 3:
        if d1 in d2 or d2 in d1:
            return True

    # 3. mapping
    for key, val in doc_map.items():
        k = clean(key)
        v = clean(val)

        if (k in d1 and v in d2) or (v in d1 and k in d2):
            return True

    # 4. fuzzy حقيقي 🔥
    similarity = SequenceMatcher(None, d1, d2).ratio()
    if similarity > 0.7:
        return True

    return False
    
# ================= HELPERS =================

def date_diff_days(d1, d2):
    try:
        d1 = pd.to_datetime(d1, errors='coerce')
        d2 = pd.to_datetime(d2, errors='coerce')

        if pd.isna(d1) or pd.isna(d2):
            return None

        return abs((d1 - d2).days)
    except:
        return None


def clean_doc(s):
    s = str(s).lower().strip()

    s = s.replace("رقم", "")
    s = s.replace("-", "")
    s = s.replace("_", "")
    s = s.replace("  ", " ")

    return s


# ================= ANALYZE =================

def analyze(d1, d2):
    res = []
    used = [False] * len(d2)
    counts = {}

    # =========================================
    # حذف العمليات العكسية
    # =========================================
    def remove_reversals(data):
        cleaned = []
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

    # تطبيق الحذف
    d1 = remove_reversals(d1)
    d2 = remove_reversals(d2)

    # =========================================
    # لو الفرع الثاني فاضي
    # =========================================
    if not d2:
        for x in d1:
            res.append({
                **x,
                "reason": "لا يوجد مقابل ❌ (الفرع الثاني فارغ)"
            })
            b = x.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
        return res, counts

    # =========================================
    # نظام التقييم
    # =========================================
    def match_score(x1, x2):
        score = 0
        reasons = []

        # المبلغ
        diff = abs(x1["amount"] - x2["amount"])
        if diff < 0.01:
            score += 50
            reasons.append("نفس المبلغ")
        elif diff < 1:
            score += 30
            reasons.append("مبلغ قريب")
        else:
            return 0, ["فرق مبلغ كبير"]

        # الاتجاه
        if (
            (x1["type"] == "credit" and x2["type"] == "debit") or
            (x1["type"] == "debit" and x2["type"] == "credit")
        ):
            score += 30
            reasons.append("اتجاه عكسي صحيح")
        else:
            return 0, ["نفس الاتجاه"]

        # 🔥 شرط المستند
        if x1.get("doc") and x2.get("doc"):
            if not match_doc(x1["doc"], x2["doc"]):
                return 0, ["اختلاف نوع المستند"]

        # التاريخ
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

        # تقييم المستند
        if match_doc(x1.get("doc"), x2.get("doc")):
            score += 20
            reasons.append("نوع مستند مطابق")
        else:
            score -= 10
            reasons.append("اختلاف نوع المستند")

        return score, reasons

    # =========================================
    # المطابقة
    # =========================================
    for x1 in d1:

        if x1.get("type") == "error":
            res.append(x1)
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1
            continue

        best_i = -1
        best_score = -1
        best_reason = []

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
            res.append({
                **x1,
                "reason": f"تطابق ضعيف ⚠️ | score={best_score} | {' , '.join(best_reason)}"
            })
            used[best_i] = True

        else:
            res.append({
                **x1,
                "reason": f"لا يوجد مقابل ❌ | score={best_score} | {' , '.join(best_reason)}"
            })
            b = x1.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1

    # =========================================
    # الباقي من الفرع الثاني
    # =========================================
    for i, x in enumerate(d2):
        if not used[i]:

            if x.get("type") == "error":
                res.append(x)
                b = x.get("branch") or "unknown"
                counts[b] = counts.get(b, 0) + 1
                continue

            res.append({
                **x,
                "reason": "لا يوجد مقابل ❌ (من الفرع الآخر)"
            })
            b = x.get("branch") or "unknown"
            counts[b] = counts.get(b, 0) + 1

    return res, counts
    
# ================= FRONTEND (نفس واجهتك) =================
@app.get("/", response_class=HTMLResponse)
def home():
    return """ 
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=Cairo:wght@300;600;800&display=swap" rel="stylesheet">

<style>
*{font-family:Cairo;box-sizing:border-box}
body{margin:0;background:#f1f5f9;color:#111;transition:0.3s;}
body.dark{background:#020617;color:#fff;}
.container{padding:20px;max-width:1100px;margin:auto;}
.topbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;}
.logo{font-size:20px;font-weight:800;color:#3b82f6;}
#welcomeUser{font-size:16px;color:#1d4ed8;font-weight:900;}
.btn{padding:10px;border:none;border-radius:10px;cursor:pointer;}
.btn-danger{background:#ef4444;color:#fff;}
.btn-mode{background:#e2e8f0;}
.card{background:#fff;padding:20px;border-radius:15px;margin-bottom:20px;box-shadow:0 5px 20px rgba(0,0,0,0.05);}
body.dark .card{background:#0f172a;}
input{width:100%;padding:10px;margin:5px 0 10px;border-radius:8px;border:1px solid #ddd;}
.analyze-btn{width:150px;margin:auto;display:block;padding:10px;background:#3b82f6;color:#fff;border:none;border-radius:10px;}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin-bottom:15px;}
.stat{background:#fff;padding:15px;border-radius:12px;text-align:center;border:1px solid #e5e7eb;}
.stat b{font-size:26px;color:#3b82f6;display:block;}
.stat span{font-size:14px;color:#666;}
.errors{display:grid;grid-template-columns:1fr 1fr;gap:15px;}
.error{background:#fff;border:1px solid #e5e7eb;padding:18px;border-radius:14px;margin-bottom:12px;}
.error div{font-size:15px;margin-bottom:5px;}
.bar{background:#e5e7eb;height:10px;border-radius:10px;margin-top:5px;overflow:hidden}
.bar-inner{background:#ef4444;height:100%}
.toast{position:fixed;bottom:20px;left:20px;background:#22c55e;color:#fff;padding:12px 20px;border-radius:10px;display:none;z-index:999;}
.hidden{display:none}

/* =================== Upload UI (matches design) =================== */
#systemBox .container{padding:28px 20px 40px;max-width:980px;}
#systemBox .topbar{display:none;}
#systemBox .card{margin-bottom:0;box-shadow:none;border:1px solid rgba(15,23,42,0.08);}
.audit-shell{background:#fff;border-radius:18px;padding:26px 22px;}
.audit-title{margin:0;text-align:center;font-size:28px;font-weight:800;color:#0f172a;}
.audit-sub{margin:8px 0 0;text-align:center;color:#64748b;font-size:14px;line-height:1.6;}
.upload-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:26px;align-items:start;}
.upload-card{border:1px solid rgba(15,23,42,0.10);border-radius:16px;padding:16px;background:#fff;}
.upload-head{display:flex;align-items:center;gap:8px;margin-bottom:10px;}
.upload-dot{width:10px;height:10px;border-radius:50%;background:#2563eb;box-shadow:0 0 0 4px rgba(37,99,235,0.10);}
.upload-label{font-weight:700;color:#1d4ed8;font-size:13px;}
.dz-excel .upload-dot{background:#10b981;box-shadow:0 0 0 4px rgba(16,185,129,0.12);}

.dropzone{
    border:2px dashed rgba(148,163,184,0.55);
    border-radius:14px;
    background:#f8fafc;
    height:120px;
    display:flex;
    align-items:center;
    justify-content:center;
    flex-direction:column;
    gap:8px;
    cursor:pointer;
    transition:0.15s;
    user-select:none;
}
.dropzone.dragover{
    border-color:#2563eb;
    background:#eff6ff;
}
.dz-excel .dropzone.dragover{
    border-color:#10b981;
    background:#ecfdf5;
}
.dz-icon{
    width:42px;height:42px;
    display:flex;align-items:center;justify-content:center;
    border-radius:12px;
    background:rgba(37,99,235,0.08);
    color:#2563eb;
}
.dz-excel .dz-icon{
    background:rgba(16,185,129,0.10);
    color:#10b981;
}
.dz-icon svg{width:22px;height:22px;stroke:currentColor;stroke-width:2;fill:none}
.dz-title{font-size:13px;font-weight:800;color:#0f172a;}
.dz-sub{font-size:12px;color:#64748b;line-height:1.4;}
.file-hint{margin-top:10px;font-size:12px;color:#94a3b8;min-height:16px;}

.start-row{display:flex;justify-content:center;margin-top:18px;}
.start-btn{
    border:none;
    background:#2563eb;
    color:#fff;
    border-radius:12px;
    padding:10px 18px;
    font-size:14px;
    font-weight:800;
    cursor:pointer;
    transition:0.15s;
}
.start-btn:active{transform:translateY(1px)}

/* results hidden until analysis */
#stats.hidden, #totals.hidden, #filterCard.hidden, #errorsCard.hidden {display:none !important;}

.spinner{
    width:14px;height:14px;
    border:2px solid rgba(255,255,255,0.35);
    border-top-color:#fff;
    border-radius:50%;
    display:inline-block;
    vertical-align:middle;
    margin-inline-start:8px;
    animation:spin 0.7s linear infinite;
}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>

<body>

<div id="toast" class="toast"></div>

<div id="loginBox" class="container">
<div class="card" style="max-width:400px;margin:auto">
<h2>تسجيل الدخول</h2>
<input id="user">
<input id="pass" type="password">
<button class="analyze-btn" onclick="login()">دخول</button>
<button class="btn btn-mode" onclick="goRegister()">إنشاء حساب</button>
</div>
</div>

<div id="registerBox" class="container hidden">
<div class="card" style="max-width:400px;margin:auto">
<h2>إنشاء حساب</h2>
<input id="ruser">
<input id="rpass" type="password">
<button class="analyze-btn" onclick="register()">تسجيل</button>
<button class="btn btn-mode" onclick="goLogin()">رجوع</button>
</div>
</div>

<div id="systemBox" class="hidden">
<div class="container">

<div class="topbar">
<div>
<div class="logo">📊 Smart Audit</div>
<div id="welcomeUser"></div>
</div>
<div>
<button class="btn btn-mode" onclick="toggleMode()">الوضع</button>
<button class="btn btn-danger" onclick="logout()">خروج</button>
</div>
</div>

<div class="card audit-shell">
    <h1 class="audit-title">تدقيق مالي</h1>
    <div class="audit-sub">ارفع ملف Excel و PDF للمقارنة وتحليل الفروقات تلقائيًا.</div>

    <!-- keep values for API -->
    <input type="hidden" id="b1" value="الفرع الأول">
    <input type="hidden" id="b2" value="الفرع الثاني">

    <div class="upload-grid">
        <div class="upload-card dz-excel">
            <div class="upload-head">
                <span class="upload-dot"></span>
                <span class="upload-label">الفرع الأول</span>
            </div>

            <input type="file" id="f1" accept=".xlsx,.xls,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" class="hidden">
            <div class="dropzone" id="dz1" data-target="f1">
                <div class="dz-icon" aria-hidden="true">
                    <svg viewBox="0 0 24 24">
                        <path d="M12 3v10" stroke-linecap="round"/>
                        <path d="M8 9l4-4 4 4" stroke-linecap="round" stroke-linejoin="round"/>
                        <path d="M5 14v4a3 3 0 0 0 3 3h8a3 3 0 0 0 3-3v-4" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                </div>
                <div class="dz-title">Excel</div>
                <div class="dz-sub">اسحب وأفلت أو اضغط للاختيار</div>
            </div>
            <div class="file-hint" id="fileName1"> </div>
        </div>

        <div class="upload-card">
            <div class="upload-head">
                <span class="upload-dot"></span>
                <span class="upload-label">الفرع الثاني</span>
            </div>

            <input type="file" id="f2" accept=".pdf,application/pdf" class="hidden">
            <div class="dropzone" id="dz2" data-target="f2">
                <div class="dz-icon" aria-hidden="true">
                    <svg viewBox="0 0 24 24">
                        <path d="M12 3v10" stroke-linecap="round"/>
                        <path d="M8 9l4-4 4 4" stroke-linecap="round" stroke-linejoin="round"/>
                        <path d="M5 14v4a3 3 0 0 0 3 3h8a3 3 0 0 0 3-3v-4" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                </div>
                <div class="dz-title">PDF</div>
                <div class="dz-sub">اسحب وأفلت أو اضغط للاختيار</div>
            </div>
            <div class="file-hint" id="fileName2"> </div>
        </div>
    </div>

    <div class="start-row">
        <button id="analyzeBtn" class="start-btn" onclick="upload()">ابدأ التحليل</button>
    </div>
    
    <div style="display:flex;justify-content:center;margin-top:12px">
        <button id="downloadBtn" class="start-btn hidden" style="background:#10b981" onclick="download()">تحميل التقرير</button>
    </div>
</div>

<div id="stats" class="stats hidden"></div>
<div id="totals" class="card hidden"></div>

<div class="card hidden" id="filterCard">
    <h3>فلترة الأخطاء</h3>

    <input id="filterDoc" placeholder="نوع المستند">
    <input id="filterAmount" placeholder="المبلغ">

    <input id="filterType" placeholder="نوع الخطأ (❌ أو ⚠️)">

    <button class="analyze-btn" onclick="applyFilter()">تطبيق</button>
    <button class="btn btn-mode" onclick="resetFilter()">إلغاء</button>
</div>

<div class="card hidden" id="errorsCard">
    <h3>الأخطاء</h3>
    <div class="errors">
        <div id="right"></div>
        <div id="left"></div>
    </div>
</div>

</div>
</div>

<script>

let TOKEN=""
let USERNAME=""
let ALL_ERRORS=[]
let SELECTED_FILE1 = null
let SELECTED_FILE2 = null

// ================= FILTER =================
function applyFilter(){

    let doc = document.getElementById("filterDoc").value.toLowerCase().trim()
    let amount = document.getElementById("filterAmount").value.trim()
    let type = document.getElementById("filterType").value.trim()

    let filtered = ALL_ERRORS

    // فلترة بالمستند
    if(doc){
        filtered = filtered.filter(x => 
            (x.doc || "").toLowerCase().includes(doc)
        )
    }

    // فلترة بالمبلغ
    if(amount){
        filtered = filtered.filter(x => 
            String(x.amount) === amount
        )
    }

    // 🔥 فلترة بنوع الخطأ
    if(type){
        filtered = filtered.filter(x =>
            (x.reason || "").includes(type)
        )
    }

    render(filtered)
}


// ================= RESET =================
function resetFilter(){
    document.getElementById("filterDoc").value = ""
    document.getElementById("filterAmount").value = ""
    document.getElementById("filterType").value = ""
    render(ALL_ERRORS)
}


// ================= UI =================
function showToast(msg,color="#22c55e"){
let t=document.getElementById("toast")
t.innerText=msg
t.style.background=color
t.style.display="block"
setTimeout(()=>t.style.display="none",3000)
}

function toggleMode(){document.body.classList.toggle("dark")}
function logout(){location.reload()}

// ================= AUTH =================
function goRegister(){
loginBox.classList.add("hidden")
registerBox.classList.remove("hidden")
}

function goLogin(){
registerBox.classList.add("hidden")
loginBox.classList.remove("hidden")
}

async function register(){
let f=new FormData()
f.append("username",ruser.value)
f.append("password",rpass.value)
let r = await fetch("/register",{method:"POST",body:f})
let d = await r.json()
showToast("تم إنشاء الحساب")
goLogin()
}

async function login(){
let f=new FormData()
f.append("username",user.value)
f.append("password",pass.value)

let r=await fetch("/login",{method:"POST",body:f})
let d=await r.json()

if(d.token){
TOKEN=d.token
USERNAME=d.username
loginBox.classList.add("hidden")
systemBox.classList.remove("hidden")
welcomeUser.innerText="مرحبًا "+USERNAME
}else{
showToast("فشل تسجيل الدخول","#ef4444")
}
}

// ================= DROPZONE =================
function setSelected(fileInputId, fileObj){
    if(fileInputId === "f1"){
        SELECTED_FILE1 = fileObj
        let el = document.getElementById("fileName1")
        if(el){
            el.innerText = fileObj ? ("تم اختيار: " + (fileObj.name || "")) : ""
        }
    }else{
        SELECTED_FILE2 = fileObj
        let el = document.getElementById("fileName2")
        if(el){
            el.innerText = fileObj ? ("تم اختيار: " + (fileObj.name || "")) : ""
        }
    }
}

function validateFile(fileObj, expected){
    if(!fileObj) return false
    let name = (fileObj.name || "").toLowerCase()
    if(expected === "excel"){
        return name.endsWith(".xlsx") || name.endsWith(".xls")
    }
    if(expected === "pdf"){
        return name.endsWith(".pdf")
    }
    return true
}

function bindDropzone(dzId, inputId, expectedType){
    let dz = document.getElementById(dzId)
    let inp = document.getElementById(inputId)
    if(!dz || !inp) return

    dz.addEventListener("click", ()=> inp.click())

    ;["dragenter","dragover"].forEach(evtName=>{
        dz.addEventListener(evtName, (e)=>{
            e.preventDefault()
            e.stopPropagation()
            dz.classList.add("dragover")
        })
    })

    ;["dragleave","drop"].forEach(evtName=>{
        dz.addEventListener(evtName, (e)=>{
            e.preventDefault()
            e.stopPropagation()
            dz.classList.remove("dragover")
        })
    })

    dz.addEventListener("drop", (e)=>{
        let dt = e.dataTransfer
        let fileObj = dt && dt.files && dt.files.length ? dt.files[0] : null
        if(!fileObj || !validateFile(fileObj, expectedType)){
            showToast("نوع الملف غير صحيح", "#ef4444")
            setSelected(inputId, null)
            return
        }

        setSelected(inputId, fileObj)
    })

    inp.addEventListener("change", ()=>{
        let fileObj = inp.files && inp.files.length ? inp.files[0] : null
        if(fileObj && !validateFile(fileObj, expectedType)){
            showToast("نوع الملف غير صحيح", "#ef4444")
            inp.value = ""
            setSelected(inputId, null)
            return
        }
        setSelected(inputId, fileObj)
    })
}

window.addEventListener("DOMContentLoaded", ()=>{
    bindDropzone("dz1","f1","excel")
    bindDropzone("dz2","f2","pdf")
})

// ================= RENDER =================
function render(errors){

    let right = document.getElementById("right")
    let left  = document.getElementById("left")

    errors.sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0))

    right.innerHTML = `<h4>${b1.value}</h4>`
    left.innerHTML  = `<h4>${b2.value}</h4>`

    errors.filter(x => x.branch == b1.value).forEach(x=>{
        right.innerHTML+=`
        <div class="error">
            <div>المبلغ: ${x.amount}</div>
            <div>نوع المستند: ${x.doc || "-"}</div>
            <div>التاريخ: ${x.date || "-"}</div>
            <div>السبب: ${x.reason || "-"}</div>
        </div>`
    })

    errors.filter(x => x.branch == b2.value).forEach(x=>{
        left.innerHTML+=`
        <div class="error">
            <div>المبلغ: ${x.amount}</div>
            <div>نوع المستند: ${x.doc || "-"}</div>
            <div>التاريخ: ${x.date || "-"}</div>
            <div>السبب: ${x.reason || "-"}</div>
        </div>`
    })
}


// ================= UPLOAD (FINAL) =================
async function upload(){

    // 🔥 إضافة فقط (ما غيرنا شيء)
    if(!TOKEN){
        showToast("يجب تسجيل الدخول أولاً","#ef4444")
        return
    }

    let btn = document.getElementById("analyzeBtn")

    // 🔥 تشغيل loading
    btn.disabled = true
    btn.innerHTML = `جاري التحليل <span class="spinner"></span>`
    btn.style.opacity = "0.6"

    let file1 = SELECTED_FILE1 || (document.getElementById("f1").files ? document.getElementById("f1").files[0] : null)
    let file2 = SELECTED_FILE2 || (document.getElementById("f2").files ? document.getElementById("f2").files[0] : null)

    if(!file1 || !file2){
        showToast("اختار الملفين أولاً","#ef4444")

        btn.disabled = false
        btn.innerText = "تحليل"
        btn.style.opacity = "1"
        return
    }

    let f=new FormData()
    f.append("file1", file1)
    f.append("file2", file2)
    f.append("b1",b1.value)
    f.append("b2",b2.value)

    try{

        let r=await fetch("/analyze",{
            method:"POST",
            body:f,
            headers:{
                "Authorization":"Bearer "+TOKEN
            }
        })

        // 🔥 إضافة فقط
        if(r.status === 401){
            showToast("انتهت الجلسة، سجل دخول مرة ثانية","#ef4444")
            logout()
            return
        }

        if(!r.ok){
            let text = await r.text()
            console.log("❌ response:", text)
            showToast("خطأ في التحليل","#ef4444")

            btn.disabled = false
            btn.innerText = "تحليل"
            btn.style.opacity = "1"
            return
        }

        let d = await r.json()

        // 🔥 حماية من crash
        if (!d || !d.errors || !Array.isArray(d.errors)) {
            console.log("❌ رد السيرفر غلط:", d)
            showToast("التحليل رجع بيانات غير صحيحة","#ef4444")

            btn.disabled = false
            btn.innerText = "تحليل"
            btn.style.opacity = "1"
            return
        }

        if (!d.counts) {
            d.counts = {}
        }

        // 🔥 تخزين الأخطاء
        ALL_ERRORS = d.errors

        // =========================================
        // 🔥 stats (عدد + نسبة)
        // =========================================
        let c1 = d.counts?.[b1.value] || 0
        let c2 = d.counts?.[b2.value] || 0

        let totalErrors = ALL_ERRORS.length || 1

        let p1 = Math.round((c1 / totalErrors) * 100)
        let p2 = Math.round((c2 / totalErrors) * 100)

        let stats = document.getElementById("stats")

        stats.innerHTML = `
<div class="stat">
    <span>${b1.value}</span>
    <b>${c1}</b>
    <span>عدد الأخطاء</span>
    <small>${p1}%</small>
</div>

<div class="stat">
    <span>${b2.value}</span>
    <b>${c2}</b>
    <span>عدد الأخطاء</span>
    <small>${p2}%</small>
</div>
`

        // 🔥 عرض الكل
        render(ALL_ERRORS)

        showToast("تم التحليل ✔️")
        
        // Show results UI
        document.getElementById("stats").classList.remove("hidden")
        document.getElementById("totals").classList.remove("hidden")
        document.getElementById("filterCard").classList.remove("hidden")
        document.getElementById("errorsCard").classList.remove("hidden")
        document.getElementById("downloadBtn").classList.remove("hidden")

    } catch(e){
        console.error("❌ error:", e)
        showToast("حصل خطأ غير متوقع","#ef4444")
    }

    // 🔥 إرجاع الزر طبيعي
    btn.disabled = false
    btn.innerText = "تحليل"
    btn.style.opacity = "1"
}

// ================= FILTER ERRORS =================
function filterErrors(){

    if (!ALL_ERRORS || !Array.isArray(ALL_ERRORS)) {
        console.log("مافي بيانات للفلترة")
        return
    }

    let filtered = ALL_ERRORS.filter(x =>
        x.reason && x.reason.includes("❌")
    )

    render(filtered)
}


// ================= SHOW ALL =================
function showAll(){
    render(ALL_ERRORS)
}

// ================= DOWNLOAD =================
function download(){
fetch("/download",{headers:{"Authorization":"Bearer "+TOKEN}})
.then(res=>res.blob())
.then(blob=>{
let url=URL.createObjectURL(blob)
let a=document.createElement("a")
a.href=url
a.download="report.xlsx"
a.click()
})
}
</script>

</body>
</html>
    """

# ================= API =================
last_errors = []

@app.post("/register")
def register(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    if db.query(User).filter_by(username=username).first():
        return {"msg":"المستخدم موجود"}
    db.add(User(username=username, password=pbkdf2_sha256.hash(password)))
    db.commit()
    return {"msg":"تم"}


@app.post("/login")
def login(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    user = db.query(User).filter_by(username=username).first()

    if not user:
        return {"error": "user_not_found"}

    if not pbkdf2_sha256.verify(password, user.password):
        return {"error": "wrong_password"}

    return {
        "token": create_token(username),
        "username": username
    }
@app.post("/analyze")
def analyze_api(
    authorization: str = Header(None),
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    b1: str = Form(...),
    b2: str = Form(...)
):
    # 🔥 حماية بدون ما يطيح
    if not authorization:
        raise HTTPException(401, "Missing token")

    parts = authorization.split()

    if len(parts) != 2:
        raise HTTPException(401, "Invalid token format")

    scheme, token = parts

    check_auth(token)

    d1 = process(file1.file, file1.filename, b1)
    d2 = process(file2.file, file2.filename, b2)

    errors, counts = analyze(d1, d2)

    global last_errors
    last_errors = errors

    totals = {
        b1: len(d1),
        b2: len(d2)
    }

    return {"errors": errors, "counts": counts, "totals": totals}
    
@app.get("/download")
def download(authorization: str = Header(None)):
    # 🔥 حماية بدون crash
    if not authorization:
        raise HTTPException(401, "Missing token")

    parts = authorization.split()

    if len(parts) != 2:
        raise HTTPException(401, "Invalid token format")

    scheme, token = parts

    check_auth(token)

    df = pd.DataFrame(last_errors)
    name = f"report_{uuid.uuid4().hex}.xlsx"
    df.to_excel(name, index=False)

    return FileResponse(name, filename="report.xlsx")
