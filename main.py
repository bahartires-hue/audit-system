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
        v = str(v).replace(",", "").strip()
        return round(float(v), 2)
    except:
        return 0

# ================= SMART DETECTION =================
def detect_columns(df):
    debit_col = None
    credit_col = None
    date_col = None

    for col in df.columns:
        name = str(col).strip().lower()

        # 👇 تحديد بالاسم (الأهم)
        if "مدين" in name or "debit" in name:
            debit_col = col
            continue

        if "دائن" in name or "credit" in name:
            credit_col = col
            continue

        if "تاريخ" in name or "date" in name:
            date_col = col
            continue

    # 👇 fallback لو الاسم فشل
    numeric_cols = []
    for col in df.columns:
        sample = df[col].dropna().head(20)
        nums = pd.to_numeric(sample, errors='coerce')

        if nums.notna().sum() > len(sample)*0.6:
            numeric_cols.append(col)

    if not debit_col and len(numeric_cols) > 0:
        debit_col = numeric_cols[0]

    if not credit_col and len(numeric_cols) > 1:
        credit_col = numeric_cols[1]

    # 👇 fallback للتاريخ
    if not date_col:
        for col in df.columns:
            sample = df[col].dropna().head(20)
            parsed = pd.to_datetime(sample, errors='coerce')

            if parsed.notna().sum() > len(sample)*0.6:
                date_col = col
                break

    return debit_col, credit_col, date_col
    
# ================= READ =================

def read_excel(file):
    return pd.read_excel(file)

def read_pdf(file):
    rows=[]
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    rows.append(row)

    df=pd.DataFrame(rows)
    df=df.dropna(how="all")
    df.columns=df.iloc[0]
    df=df[1:]
    return df

# 👇 هذا اللي ناقصك (المهم جداً)
def read_any(file, filename):
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        return read_excel(file)
    elif filename.endswith(".pdf"):
        return read_pdf(file)
    else:
        raise Exception("نوع الملف غير مدعوم")

def process(file, filename, branch):
    df = read_any(file, filename)
    df.columns = df.columns.astype(str).str.strip()

    debit_col, credit_col, date_col = detect_columns(df)

    # عمود المستند (اختياري)
    doc_col = None
    for col in df.columns:
        name = str(col).lower()
        if any(x in name for x in ["مستند", "doc", "نوع", "بيان", "الوصف", "description"]):
            doc_col = col

    if not debit_col and not credit_col:
        raise Exception("❌ لم يتم التعرف على الأعمدة")

    data = []

    for _, row in df.iterrows():

        if row.isna().all():
            continue

        debit  = safe(row[debit_col]) if debit_col else 0
        credit = safe(row[credit_col]) if credit_col else 0

        # 🔥 إصلاح التاريخ (المهم)
        date_val = row[date_col] if date_col else None

        try:
            if isinstance(date_val, (int, float)):
                date = pd.to_datetime(date_val, unit='d', origin='1899-12-30')
            else:
                date = pd.to_datetime(str(date_val), errors='coerce', dayfirst=True)

            if pd.isna(date):
                date = str(date_val)
            else:
                date = date.strftime("%Y-%m-%d")

        except:
            date = str(date_val)

        # المستند
        doc = str(row[doc_col]).strip() if doc_col else ""

        # تجاهل الصفوف بدون مبلغ
        if debit == 0 and credit == 0:
            continue

        # لا نكرر الصف
        if credit > 0:
            amount = credit
            t = "credit"
        else:
            amount = debit
            t = "debit"

        data.append({
            "amount": float(amount),
            "type": t,
            "branch": branch,
            "date": date,
            "doc": doc
        })

    return data
    
# ================= ANALYZE =================
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

def clean(s):
    s = str(s).strip().lower()
    s = s.replace(" ", "")
    s = s.replace("-", "")
    s = s.replace("_", "")
    return s

def match_doc(d1, d2):
    if not d1 or not d2:
        return False

    d1 = clean(d1)
    d2 = clean(d2)

    for key, val in doc_map.items():
        k = clean(key)
        v = clean(val)

        # 👇 تطابق مرن
        if k in d1 and v in d2:
            return True

        # 👇 دعم الاتجاه العكسي (احتياط)
        if v in d1 and k in d2:
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

    # 🔥 حذف العمليات العكسية داخل نفس الفرع
    def remove_internal_matches(data):
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

                # تاريخ مرن
                days = date_diff_days(x1["date"], x2["date"])
                if days is None or days > 1:
                    continue

                # 🔥 تعديل المبلغ (دقيق جداً)
                if abs(x1["amount"] - x2["amount"]) > 0.01:
                    continue

                # عكس النوع
                if x1["type"] != x2["type"]:
                    used_local[i] = True
                    used_local[j] = True
                    found = True
                    break

            if not found:
                cleaned.append(x1)

        return cleaned

    # تنظيف داخلي
    d1 = remove_internal_matches(d1)
    d2 = remove_internal_matches(d2)

    # 🔥 المطابقة الذكية
    for x1 in d1:
        best_i = -1
        best_score = -1

        for i, x2 in enumerate(d2):
            if used[i]:
                continue

            # لازم مدين مقابل دائن
            if x1.get("type") == x2.get("type"):
                continue

            amount1 = float(x1.get("amount") or 0)
            amount2 = float(x2.get("amount") or 0)

            diff = abs(amount1 - amount2)

            # 🔥 التعديل هنا
            if diff > 0.01:
                continue

            score = 0

            # 🔥 1. المبلغ
            score += 5

            # 🔥 2. التاريخ
            days = date_diff_days(x1.get("date"), x2.get("date"))

            if days is not None:
                if days == 0:
                    score += 3
                elif days <= 2:
                    score += 2
                elif days <= 5:
                    score += 1

            # 🔥 3. المستند
            doc1 = clean_doc(x1.get("doc"))
            doc2 = clean_doc(x2.get("doc"))

            if doc1 and doc2:
                if doc1 in doc2 or doc2 in doc1:
                    score += 2

            # اختيار الأفضل
            if score > best_score:
                best_score = score
                best_i = i

        if best_i != -1:
    used[best_i] = True
else:
    res.append({
        **x1,
        "reason": "لا يوجد عملية مطابقة في الفرع الآخر"
    })
    b = x1.get("branch") or "unknown"
    counts[b] = counts.get(b, 0) + 1
    
    # الباقي من الفرع الثاني
    for i, x in enumerate(d2):
    if not used[i]:
        res.append({
            **x,
            "reason": "لا يوجد عملية مطابقة في الفرع الآخر"
        })
        b = x.get("branch") or "unknown"
        counts[b] = counts.get(b, 0) + 1
    
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

#welcomeUser{
font-size:16px;
color:#1d4ed8;
font-weight:900;
}

.btn{padding:10px;border:none;border-radius:10px;cursor:pointer;}
.btn-danger{background:#ef4444;color:#fff;}
.btn-mode{background:#e2e8f0;}

.card{
background:#fff;
padding:20px;
border-radius:15px;
margin-bottom:20px;
box-shadow:0 5px 20px rgba(0,0,0,0.05);
}
body.dark .card{background:#0f172a;}

input{
width:100%;
padding:10px;
margin:5px 0 10px;
border-radius:8px;
border:1px solid #ddd;
}

.analyze-btn{
width:150px;
margin:auto;
display:block;
padding:10px;
background:#3b82f6;
color:#fff;
border:none;
border-radius:10px;
}

.stats{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(200px,1fr));
gap:10px;
margin-bottom:15px;
}
.stat{
background:#fff;
padding:15px;
border-radius:12px;
text-align:center;
border:1px solid #e5e7eb;
}
.stat b{font-size:26px;color:#3b82f6;display:block;}
.stat span{font-size:14px;color:#666;}

.errors{
display:grid;
grid-template-columns:1fr 1fr;
gap:15px;
}

.error{
background:#fff;
border:1px solid #e5e7eb;
padding:18px;
border-radius:14px;
margin-bottom:12px;
}
.error div{
font-size:15px;
margin-bottom:5px;
}

.bar{
background:#e5e7eb;
height:10px;
border-radius:10px;
margin-top:5px;
overflow:hidden
}
.bar-inner{background:#ef4444;height:100%}

.toast{
position:fixed;
bottom:20px;
left:20px;
background:#22c55e;
color:#fff;
padding:12px 20px;
border-radius:10px;
display:none;
z-index:999;
}

.hidden{display:none}
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

<div class="card">
<input id="b1" placeholder="الفرع الأول">
<input id="b2" placeholder="الفرع الثاني">
<input type="file" id="f1">
<input type="file" id="f2">

<div style="display:flex;gap:10px;justify-content:center;margin-top:10px">
<button class="analyze-btn" onclick="upload()">تحليل</button>
<button class="analyze-btn" style="background:#10b981" onclick="download()">تحميل التقرير</button>
</div>
</div>

<div id="stats" class="stats"></div>
<div id="totals" class="card"></div>

<div class="card">
<h3>فلترة الأخطاء</h3>
<input id="filterDoc" placeholder="نوع المستند">
<input id="filterAmount" placeholder="المبلغ">
<button class="analyze-btn" onclick="applyFilter()">تطبيق</button>
<button class="btn btn-mode" onclick="resetFilter()">إلغاء</button>
</div>

<div class="card">
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

function applyFilter(){

    let doc = document.getElementById("filterDoc").value.toLowerCase().trim()
    let amount = document.getElementById("filterAmount").value.trim()

    let filtered = ALL_ERRORS

    if(doc){
        filtered = filtered.filter(x => 
            (x.doc || "").toLowerCase().includes(doc)
        )
    }

    if(amount){
        filtered = filtered.filter(x => 
            String(x.amount) === amount
        )
    }

    render(filtered)
}

function resetFilter(){
    document.getElementById("filterDoc").value = ""
    document.getElementById("filterAmount").value = ""

    render(ALL_ERRORS)
}


function showToast(msg,color="#22c55e"){
let t=document.getElementById("toast")
t.innerText=msg
t.style.background=color
t.style.display="block"
setTimeout(()=>t.style.display="none",3000)
}

function toggleMode(){document.body.classList.toggle("dark")}
function logout(){location.reload()}

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

if(!r.ok){
    let text = await r.text()
    console.log("REGISTER ERROR:", text)
    showToast("خطأ في التسجيل","#ef4444")
    return
}

let d = await r.json()
showToast("تم إنشاء الحساب")
goLogin()
}

async function login(){
let f=new FormData()

let username = document.getElementById("user").value
let password = document.getElementById("pass").value

f.append("username", username)
f.append("password", password)

let r=await fetch("/login",{method:"POST",body:f})

if(!r.ok){
    let text = await r.text()
    console.log("SERVER ERROR:", text)
    showToast("خطأ في السيرفر","#ef4444")
    return
}

let d=await r.json()
console.log(d)

if(d.token){
TOKEN=d.token
USERNAME=d.username
loginBox.classList.add("hidden")
systemBox.classList.remove("hidden")
document.getElementById("welcomeUser").innerText="مرحبًا "+USERNAME
}else{
showToast("فشل تسجيل الدخول","#ef4444")
}
}

function render(errors){

    errors.sort((a, b) => new Date(b.date) - new Date(a.date))

    right.innerHTML = `<h4>${b1.value}</h4>`
    left.innerHTML  = `<h4>${b2.value}</h4>`
    
    errors
    .filter(x => x.branch == b1.value)
    .forEach(x => {
        right.innerHTML += `
        <div class="error">
            <div>المبلغ: ${x.amount}</div>
            <div>نوع المستند: ${x.doc || "-"}</div>
            <div>التاريخ: ${x.date ? new Date(x.date).toISOString().split('T')[0] : "-"}</div>
        </div>`
    })

    errors
    .filter(x => x.branch == b2.value)
    .forEach(x => {
        left.innerHTML += `
        <div class="error">
            <div>المبلغ: ${x.amount}</div>
            <div>نوع المستند: ${x.doc || "-"}</div>
            <div>التاريخ: ${x.date ? new Date(x.date).toISOString().split('T')[0] : "-"}</div>
        </div>`
    })
}
async function upload(){

let f=new FormData()
f.append("file1",f1.files[0])
f.append("file2",f2.files[0])
f.append("b1",b1.value)
f.append("b2",b2.value)

let r=await fetch("/analyze",{
method:"POST",
body:f,
headers:{
"Authorization":"Bearer "+TOKEN
}
})
if(!r.ok){
    let text = await r.text()
    console.log("ANALYZE ERROR:", text)
    showToast("خطأ في التحليل","#ef4444")
    return
}

let d=await r.json()

ALL_ERRORS=d.errors

let ordered = [
[b1.value, d.counts[b1.value] || 0],
[b2.value, d.counts[b2.value] || 0]
]

stats.innerHTML=""
ordered.forEach(([b,count])=>{
stats.innerHTML+=`
<div class="stat">
<span>${b}</span>
<b>${count}</b>
<span>عدد الأخطاء</span>
</div>`
})

let totalHTML = "<h3>نسبة الخطأ لكل فرع</h3>"

ordered.forEach(([b,count])=>{
let total = d.totals[b] || 0
let percent = total ? ((count / total) * 100).toFixed(1) : 0

totalHTML += `
<div style="margin-bottom:12px">
📍 ${b}: ${percent}%
<div class="bar">
<div class="bar-inner" style="width:${percent}%"></div>
</div>
</div>`
})

totals.innerHTML = totalHTML

render(ALL_ERRORS)
showToast("تم التحليل ✔️")
}

function download(){
fetch("/download", {
headers: {
"Authorization": "Bearer " + TOKEN
}
})
.then(res => res.blob())
.then(blob => {
let url = window.URL.createObjectURL(blob)
let a = document.createElement("a")
a.href = url
a.download = "report.xlsx"
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
    authorization: str = Header(...),
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    b1: str = Form(...),
    b2: str = Form(...)
):
    scheme, token = authorization.split()
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
def download(authorization: str = Header(...)):
    scheme, token = authorization.split()
    check_auth(token)

    df = pd.DataFrame(last_errors)
    name = f"report_{uuid.uuid4().hex}.xlsx"
    df.to_excel(name, index=False)

    return FileResponse(name, filename="report.xlsx")
