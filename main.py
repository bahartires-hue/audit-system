from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base, Session
import pandas as pd
import uuid

app = FastAPI()

engine = create_engine("sqlite:///db.sqlite", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    password = Column(String)

Base.metadata.create_all(engine)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

tokens = {}
last_errors = []

def check_auth(token: str):
    if token not in tokens:
        raise HTTPException(401, "غير مصرح")

def safe(v):
    try: return round(float(v),2)
    except: return 0

def read(file, branch):
    df = pd.read_excel(file)
    df.columns = df.columns.str.strip()
    data=[]
    for _,row in df.iterrows():
        debit  = safe(row.get("مدين_8",0))
        credit = safe(row.get("دائن_9",0))
        date = str(row.get("التأريخ_7",""))
        doc  = str(row.get("المستند_4",""))

        if debit > 0:
            data.append({"amount":debit,"branch":branch,"date":date,"doc":doc})
        elif credit > 0:
            data.append({"amount":credit,"branch":branch,"date":date,"doc":doc})
    return data

def analyze(d1,d2):
    res=[]; used=[False]*len(d2); counts={}
    for x1 in d1:
        found=False
        for i,x2 in enumerate(d2):
            if used[i]: continue
            if abs(x1["amount"]-x2["amount"])<=0.05:
                used[i]=True; found=True; break
        if not found:
            res.append(x1)
            counts[x1["branch"]] = counts.get(x1["branch"],0)+1

    for i,x in enumerate(d2):
        if not used[i]:
            res.append(x)
            counts[x["branch"]] = counts.get(x["branch"],0)+1

    return res,counts

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

/* ✅ تعديل فقط */
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

/* الإحصائيات */
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

/* الأخطاء */
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
color:#111;
margin-bottom:5px;
}

body.dark .error{
background:#0f172a;
border:1px solid #1e293b;
}
body.dark .error div{
color:#f1f5f9;
}

/* شريط النسبة */
.bar{
background:#e5e7eb;
height:10px;
border-radius:10px;
margin-top:5px;
overflow:hidden
}
.bar-inner{background:#ef4444;height:100%}

/* Toast */
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

<!-- تسجيل الدخول -->
<div id="loginBox" class="container">
<div class="card" style="max-width:400px;margin:auto">
<h2>تسجيل الدخول</h2>
<input id="user">
<input id="pass" type="password">
<button class="analyze-btn" onclick="login()">دخول</button>
<button class="btn btn-mode" onclick="goRegister()">إنشاء حساب</button>
</div>
</div>

<!-- تسجيل -->
<div id="registerBox" class="container hidden">
<div class="card" style="max-width:400px;margin:auto">
<h2>إنشاء حساب</h2>
<input id="ruser">
<input id="rpass" type="password">
<button class="analyze-btn" onclick="register()">تسجيل</button>
<button class="btn btn-mode" onclick="goLogin()">رجوع</button>
</div>
</div>

<!-- النظام -->
<div id="systemBox" class="hidden">
<div class="container">

<div class="topbar">
<div>
<div class="logo">📊 Smart Audit</div>
<div id="welcomeUser" style=""></div>
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

<!-- ✅ تعديل فقط -->
<div style="display:flex;gap:10px;justify-content:center;margin-top:10px">
<button class="analyze-btn" onclick="upload()">تحليل</button>
<button class="analyze-btn" style="background:#10b981" onclick="download()">تحميل التقرير</button>
</div>

</div>

<!-- الإحصائيات -->
<div id="stats" class="stats"></div>

<!-- النسب -->
<div id="totals" class="card"></div>

<!-- الفلترة -->
<div class="card">
<h3>فلترة الأخطاء</h3>
<input id="filterDoc" placeholder="نوع المستند">
<input id="filterAmount" placeholder="المبلغ">
<button class="analyze-btn" onclick="applyFilter()">تطبيق</button>
<button class="btn btn-mode" onclick="resetFilter()">إلغاء الفلترة</button>
</div>

<!-- الأخطاء -->
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
await fetch("/register",{method:"POST",body:f})
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
document.getElementById("welcomeUser").innerText="مرحبًا "+USERNAME
}else{
showToast("خطأ","#ef4444")
}
}

function render(errors){
right.innerHTML = `<h4 style="margin-bottom:10px">${b1.value}</h4>`
left.innerHTML  = `<h4 style="margin-bottom:10px">${b2.value}</h4>`

errors.filter(x=>x.branch==b1.value).forEach(x=>{
right.innerHTML+=`
<div class="error">
<div>المبلغ: ${x.amount}</div>
<div>نوع المستند: ${x.doc || "-"}</div>
<div>التاريخ: ${x.date || "-"}</div>
</div>`
})

errors.filter(x=>x.branch==b2.value).forEach(x=>{
left.innerHTML+=`
<div class="error">
<div>المبلغ: ${x.amount}</div>
<div>نوع المستند: ${x.doc || "-"}</div>
<div>التاريخ: ${x.date || "-"}</div>
</div>`
})
}

async function upload(){

let f=new FormData()
f.append("file1",f1.files[0])
f.append("file2",f2.files[0])
f.append("b1",b1.value)
f.append("b2",b2.value)
f.append("token",TOKEN)

let r=await fetch("/analyze",{method:"POST",body:f})
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

function applyFilter(){
let doc=filterDoc.value.toLowerCase()
let amount=filterAmount.value

let filtered=ALL_ERRORS.filter(x=>{
let dmatch=doc?(x.doc||"").toLowerCase().includes(doc):true
let amatch=amount?String(x.amount).includes(amount):true
return dmatch && amatch
})

render(filtered)

if(filtered.length==0){
showToast("لا توجد نتائج","#ef4444")
}else{
showToast("تمت الفلترة ✔️")
}
}

function resetFilter(){
filterDoc.value=""
filterAmount.value=""
render(ALL_ERRORS)
showToast("تم إلغاء الفلترة ✔️")
}

function download(){
showToast("تم تحميل التقرير ✔️")
window.open("/download?token="+TOKEN)
}
</script>

</body>
</html>
"""

@app.post("/register")
def register(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    if db.query(User).filter_by(username=username).first():
        return {"msg":"المستخدم موجود"}
    db.add(User(username=username, password=password))
    db.commit()
    return {"msg":"تم"}

@app.post("/login")
def login(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    user = db.query(User).filter_by(username=username, password=password).first()
    if not user:
        return {"msg":"خطأ"}
    token=str(uuid.uuid4())
    tokens[token]=user.username
    return {"token":token,"username":user.username}

@app.post("/analyze")
def analyze_api(token: str = Form(...),
file1: UploadFile = File(...),
file2: UploadFile = File(...),
b1: str = Form(...),
b2: str = Form(...)):

    check_auth(token)

    d1=read(file1.file,b1)
    d2=read(file2.file,b2)

    errors,counts=analyze(d1,d2)

    global last_errors
    last_errors=errors

    totals = {
        b1: len(d1),
        b2: len(d2)
    }

    return {"errors":errors,"counts":counts,"totals":totals}

@app.get("/download")
def download(token: str):
    check_auth(token)
    df=pd.DataFrame(last_errors)
    name=f"report_{uuid.uuid4().hex}.xlsx"
    df.to_excel(name,index=False)
    return FileResponse(name,filename="report.xlsx")