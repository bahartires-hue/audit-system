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

# ================= DETECT =================
def detect_columns(df):
    df.columns = df.columns.astype(str).str.strip()

    debit_col = None
    credit_col = None
    date_col = None

    for col in df.columns:
        name = str(col).lower()

        if not debit_col and any(x in name for x in ["مدين","debit","dr"]):
            debit_col = col
        if not credit_col and any(x in name for x in ["دائن","credit","cr"]):
            credit_col = col
        if not date_col and any(x in name for x in ["تاريخ","date"]):
            date_col = col

    numeric_scores = {c: pd.to_numeric(df[c], errors='coerce').notna().sum() for c in df.columns}
    sorted_cols = sorted(numeric_scores, key=numeric_scores.get, reverse=True)

    if not debit_col and sorted_cols:
        debit_col = sorted_cols[0]
    if not credit_col and len(sorted_cols) > 1:
        credit_col = sorted_cols[1]

    if not date_col:
        for col in df.columns:
            if pd.to_datetime(df[col], errors='coerce').notna().sum() > len(df)*0.5:
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
            for table in page.extract_tables():
                for row in table:
                    rows.append(row)

    df=pd.DataFrame(rows)
    df=df.dropna(how="all")
    df.columns=df.iloc[0]
    return df[1:]

def read_any(file, filename):
    if filename.endswith((".xlsx",".xls")):
        return read_excel(file)
    elif filename.endswith(".pdf"):
        return read_pdf(file)
    else:
        raise Exception("نوع الملف غير مدعوم")

# ================= PROCESS =================
def process(file, filename, branch):
    df = read_any(file, filename)

    if df is None or df.empty:
        return []

    df.columns = df.columns.astype(str).str.strip()

    debit_col, credit_col, date_col = detect_columns(df)

    doc_col = None
    for col in df.columns:
        if any(x in str(col).lower() for x in ["مستند","doc","نوع","بيان","الوصف"]):
            doc_col = col
            break

    data = []

    for _, row in df.iterrows():

        debit  = safe(row[debit_col]) if debit_col in df.columns else 0
        credit = safe(row[credit_col]) if credit_col in df.columns else 0

        if debit == 0 and credit == 0:
            continue

        if debit > 0 and credit > 0:
            data.append({
                "amount": max(debit, credit),
                "type": "error",
                "branch": branch,
                "date": None,
                "doc": "",
                "reason": "خطأ: مدين + دائن"
            })
            continue

        t = "credit" if credit > 0 else "debit"
        amount = credit if credit > 0 else debit

        date=None
        if date_col:
            d = pd.to_datetime(row[date_col], errors='coerce')
            if not pd.isna(d):
                date = d.strftime("%Y-%m-%d")

        doc = str(row[doc_col]).strip() if doc_col else ""

        data.append({
            "amount": float(amount),
            "type": t,
            "branch": branch,
            "date": date,
            "doc": doc
        })

    return data

# ================= MATCH =================
def clean(s):
    return str(s).lower().replace(" ","").replace("-","")

doc_map = {
    "مبيعات":"مشتريات",
    "مشتريات":"مبيعات"
}

def match_doc(d1,d2):
    d1,d2=clean(d1),clean(d2)
    for k,v in doc_map.items():
        if k in d1 and v in d2:
            return True
    return False

def date_diff(d1,d2):
    try:
        return abs((pd.to_datetime(d1)-pd.to_datetime(d2)).days)
    except:
        return None

# ================= ANALYZE =================
def analyze(d1,d2):
    res=[]
    used=[False]*len(d2)
    counts={}

    for x1 in d1:

        if x1.get("type")=="error":
            res.append(x1)
            counts[x1["branch"]] = counts.get(x1["branch"],0)+1
            continue

        matched=False

        for i,x2 in enumerate(d2):
            if used[i]: continue
            if x2.get("type")=="error": continue

            if x1["type"]==x2["type"]:
                continue

            if abs(x1["amount"]-x2["amount"])>1:
                continue

            if not match_doc(x1["doc"],x2["doc"]):
                continue

            days=date_diff(x1["date"],x2["date"])
            if days is None or days<=2:
                used[i]=True
                matched=True
                break

        if not matched:
            x1["reason"]="لا يوجد مقابل"
            res.append(x1)
            counts[x1["branch"]] = counts.get(x1["branch"],0)+1

    for i,x in enumerate(d2):
        if not used[i]:
            x["reason"]="لا يوجد مقابل"
            res.append(x)
            counts[x["branch"]] = counts.get(x["branch"],0)+1

    return res,counts

# ================= FRONT =================
@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html dir="rtl">
<body>

<input id="b1" placeholder="فرع1">
<input id="b2" placeholder="فرع2">
<input type="file" id="f1">
<input type="file" id="f2">
<button onclick="upload()">تحليل</button>

<div id="out"></div>

<script>
async function upload(){

let f=new FormData()
f.append("file1",f1.files[0])
f.append("file2",f2.files[0])
f.append("b1",b1.value)
f.append("b2",b2.value)

let r=await fetch("/analyze",{method:"POST",body:f})

let d=await r.json()

let html=""

;(d.errors||[]).forEach(x=>{
html+=`<div>
${x.branch} | ${x.amount} | ${x.doc} | ${x.reason}
</div>`
})

out.innerHTML=html
}
</script>

</body>
</html>
"""

# ================= API =================
last_errors=[]

@app.post("/analyze")
def analyze_api(
file1: UploadFile = File(...),
file2: UploadFile = File(...),
b1: str = Form(...),
b2: str = Form(...)
):
    try:
        file1.file.seek(0)
        file2.file.seek(0)

        d1=process(file1.file,file1.filename,b1)
        d2=process(file2.file,file2.filename,b2)

        errors,counts=analyze(d1,d2)

        global last_errors
        last_errors=errors

        return {"errors":errors,"counts":counts}

    except Exception as e:
        return {"error":str(e)}

@app.get("/download")
def download():
    df=pd.DataFrame(last_errors)
    name=f"{uuid.uuid4().hex}.xlsx"
    df.to_excel(name,index=False)
    return FileResponse(name,filename="report.xlsx")
