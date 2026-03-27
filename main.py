from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base, Session
import pandas as pd
import uuid

app = FastAPI()

# 🔥 PostgreSQL بدل SQLite
DATABASE_URL = "postgresql://app_user:sL2xxFyLUVcQqJQ01Li9SY35QLoMqvM3@dpg-d73a0slm5p6s73e56bg0-a.oregon-postgres.render.com/app_db_mf72"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"sslmode": "require"}
)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    password = Column(String)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

tokens = {}
last_errors = []

def check_auth(token: str):
    if token not in tokens:
        raise HTTPException(401, "غير مصرح")

def safe(v):
    try:
        return round(float(v), 2)
    except:
        return 0

def read(file, branch):
    df = pd.read_excel(file)
    df.columns = df.columns.str.strip()
    data = []
    for _, row in df.iterrows():
        debit = safe(row.get("مدين_8", 0))
        credit = safe(row.get("دائن_9", 0))
        date = str(row.get("التأريخ_7", ""))
        doc = str(row.get("المستند_4", ""))

        if debit > 0:
            data.append({"amount": debit, "branch": branch, "date": date, "doc": doc})
        elif credit > 0:
            data.append({"amount": credit, "branch": branch, "date": date, "doc": doc})
    return data

def analyze(d1, d2):
    res = []
    used = [False] * len(d2)
    counts = {}

    for x1 in d1:
        found = False
        for i, x2 in enumerate(d2):
            if used[i]:
                continue
            if abs(x1["amount"] - x2["amount"]) <= 0.05:
                used[i] = True
                found = True
                break
        if not found:
            res.append(x1)
            counts[x1["branch"]] = counts.get(x1["branch"], 0) + 1

    for i, x in enumerate(d2):
        if not used[i]:
            res.append(x)
            counts[x["branch"]] = counts.get(x["branch"], 0) + 1

    return res, counts

@app.get("/", response_class=HTMLResponse)
def home():
    return "<h1>النظام شغال ✅</h1>"

@app.post("/register")
def register(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    if db.query(User).filter_by(username=username).first():
        return {"msg": "المستخدم موجود"}
    db.add(User(username=username, password=password))
    db.commit()
    return {"msg": "تم"}

@app.post("/login")
def login(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    user = db.query(User).filter_by(username=username, password=password).first()
    if not user:
        return {"msg": "خطأ"}
    token = str(uuid.uuid4())
    tokens[token] = user.username
    return {"token": token, "username": user.username}

@app.post("/analyze")
def analyze_api(
    token: str = Form(...),
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    b1: str = Form(...),
    b2: str = Form(...)
):
    check_auth(token)

    d1 = read(file1.file, b1)
    d2 = read(file2.file, b2)

    errors, counts = analyze(d1, d2)

    global last_errors
    last_errors = errors

    totals = {
        b1: len(d1),
        b2: len(d2)
    }

    return {"errors": errors, "counts": counts, "totals": totals}

@app.get("/download")
def download(token: str):
    check_auth(token)
    df = pd.DataFrame(last_errors)
    name = f"report_{uuid.uuid4().hex}.xlsx"
    df.to_excel(name, index=False)
    return FileResponse(name, filename="report.xlsx")
