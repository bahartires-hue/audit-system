from __future__ import annotations

import datetime as dt
import io
import os as _os
import uuid
from urllib.parse import quote as _quote
from pathlib import Path as _Path
from typing import Optional

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from pydantic import BaseModel, Field

from ..auth_core import log_event, require_csrf, require_user
from ..db import SessionLocal
from ..models import (
    Employee,
    EmployeeContract,
    EmployeeDocument,
    EmployeeDocumentRenewal,
    Advance,
    DeductionRecord,
    AllowanceRecord,
    CommissionRecord,
    LeaveRequest,
    Attendance,
    EndOfService,
    HRConfigItem,
    Payroll,
    PayrollAllowance,
    PayrollDeduction,
    AppSetting,
    CompanyProfile,
)
from ..pdf_utils import generate_pdf_report

router = APIRouter(prefix="/api/hr", tags=["hr"])

_BASE_DIR = _Path(__file__).resolve().parents[3]
_data_root = (_os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
_UPLOAD_DIR = (_Path(_data_root) / "uploads") if _data_root else (_BASE_DIR / "uploads")
_HR_DOCS_DIR = _UPLOAD_DIR / "hr_documents"
_HR_CONTRACTS_DIR = _UPLOAD_DIR / "hr_contracts"
_HR_PHOTOS_DIR = _UPLOAD_DIR / "hr_photos"
for _d in (_HR_DOCS_DIR, _HR_CONTRACTS_DIR, _HR_PHOTOS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

_ALLOWED_DOC_SUFFIXES = (".pdf", ".png", ".jpg", ".jpeg")
_ALLOWED_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp")
_MAX_DOC_MB = 10


def _parse_simple_date(s, field_name: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, f"{field_name} يجب أن يكون بصيغة YYYY-MM-DD")


def _fmt_date(d):
    return d.strftime("%Y-%m-%d") if d else ""


def _fmt_dt(d):
    return d.strftime("%Y-%m-%d %H:%M") if d else ""


def _days_left(expiry):
    if not expiry:
        return None
    today = dt.datetime.utcnow().date()
    exp = expiry.date() if hasattr(expiry, "date") else expiry
    return (exp - today).days


def _next_employee_number(db, user_id: str) -> str:
    count = db.query(Employee).filter(Employee.user_id == user_id).count()
    return f"EMP-{count + 1:04d}"


def _xlsx_response(headers, rows, filename):
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    ascii_fallback = "export.xlsx"
    disposition = f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{_quote(filename, safe='')}"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": disposition},
    )


def _pdf_meta(db, user):
    company = db.query(CompanyProfile).filter(CompanyProfile.id == "default").first()
    company_name = (company.trade_name or company.company_name) if company else None
    logo_url = company.logo_url if company else None
    generated_by = getattr(user, "username", None) or getattr(user, "email", None)
    return company_name, logo_url, generated_by


STATUS_LABELS = {
    "active": "نشط",
    "on_leave": "إجازة",
    "suspended": "موقوف",
    "resigned": "مستقيل",
    "terminated": "منتهي الخدمة",
}


# ============================================================
# الإعدادات (الأقسام / الوظائف / أنواع الإجازات / البدلات / الاستقطاعات / الفروع / الضريبة / العملة)
# ============================================================

class ConfigItemCreate(BaseModel):
    category: str
    name: str = Field(min_length=1, max_length=120)


@router.get("/config")
def list_config(request: Request, category: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(HRConfigItem).filter(HRConfigItem.user_id == user.id)
        if category:
            q = q.filter(HRConfigItem.category == category)
        rows = q.order_by(HRConfigItem.name.asc()).all()
        return {"items": [{"id": r.id, "category": r.category, "name": r.name} for r in rows]}
    finally:
        db.close()


@router.post("/config")
def create_config(request: Request, body: ConfigItemCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = HRConfigItem(id=uuid.uuid4().hex, user_id=user.id, category=body.category.strip(), name=body.name.strip())
        db.add(rec)
        db.commit()
        return {"id": rec.id, "category": rec.category, "name": rec.name}
    finally:
        db.close()


@router.delete("/config/{item_id}")
def delete_config(item_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(HRConfigItem).filter(HRConfigItem.user_id == user.id, HRConfigItem.id == item_id).first()
        if not rec:
            raise HTTPException(404, "العنصر غير موجود")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


class HRSettingsIn(BaseModel):
    tax_percentage: float = 0.0
    tax_enabled: bool = False
    currency: str = "SAR"


def _get_hr_settings(db) -> dict:
    row = db.query(AppSetting).filter(AppSetting.key == "hr.settings").first()
    if row and isinstance(row.value_json, dict):
        return {"tax_percentage": 0.0, "tax_enabled": False, "currency": "SAR", **row.value_json}
    return {"tax_percentage": 0.0, "tax_enabled": False, "currency": "SAR"}


@router.get("/settings")
def get_hr_settings(request: Request):
    db = SessionLocal()
    try:
        require_user(db, request)
        return _get_hr_settings(db)
    finally:
        db.close()


@router.patch("/settings")
def update_hr_settings(request: Request, body: HRSettingsIn = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        require_user(db, request)
        row = db.query(AppSetting).filter(AppSetting.key == "hr.settings").first()
        value = {"tax_percentage": body.tax_percentage, "tax_enabled": body.tax_enabled, "currency": body.currency}
        if row:
            row.value_json = value
            row.updated_at = dt.datetime.utcnow()
        else:
            row = AppSetting(key="hr.settings", value_json=value)
            db.add(row)
        db.commit()
        return value
    finally:
        db.close()



# ============================================================
# الموظفون
# ============================================================

class EmployeeCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    nationality: str = ""
    national_id: str = ""
    residency_expiry: str = ""
    passport_number: str = ""
    passport_expiry: str = ""
    position: str = ""
    department: str = ""
    branch_name: str = ""
    basic_salary: float = 0.0
    commission_percentage: Optional[float] = None
    hire_date: str = ""
    birth_date: str = ""
    phone: str = ""
    email: str = ""
    status: str = "active"
    notes: str = ""


class EmployeeUpdate(BaseModel):
    name: Optional[str] = None
    nationality: Optional[str] = None
    national_id: Optional[str] = None
    residency_expiry: Optional[str] = None
    passport_number: Optional[str] = None
    passport_expiry: Optional[str] = None
    position: Optional[str] = None
    department: Optional[str] = None
    branch_name: Optional[str] = None
    basic_salary: Optional[float] = None
    commission_percentage: Optional[float] = None
    hire_date: Optional[str] = None
    birth_date: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    status: Optional[str] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


def _employee_out(e: Employee) -> dict:
    residency_days = _days_left(e.residency_expiry)
    passport_days = _days_left(e.passport_expiry)
    return {
        "id": e.id,
        "employee_number": e.employee_number or "",
        "name": e.name,
        "nationality": e.nationality or "",
        "national_id": e.national_id or "",
        "residency_expiry": _fmt_date(e.residency_expiry),
        "residency_days_left": residency_days,
        "passport_number": e.passport_number or "",
        "passport_expiry": _fmt_date(e.passport_expiry),
        "passport_days_left": passport_days,
        "position": e.position or "",
        "department": e.department or "",
        "branch_name": e.branch_name or "",
        "basic_salary": round(float(e.basic_salary or 0.0), 2),
        "commission_percentage": e.commission_percentage,
        "hire_date": _fmt_date(e.hire_date),
        "birth_date": _fmt_date(e.birth_date),
        "phone": e.phone or "",
        "email": e.email or "",
        "status": e.status or "active",
        "status_label": STATUS_LABELS.get(e.status or "active", e.status or ""),
        "photo_url": e.photo_url or "",
        "is_active": bool(int(e.is_active or 0)),
        "notes": e.notes or "",
    }


@router.get("/employees")
def list_employees(request: Request, q: str = Query(""), department: str = Query(""), branch_name: str = Query(""), status: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id).order_by(Employee.created_at.desc()).all()
        qn = (q or "").strip().lower()
        if qn:
            rows = [r for r in rows if qn in (r.name or "").lower() or qn in (r.employee_number or "").lower()]
        if department:
            rows = [r for r in rows if (r.department or "") == department]
        if branch_name:
            rows = [r for r in rows if (r.branch_name or "") == branch_name]
        if status:
            rows = [r for r in rows if (r.status or "active") == status]
        out = []
        for r in rows:
            item = _employee_out(r)
            contract = (
                db.query(EmployeeContract)
                .filter(EmployeeContract.employee_id == r.id, EmployeeContract.status == "active")
                .order_by(EmployeeContract.created_at.desc())
                .first()
            )
            item["contract_status"] = "ساري" if contract else "لا يوجد"
            out.append(item)
        return {"items": out}
    finally:
        db.close()


@router.post("/employees")
def create_employee(request: Request, body: EmployeeCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = Employee(
            id=uuid.uuid4().hex,
            user_id=user.id,
            employee_number=_next_employee_number(db, user.id),
            name=body.name.strip(),
            nationality=(body.nationality or "").strip() or None,
            national_id=(body.national_id or "").strip() or None,
            residency_expiry=_parse_simple_date(body.residency_expiry, "تاريخ انتهاء الإقامة"),
            passport_number=(body.passport_number or "").strip() or None,
            passport_expiry=_parse_simple_date(body.passport_expiry, "تاريخ انتهاء الجواز"),
            position=(body.position or "").strip() or None,
            department=(body.department or "").strip() or None,
            branch_name=(body.branch_name or "").strip() or None,
            basic_salary=body.basic_salary or 0.0,
            commission_percentage=body.commission_percentage,
            hire_date=_parse_simple_date(body.hire_date, "تاريخ التعيين"),
            birth_date=_parse_simple_date(body.birth_date, "تاريخ الميلاد"),
            phone=(body.phone or "").strip() or None,
            email=(body.email or "").strip() or None,
            status=body.status or "active",
            is_active=1 if (body.status or "active") == "active" else 0,
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.commit()
        log_event(db, "hr.employee.create", user.id, {"employee_id": rec.id})
        return _employee_out(rec)
    finally:
        db.close()


EMPLOYEE_IMPORT_HEADERS = [
    "الرقم الوظيفي", "اسم الموظف", "الجنسية", "رقم الهوية/الإقامة",
    "تاريخ انتهاء الهوية/الإقامة", "رقم الجواز", "تاريخ انتهاء الجواز",
    "الجوال", "البريد الإلكتروني", "الوظيفة", "القسم", "الفرع",
    "الراتب الأساسي", "نسبة العمولة", "تاريخ التعيين", "الحالة", "ملاحظات",
]

_STATUS_LABEL_TO_VALUE = {v: k for k, v in STATUS_LABELS.items()}


def _cell_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, dt.datetime):
        return v.strftime("%Y-%m-%d")
    return str(v).strip()


def _parse_import_date(v, field_name: str):
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    if isinstance(v, dt.datetime):
        return v
    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"{field_name} بصيغة غير صحيحة (استخدم YYYY-MM-DD): {s}")


def _parse_import_status(v):
    s = _cell_str(v)
    if not s:
        return "active"
    if s in STATUS_LABELS:
        return s
    if s in _STATUS_LABEL_TO_VALUE:
        return _STATUS_LABEL_TO_VALUE[s]
    raise ValueError(f"قيمة الحالة غير معروفة: {s}")


@router.get("/employees/import-template")
def download_employees_import_template(request: Request):
    db = SessionLocal()
    try:
        require_user(db, request)
        example = [
            "EMP-0001", "محمد أحمد", "سعودي", "1012345678", "2027-01-01",
            "A1234567", "2029-05-01", "0501234567", "employee@example.com",
            "محاسب", "المالية", "الفرع الرئيسي", 6000, 2.5, "2024-01-01", "نشط", "",
        ]
        return _xlsx_response(EMPLOYEE_IMPORT_HEADERS, [example], "قالب_استيراد_الموظفين.xlsx")
    finally:
        db.close()


@router.get("/employees/export")
def export_employees(request: Request, q: str = Query(""), department: str = Query(""), branch_name: str = Query(""), status: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id).order_by(Employee.created_at.desc()).all()
        qn = (q or "").strip().lower()
        if qn:
            rows = [r for r in rows if qn in (r.name or "").lower() or qn in (r.employee_number or "").lower()]
        if department:
            rows = [r for r in rows if (r.department or "") == department]
        if branch_name:
            rows = [r for r in rows if (r.branch_name or "") == branch_name]
        if status:
            rows = [r for r in rows if (r.status or "active") == status]
        out_rows = []
        for e in rows:
            out_rows.append([
                e.employee_number or "",
                e.name or "",
                e.nationality or "",
                e.national_id or "",
                _fmt_date(e.residency_expiry),
                e.passport_number or "",
                _fmt_date(e.passport_expiry),
                e.phone or "",
                e.email or "",
                e.position or "",
                e.department or "",
                e.branch_name or "",
                round(float(e.basic_salary or 0.0), 2),
                e.commission_percentage,
                _fmt_date(e.hire_date),
                STATUS_LABELS.get(e.status or "active", e.status or ""),
                e.notes or "",
            ])
        log_event(db, "hr.employee.export", user.id, {"count": len(out_rows)})
        return _xlsx_response(EMPLOYEE_IMPORT_HEADERS, out_rows, "الموظفون.xlsx")
    finally:
        db.close()


@router.post("/employees/import")
async def import_employees(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        original = file.filename or "import.xlsx"
        if not original.lower().endswith((".xlsx", ".xlsm")):
            raise HTTPException(400, "يجب أن يكون الملف بصيغة Excel (.xlsx)")
        content = await file.read()
        if len(content) > 15 * 1024 * 1024:
            raise HTTPException(400, "حجم الملف أكبر من 15 ميجابايت")
        try:
            from openpyxl import load_workbook
            wb = load_workbook(io.BytesIO(content), data_only=True)
            ws = wb.active
        except Exception:
            raise HTTPException(400, "تعذر قراءة ملف Excel — تأكد أنه بصيغة .xlsx صحيحة")

        added = 0
        updated = 0
        errors = []
        row_num = 1
        for raw_row in ws.iter_rows(min_row=2, values_only=True):
            row_num += 1
            if raw_row is None or all(c is None or (isinstance(c, str) and not c.strip()) for c in raw_row):
                continue
            cells = list(raw_row) + [None] * max(0, 17 - len(raw_row))
            try:
                employee_number = _cell_str(cells[0])
                name = _cell_str(cells[1])
                if not name:
                    raise ValueError("اسم الموظف مطلوب")
                nationality = _cell_str(cells[2])
                national_id = _cell_str(cells[3])
                residency_expiry = _parse_import_date(cells[4], "تاريخ انتهاء الهوية/الإقامة")
                passport_number = _cell_str(cells[5])
                passport_expiry = _parse_import_date(cells[6], "تاريخ انتهاء الجواز")
                phone = _cell_str(cells[7])
                email = _cell_str(cells[8])
                position = _cell_str(cells[9])
                department = _cell_str(cells[10])
                branch_name = _cell_str(cells[11])
                salary_raw = cells[12]
                try:
                    basic_salary = float(salary_raw) if salary_raw not in (None, "") else 0.0
                except (TypeError, ValueError):
                    raise ValueError(f"الراتب الأساسي غير رقمي: {salary_raw}")
                comm_raw = cells[13]
                try:
                    commission_percentage = float(comm_raw) if comm_raw not in (None, "") else None
                except (TypeError, ValueError):
                    raise ValueError(f"نسبة العمولة غير رقمية: {comm_raw}")
                hire_date = _parse_import_date(cells[14], "تاريخ التعيين")
                status = _parse_import_status(cells[15])
                notes = _cell_str(cells[16])
            except ValueError as ve:
                errors.append({"row": row_num, "reason": str(ve)})
                continue

            existing = None
            if national_id:
                existing = db.query(Employee).filter(Employee.user_id == user.id, Employee.national_id == national_id).first()
            if not existing and employee_number:
                existing = db.query(Employee).filter(Employee.user_id == user.id, Employee.employee_number == employee_number).first()

            try:
                if existing:
                    existing.name = name
                    existing.nationality = nationality or None
                    existing.national_id = national_id or existing.national_id
                    existing.residency_expiry = residency_expiry
                    existing.passport_number = passport_number or None
                    existing.passport_expiry = passport_expiry
                    existing.position = position or None
                    existing.department = department or None
                    existing.branch_name = branch_name or None
                    existing.basic_salary = basic_salary
                    existing.commission_percentage = commission_percentage
                    existing.hire_date = hire_date
                    existing.phone = phone or None
                    existing.email = email or None
                    existing.status = status
                    existing.is_active = 1 if status == "active" else 0
                    existing.notes = notes or None
                    updated += 1
                else:
                    rec = Employee(
                        id=uuid.uuid4().hex,
                        user_id=user.id,
                        employee_number=employee_number or _next_employee_number(db, user.id),
                        name=name,
                        nationality=nationality or None,
                        national_id=national_id or None,
                        residency_expiry=residency_expiry,
                        passport_number=passport_number or None,
                        passport_expiry=passport_expiry,
                        position=position or None,
                        department=department or None,
                        branch_name=branch_name or None,
                        basic_salary=basic_salary,
                        commission_percentage=commission_percentage,
                        hire_date=hire_date,
                        phone=phone or None,
                        email=email or None,
                        status=status,
                        is_active=1 if status == "active" else 0,
                        notes=notes or None,
                    )
                    db.add(rec)
                    added += 1
                db.commit()
            except Exception as ex:
                db.rollback()
                errors.append({"row": row_num, "reason": f"تعذر الحفظ: {ex}"})

        log_event(db, "hr.employee.import", user.id, {"added": added, "updated": updated, "errors": len(errors)})
        return {"added": added, "updated": updated, "errors": errors, "total_rows": row_num - 1}
    finally:
        db.close()



@router.get("/employees/{employee_id}")
def get_employee(employee_id: str, request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rec = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not rec:
            raise HTTPException(404, "الموظف غير موجود")
        return _employee_out(rec)
    finally:
        db.close()


@router.patch("/employees/{employee_id}")
def update_employee(employee_id: str, request: Request, body: EmployeeUpdate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not rec:
            raise HTTPException(404, "الموظف غير موجود")
        data = body.dict(exclude_unset=True)
        simple_fields = ["name", "nationality", "national_id", "passport_number", "position", "department", "branch_name", "phone", "email", "notes"]
        for f in simple_fields:
            if f in data and data[f] is not None:
                setattr(rec, f, (data[f] or "").strip() or None)
        if "basic_salary" in data and data["basic_salary"] is not None:
            rec.basic_salary = data["basic_salary"]
        if "commission_percentage" in data:
            rec.commission_percentage = data["commission_percentage"]
        if "residency_expiry" in data:
            rec.residency_expiry = _parse_simple_date(data["residency_expiry"], "تاريخ انتهاء الإقامة")
        if "passport_expiry" in data:
            rec.passport_expiry = _parse_simple_date(data["passport_expiry"], "تاريخ انتهاء الجواز")
        if "hire_date" in data:
            rec.hire_date = _parse_simple_date(data["hire_date"], "تاريخ التعيين")
        if "birth_date" in data:
            rec.birth_date = _parse_simple_date(data["birth_date"], "تاريخ الميلاد")
        if "status" in data and data["status"]:
            rec.status = data["status"]
            rec.is_active = 1 if data["status"] == "active" else 0
        if "is_active" in data and data["is_active"] is not None:
            rec.is_active = 1 if data["is_active"] else 0
        db.commit()
        log_event(db, "hr.employee.update", user.id, {"employee_id": rec.id})
        return _employee_out(rec)
    finally:
        db.close()


@router.delete("/employees/{employee_id}")
def delete_employee(employee_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not rec:
            raise HTTPException(404, "الموظف غير موجود")
        has_payroll = db.query(Payroll).filter(Payroll.employee_id == employee_id).first()
        if has_payroll:
            raise HTTPException(400, "لا يمكن حذف موظف لديه سجل رواتب — يمكنك تغيير حالته إلى (منتهي الخدمة) بدلاً من الحذف")
        db.delete(rec)
        db.commit()
        log_event(db, "hr.employee.delete", user.id, {"employee_id": employee_id})
        return {"deleted": True}
    finally:
        db.close()


@router.post("/employees/{employee_id}/photo")
async def upload_employee_photo(employee_id: str, request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not rec:
            raise HTTPException(404, "الموظف غير موجود")
        original = file.filename or "photo"
        lower = original.lower()
        if not any(lower.endswith(s) for s in _ALLOWED_IMAGE_SUFFIXES):
            raise HTTPException(400, "صيغة الصورة يجب أن تكون PNG أو JPG أو WEBP")
        content = await file.read()
        if len(content) > _MAX_DOC_MB * 1024 * 1024:
            raise HTTPException(400, f"حجم الصورة أكبر من {_MAX_DOC_MB} ميجابايت")
        suffix = _Path(original).suffix.lower() or ".jpg"
        saved_name = f"{uuid.uuid4().hex}{suffix}"
        with open(_HR_PHOTOS_DIR / saved_name, "wb") as f:
            f.write(content)
        rec.photo_url = f"/uploads/hr_photos/{saved_name}"
        db.commit()
        return _employee_out(rec)
    finally:
        db.close()



# ============================================================
# العقود
# ============================================================

def _contract_out(c: EmployeeContract) -> dict:
    return {
        "id": c.id,
        "employee_id": c.employee_id,
        "contract_type": c.contract_type or "",
        "start_date": _fmt_date(c.start_date),
        "end_date": _fmt_date(c.end_date),
        "end_days_left": _days_left(c.end_date),
        "salary": round(float(c.salary or 0.0), 2),
        "housing_allowance": round(float(c.housing_allowance or 0.0), 2),
        "transport_allowance": round(float(c.transport_allowance or 0.0), 2),
        "other_allowances": round(float(c.other_allowances or 0.0), 2),
        "probation_days": c.probation_days,
        "annual_leave_days": c.annual_leave_days,
        "file_url": c.file_url or "",
        "status": c.status,
        "created_at": _fmt_dt(c.created_at),
    }


@router.get("/contracts")
def list_contracts(request: Request, employee_id: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(EmployeeContract, Employee).join(Employee, Employee.id == EmployeeContract.employee_id).filter(EmployeeContract.user_id == user.id)
        if employee_id:
            q = q.filter(EmployeeContract.employee_id == employee_id)
        rows = q.order_by(EmployeeContract.created_at.desc()).all()
        items = []
        for c, e in rows:
            out = _contract_out(c)
            out["employee_name"] = e.name
            items.append(out)
        return {"items": items}
    finally:
        db.close()


@router.post("/employees/{employee_id}/contracts")
async def create_contract(
    employee_id: str,
    request: Request,
    contract_type: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    salary: float = Form(0.0),
    housing_allowance: float = Form(0.0),
    transport_allowance: float = Form(0.0),
    other_allowances: float = Form(0.0),
    probation_days: int = Form(0),
    annual_leave_days: int = Form(21),
    file: Optional[UploadFile] = File(None),
):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        file_url = None
        if file is not None and getattr(file, "filename", ""):
            original = file.filename or "contract"
            lower = original.lower()
            if not any(lower.endswith(s) for s in _ALLOWED_DOC_SUFFIXES):
                raise HTTPException(400, "صيغة الملف يجب أن تكون PDF أو JPG أو PNG")
            content = await file.read()
            if len(content) > _MAX_DOC_MB * 1024 * 1024:
                raise HTTPException(400, f"حجم الملف أكبر من {_MAX_DOC_MB} ميجابايت")
            suffix = _Path(original).suffix.lower() or ".pdf"
            saved_name = f"{uuid.uuid4().hex}{suffix}"
            with open(_HR_CONTRACTS_DIR / saved_name, "wb") as f:
                f.write(content)
            file_url = f"/uploads/hr_contracts/{saved_name}"

        # عقد جديد ينهي أي عقد سابق ساري لنفس الموظف
        db.query(EmployeeContract).filter(
            EmployeeContract.employee_id == employee_id, EmployeeContract.status == "active"
        ).update({"status": "ended"})

        rec = EmployeeContract(
            id=uuid.uuid4().hex,
            user_id=user.id,
            employee_id=employee_id,
            contract_type=(contract_type or "").strip() or None,
            start_date=_parse_simple_date(start_date, "بداية العقد"),
            end_date=_parse_simple_date(end_date, "نهاية العقد"),
            salary=salary or 0.0,
            housing_allowance=housing_allowance or 0.0,
            transport_allowance=transport_allowance or 0.0,
            other_allowances=other_allowances or 0.0,
            probation_days=probation_days or 0,
            annual_leave_days=annual_leave_days or 21,
            file_url=file_url,
            status="active",
        )
        db.add(rec)

        # ترابط تلقائي: بدلات العقد تتحول لبدلات شهرية متكررة على الموظف دون إدخال يدوي
        db.query(AllowanceRecord).filter(
            AllowanceRecord.employee_id == employee_id, AllowanceRecord.recurring == 1, AllowanceRecord.applied == 0
        ).delete()
        for label, amount in (("بدل سكن (من العقد)", rec.housing_allowance), ("بدل نقل (من العقد)", rec.transport_allowance), ("بدلات أخرى (من العقد)", rec.other_allowances)):
            if amount and amount > 0:
                db.add(AllowanceRecord(
                    id=uuid.uuid4().hex, user_id=user.id, employee_id=employee_id,
                    allowance_type=label, amount=amount, recurring=1, date=dt.datetime.utcnow(), applied=0,
                ))

        if employee.basic_salary in (None, 0) and rec.salary:
            employee.basic_salary = rec.salary

        db.commit()
        log_event(db, "hr.contract.create", user.id, {"contract_id": rec.id, "employee_id": employee_id})
        return _contract_out(rec)
    finally:
        db.close()


@router.delete("/contracts/{contract_id}")
def delete_contract(contract_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(EmployeeContract).filter(EmployeeContract.user_id == user.id, EmployeeContract.id == contract_id).first()
        if not rec:
            raise HTTPException(404, "العقد غير موجود")
        has_payroll = db.query(Payroll).filter(Payroll.employee_id == rec.employee_id).first()
        if has_payroll:
            raise HTTPException(400, "لا يمكن حذف العقد لأن الموظف لديه سجل رواتب")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()



# ============================================================
# السلف
# ============================================================

class AdvanceCreate(BaseModel):
    employee_id: str
    date: str = ""
    amount: float = Field(gt=0)
    reason: str = ""
    installments_count: int = Field(default=1, ge=1)


def _advance_out(a: Advance, employee_name: str = "") -> dict:
    return {
        "id": a.id, "employee_id": a.employee_id, "employee_name": employee_name,
        "date": _fmt_date(a.date), "amount": round(float(a.amount or 0.0), 2),
        "reason": a.reason or "", "installments_count": a.installments_count,
        "installment_amount": round(float(a.installment_amount or 0.0), 2),
        "remaining_amount": round(float(a.remaining_amount or 0.0), 2),
        "status": a.status,
    }


@router.get("/advances")
def list_advances(request: Request, employee_id: str = Query(""), status: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Advance, Employee).join(Employee, Employee.id == Advance.employee_id).filter(Advance.user_id == user.id)
        if employee_id:
            q = q.filter(Advance.employee_id == employee_id)
        if status:
            q = q.filter(Advance.status == status)
        rows = q.order_by(Advance.created_at.desc()).all()
        return {"items": [_advance_out(a, e.name) for a, e in rows]}
    finally:
        db.close()


@router.post("/advances")
def create_advance(request: Request, body: AdvanceCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == body.employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        installment_amount = round(body.amount / max(body.installments_count, 1), 2)
        rec = Advance(
            id=uuid.uuid4().hex, user_id=user.id, employee_id=body.employee_id,
            date=_parse_simple_date(body.date, "التاريخ") or dt.datetime.utcnow(),
            amount=body.amount, reason=(body.reason or "").strip() or None,
            installments_count=body.installments_count, installment_amount=installment_amount,
            remaining_amount=body.amount, status="active",
        )
        db.add(rec)
        db.commit()
        log_event(db, "hr.advance.create", user.id, {"advance_id": rec.id})
        return _advance_out(rec, employee.name)
    finally:
        db.close()


@router.delete("/advances/{advance_id}")
def delete_advance(advance_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(Advance).filter(Advance.user_id == user.id, Advance.id == advance_id).first()
        if not rec:
            raise HTTPException(404, "السلفة غير موجودة")
        if rec.remaining_amount < rec.amount:
            raise HTTPException(400, "لا يمكن حذف سلفة تم خصم جزء منها بالفعل — يمكن إلغاؤها فقط")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


@router.post("/advances/{advance_id}/cancel")
def cancel_advance(advance_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(Advance).filter(Advance.user_id == user.id, Advance.id == advance_id).first()
        if not rec:
            raise HTTPException(404, "السلفة غير موجودة")
        rec.status = "cancelled"
        db.commit()
        return _advance_out(rec)
    finally:
        db.close()


# ============================================================
# الاستقطاعات
# ============================================================

class DeductionCreate(BaseModel):
    employee_id: str
    deduction_type: str = ""
    amount: float = Field(gt=0)
    reason: str = ""
    date: str = ""


def _deduction_out(d: DeductionRecord, employee_name: str = "") -> dict:
    return {
        "id": d.id, "employee_id": d.employee_id, "employee_name": employee_name,
        "deduction_type": d.deduction_type or "", "amount": round(float(d.amount or 0.0), 2),
        "reason": d.reason or "", "date": _fmt_date(d.date),
        "applied": bool(d.applied), "payroll_id": d.payroll_id,
    }


@router.get("/deductions")
def list_deductions(request: Request, employee_id: str = Query(""), applied: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(DeductionRecord, Employee).join(Employee, Employee.id == DeductionRecord.employee_id).filter(DeductionRecord.user_id == user.id)
        if employee_id:
            q = q.filter(DeductionRecord.employee_id == employee_id)
        if applied == "0":
            q = q.filter(DeductionRecord.applied == 0)
        elif applied == "1":
            q = q.filter(DeductionRecord.applied == 1)
        rows = q.order_by(DeductionRecord.created_at.desc()).all()
        return {"items": [_deduction_out(d, e.name) for d, e in rows]}
    finally:
        db.close()


@router.post("/deductions")
def create_deduction(request: Request, body: DeductionCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == body.employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        rec = DeductionRecord(
            id=uuid.uuid4().hex, user_id=user.id, employee_id=body.employee_id,
            deduction_type=(body.deduction_type or "").strip() or None, amount=body.amount,
            reason=(body.reason or "").strip() or None,
            date=_parse_simple_date(body.date, "التاريخ") or dt.datetime.utcnow(), applied=0,
        )
        db.add(rec)
        db.commit()
        log_event(db, "hr.deduction.create", user.id, {"deduction_id": rec.id})
        return _deduction_out(rec, employee.name)
    finally:
        db.close()


@router.delete("/deductions/{deduction_id}")
def delete_deduction(deduction_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(DeductionRecord).filter(DeductionRecord.user_id == user.id, DeductionRecord.id == deduction_id).first()
        if not rec:
            raise HTTPException(404, "الاستقطاع غير موجود")
        if rec.applied:
            raise HTTPException(400, "لا يمكن حذف استقطاع تم تطبيقه على راتب بالفعل")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


# ============================================================
# البدلات
# ============================================================

class AllowanceCreate(BaseModel):
    employee_id: str
    allowance_type: str = ""
    amount: float = Field(gt=0)
    recurring: bool = False
    date: str = ""


def _allowance_out(a: AllowanceRecord, employee_name: str = "") -> dict:
    return {
        "id": a.id, "employee_id": a.employee_id, "employee_name": employee_name,
        "allowance_type": a.allowance_type or "", "amount": round(float(a.amount or 0.0), 2),
        "recurring": bool(a.recurring), "date": _fmt_date(a.date),
        "applied": bool(a.applied), "payroll_id": a.payroll_id,
    }


@router.get("/allowances")
def list_allowances(request: Request, employee_id: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(AllowanceRecord, Employee).join(Employee, Employee.id == AllowanceRecord.employee_id).filter(AllowanceRecord.user_id == user.id)
        if employee_id:
            q = q.filter(AllowanceRecord.employee_id == employee_id)
        rows = q.order_by(AllowanceRecord.created_at.desc()).all()
        return {"items": [_allowance_out(a, e.name) for a, e in rows]}
    finally:
        db.close()


@router.post("/allowances")
def create_allowance(request: Request, body: AllowanceCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == body.employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        rec = AllowanceRecord(
            id=uuid.uuid4().hex, user_id=user.id, employee_id=body.employee_id,
            allowance_type=(body.allowance_type or "").strip() or None, amount=body.amount,
            recurring=1 if body.recurring else 0,
            date=_parse_simple_date(body.date, "التاريخ") or dt.datetime.utcnow(), applied=0,
        )
        db.add(rec)
        db.commit()
        log_event(db, "hr.allowance.create", user.id, {"allowance_id": rec.id})
        return _allowance_out(rec, employee.name)
    finally:
        db.close()


@router.delete("/allowances/{allowance_id}")
def delete_allowance(allowance_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(AllowanceRecord).filter(AllowanceRecord.user_id == user.id, AllowanceRecord.id == allowance_id).first()
        if not rec:
            raise HTTPException(404, "البدل غير موجود")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


# ============================================================
# العمولات
# ============================================================

class CommissionCreate(BaseModel):
    employee_id: str
    sales_amount: float = Field(ge=0)
    percentage: Optional[float] = None
    date: str = ""


def _commission_out(c: CommissionRecord, employee_name: str = "") -> dict:
    return {
        "id": c.id, "employee_id": c.employee_id, "employee_name": employee_name,
        "sales_amount": round(float(c.sales_amount or 0.0), 2), "percentage": round(float(c.percentage or 0.0), 2),
        "commission_value": round(float(c.commission_value or 0.0), 2), "date": _fmt_date(c.date),
        "applied": bool(c.applied), "payroll_id": c.payroll_id,
    }


@router.get("/commissions")
def list_commissions(request: Request, employee_id: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(CommissionRecord, Employee).join(Employee, Employee.id == CommissionRecord.employee_id).filter(CommissionRecord.user_id == user.id)
        if employee_id:
            q = q.filter(CommissionRecord.employee_id == employee_id)
        rows = q.order_by(CommissionRecord.created_at.desc()).all()
        return {"items": [_commission_out(c, e.name) for c, e in rows]}
    finally:
        db.close()


@router.post("/commissions")
def create_commission(request: Request, body: CommissionCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == body.employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        pct = body.percentage if body.percentage is not None else (employee.commission_percentage or 0.0)
        value = round(body.sales_amount * (pct or 0.0) / 100.0, 2)
        rec = CommissionRecord(
            id=uuid.uuid4().hex, user_id=user.id, employee_id=body.employee_id,
            sales_amount=body.sales_amount, percentage=pct or 0.0, commission_value=value,
            date=_parse_simple_date(body.date, "التاريخ") or dt.datetime.utcnow(), applied=0,
        )
        db.add(rec)
        db.commit()
        log_event(db, "hr.commission.create", user.id, {"commission_id": rec.id})
        return _commission_out(rec, employee.name)
    finally:
        db.close()


@router.delete("/commissions/{commission_id}")
def delete_commission(commission_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(CommissionRecord).filter(CommissionRecord.user_id == user.id, CommissionRecord.id == commission_id).first()
        if not rec:
            raise HTTPException(404, "العمولة غير موجودة")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()



# ============================================================
# الإجازات
# ============================================================

class LeaveCreate(BaseModel):
    employee_id: str
    leave_type: str = ""
    start_date: str
    end_date: str
    paid: bool = True


def _leave_out(l: LeaveRequest, employee_name: str = "") -> dict:
    return {
        "id": l.id, "employee_id": l.employee_id, "employee_name": employee_name,
        "leave_type": l.leave_type or "", "start_date": _fmt_date(l.start_date), "end_date": _fmt_date(l.end_date),
        "days_count": l.days_count, "paid": bool(l.paid), "status": l.status,
        "status_label": {"pending": "بانتظار الموافقة", "accepted": "مقبولة", "rejected": "مرفوضة"}.get(l.status, l.status),
        "applied": bool(l.applied),
    }


@router.get("/leaves")
def list_leaves(request: Request, employee_id: str = Query(""), status: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(LeaveRequest, Employee).join(Employee, Employee.id == LeaveRequest.employee_id).filter(LeaveRequest.user_id == user.id)
        if employee_id:
            q = q.filter(LeaveRequest.employee_id == employee_id)
        if status:
            q = q.filter(LeaveRequest.status == status)
        rows = q.order_by(LeaveRequest.created_at.desc()).all()
        return {"items": [_leave_out(l, e.name) for l, e in rows]}
    finally:
        db.close()


@router.post("/leaves")
def create_leave(request: Request, body: LeaveCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == body.employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        start = _parse_simple_date(body.start_date, "من تاريخ")
        end = _parse_simple_date(body.end_date, "إلى تاريخ")
        if not start or not end or end < start:
            raise HTTPException(400, "التواريخ غير صحيحة")
        days = (end - start).days + 1
        rec = LeaveRequest(
            id=uuid.uuid4().hex, user_id=user.id, employee_id=body.employee_id,
            leave_type=(body.leave_type or "").strip() or None, start_date=start, end_date=end,
            days_count=days, paid=1 if body.paid else 0, status="pending",
        )
        db.add(rec)
        db.commit()
        log_event(db, "hr.leave.create", user.id, {"leave_id": rec.id})
        return _leave_out(rec, employee.name)
    finally:
        db.close()


@router.post("/leaves/{leave_id}/decision")
def decide_leave(leave_id: str, request: Request, decision: str = Body(..., embed=True)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(LeaveRequest).filter(LeaveRequest.user_id == user.id, LeaveRequest.id == leave_id).first()
        if not rec:
            raise HTTPException(404, "طلب الإجازة غير موجود")
        if decision not in ("accepted", "rejected"):
            raise HTTPException(400, "قرار غير صالح")
        rec.status = decision
        db.commit()
        return _leave_out(rec)
    finally:
        db.close()


@router.delete("/leaves/{leave_id}")
def delete_leave(leave_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(LeaveRequest).filter(LeaveRequest.user_id == user.id, LeaveRequest.id == leave_id).first()
        if not rec:
            raise HTTPException(404, "طلب الإجازة غير موجود")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


# ============================================================
# الحضور والانصراف
# ============================================================

class AttendanceCreate(BaseModel):
    employee_id: str
    date: str
    check_in: str = ""
    check_out: str = ""
    late_minutes: int = 0
    overtime_minutes: int = 0
    is_absent: bool = False
    notes: str = ""


def _attendance_out(a: Attendance, employee_name: str = "") -> dict:
    return {
        "id": a.id, "employee_id": a.employee_id, "employee_name": employee_name,
        "date": _fmt_date(a.date), "check_in": a.check_in or "", "check_out": a.check_out or "",
        "late_minutes": a.late_minutes, "overtime_minutes": a.overtime_minutes,
        "is_absent": bool(a.is_absent), "notes": a.notes or "",
    }


@router.get("/attendance")
def list_attendance(request: Request, employee_id: str = Query(""), month: int = Query(0), year: int = Query(0)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Attendance, Employee).join(Employee, Employee.id == Attendance.employee_id).filter(Attendance.user_id == user.id)
        if employee_id:
            q = q.filter(Attendance.employee_id == employee_id)
        rows = q.order_by(Attendance.date.desc()).all()
        if month:
            rows = [r for r in rows if r[0].date and r[0].date.month == month]
        if year:
            rows = [r for r in rows if r[0].date and r[0].date.year == year]
        return {"items": [_attendance_out(a, e.name) for a, e in rows]}
    finally:
        db.close()


@router.post("/attendance")
def create_attendance(request: Request, body: AttendanceCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == body.employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        day = _parse_simple_date(body.date, "التاريخ")
        rec = Attendance(
            id=uuid.uuid4().hex, user_id=user.id, employee_id=body.employee_id, date=day,
            check_in=(body.check_in or "").strip() or None, check_out=(body.check_out or "").strip() or None,
            late_minutes=body.late_minutes or 0, overtime_minutes=body.overtime_minutes or 0,
            is_absent=1 if body.is_absent else 0, notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.commit()
        return _attendance_out(rec, employee.name)
    finally:
        db.close()


@router.delete("/attendance/{attendance_id}")
def delete_attendance(attendance_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(Attendance).filter(Attendance.user_id == user.id, Attendance.id == attendance_id).first()
        if not rec:
            raise HTTPException(404, "السجل غير موجود")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()



# ============================================================
# الرواتب (مترابطة تلقائياً مع السلف/الاستقطاعات/البدلات/العمولات/الحضور/الإجازات)
# ============================================================

class LineItemIn(BaseModel):
    label: str = Field(min_length=1, max_length=120)
    amount: float = Field(ge=0)


class PayrollCreate(BaseModel):
    employee_id: str
    month: int = Field(ge=1, le=12)
    year: int = Field(ge=2000, le=2100)
    base_salary: Optional[float] = None
    extra_allowances: list[LineItemIn] = []
    extra_deductions: list[LineItemIn] = []
    notes: str = ""


def _payroll_out(db, p: Payroll, employee_name: str = "") -> dict:
    allowances = db.query(PayrollAllowance).filter(PayrollAllowance.payroll_id == p.id).all()
    deductions = db.query(PayrollDeduction).filter(PayrollDeduction.payroll_id == p.id).all()
    return {
        "id": p.id, "employee_id": p.employee_id, "employee_name": employee_name,
        "month": p.month, "year": p.year,
        "base_salary": round(float(p.base_salary or 0.0), 2),
        "total_allowances": round(float(p.total_allowances or 0.0), 2),
        "total_deductions": round(float(p.total_deductions or 0.0), 2),
        "gross_before_tax": round(float(p.gross_before_tax or 0.0), 2),
        "tax_amount": round(float(p.tax_amount or 0.0), 2),
        "net_salary": round(float(p.net_salary or 0.0), 2),
        "status": p.status,
        "paid_at": _fmt_dt(p.paid_at),
        "notes": p.notes or "",
        "allowances": [{"label": a.label, "amount": a.amount} for a in allowances],
        "deductions": [{"label": d.label, "amount": d.amount} for d in deductions],
    }


@router.get("/payrolls")
def list_payrolls(request: Request, month: int = Query(0), year: int = Query(0), status: str = Query(""), employee_id: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Payroll, Employee).join(Employee, Employee.id == Payroll.employee_id).filter(Payroll.user_id == user.id)
        if month:
            q = q.filter(Payroll.month == month)
        if year:
            q = q.filter(Payroll.year == year)
        if status:
            q = q.filter(Payroll.status == status)
        if employee_id:
            q = q.filter(Payroll.employee_id == employee_id)
        rows = q.order_by(Payroll.year.desc(), Payroll.month.desc(), Payroll.created_at.desc()).all()
        return {"items": [_payroll_out(db, p, e.name) for p, e in rows]}
    finally:
        db.close()


@router.get("/payrolls/{payroll_id}")
def get_payroll(payroll_id: str, request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        p = db.query(Payroll).filter(Payroll.user_id == user.id, Payroll.id == payroll_id).first()
        if not p:
            raise HTTPException(404, "الراتب غير موجود")
        e = db.query(Employee).filter(Employee.id == p.employee_id).first()
        return _payroll_out(db, p, e.name if e else "")
    finally:
        db.close()


@router.post("/payrolls")
def create_payroll(request: Request, body: PayrollCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == body.employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        existing = db.query(Payroll).filter(
            Payroll.employee_id == body.employee_id, Payroll.month == body.month, Payroll.year == body.year,
            Payroll.status != "cancelled",
        ).first()
        if existing:
            raise HTTPException(400, "يوجد بالفعل راتب لهذا الموظف لنفس الشهر والسنة")

        base_salary = body.base_salary if body.base_salary is not None else (employee.basic_salary or 0.0)
        daily_rate = (base_salary / 30.0) if base_salary else 0.0

        allow_lines = []
        ded_lines = []

        # 1) البدلات (متكررة كل شهر + مرة واحدة غير مطبّقة)
        allowances = db.query(AllowanceRecord).filter(
            AllowanceRecord.user_id == user.id, AllowanceRecord.employee_id == body.employee_id,
        ).filter((AllowanceRecord.recurring == 1) | (AllowanceRecord.applied == 0)).all()
        consumed_allowance_ids = []
        for a in allowances:
            allow_lines.append((a.allowance_type or "بدل", a.amount))
            if not a.recurring:
                consumed_allowance_ids.append(a.id)

        # 2) العمولات غير المطبّقة
        commissions = db.query(CommissionRecord).filter(
            CommissionRecord.user_id == user.id, CommissionRecord.employee_id == body.employee_id, CommissionRecord.applied == 0,
        ).all()
        for c in commissions:
            allow_lines.append((f"عمولة ({c.percentage}% من {c.sales_amount})", c.commission_value))

        # 3) الاستقطاعات غير المطبّقة
        deductions = db.query(DeductionRecord).filter(
            DeductionRecord.user_id == user.id, DeductionRecord.employee_id == body.employee_id, DeductionRecord.applied == 0,
        ).all()
        for d in deductions:
            ded_lines.append((d.deduction_type or "استقطاع", d.amount))

        # 4) السلف السارية — خصم قسط واحد تلقائياً
        advances = db.query(Advance).filter(
            Advance.user_id == user.id, Advance.employee_id == body.employee_id, Advance.status == "active",
        ).all()
        for adv in advances:
            installment = min(adv.installment_amount, adv.remaining_amount)
            if installment > 0:
                ded_lines.append((f"قسط سلفة بتاريخ {_fmt_date(adv.date)}", installment))

        # 5) الغياب من سجل الحضور لنفس الشهر/السنة
        attendance_rows = db.query(Attendance).filter(
            Attendance.user_id == user.id, Attendance.employee_id == body.employee_id, Attendance.is_absent == 1,
        ).all()
        absent_days = sum(1 for a in attendance_rows if a.date and a.date.month == body.month and a.date.year == body.year)
        if absent_days and daily_rate:
            ded_lines.append((f"خصم غياب ({absent_days} يوم)", round(daily_rate * absent_days, 2)))

        # 6) الإجازات غير المدفوعة المقبولة ضمن نفس الشهر
        leaves = db.query(LeaveRequest).filter(
            LeaveRequest.user_id == user.id, LeaveRequest.employee_id == body.employee_id,
            LeaveRequest.status == "accepted", LeaveRequest.paid == 0, LeaveRequest.applied == 0,
        ).all()
        consumed_leave_ids = []
        for lv in leaves:
            in_month = (lv.start_date and lv.start_date.month == body.month and lv.start_date.year == body.year) or \
                       (lv.end_date and lv.end_date.month == body.month and lv.end_date.year == body.year)
            if in_month and daily_rate:
                ded_lines.append((f"خصم إجازة غير مدفوعة ({lv.days_count} يوم)", round(daily_rate * lv.days_count, 2)))
                consumed_leave_ids.append(lv.id)

        # 7) بنود يدوية إضافية من المستخدم
        for item in body.extra_allowances:
            allow_lines.append((item.label, item.amount))
        for item in body.extra_deductions:
            ded_lines.append((item.label, item.amount))

        total_allowances = round(sum(x[1] for x in allow_lines), 2)
        total_deductions_no_advance = round(sum(x[1] for x in ded_lines), 2)
        gross_before_tax = round(base_salary + total_allowances, 2)

        settings = _get_hr_settings(db)
        tax_amount = round(gross_before_tax * (settings.get("tax_percentage") or 0) / 100.0, 2) if settings.get("tax_enabled") else 0.0

        net_salary = round(gross_before_tax - tax_amount - total_deductions_no_advance, 2)

        payroll = Payroll(
            id=uuid.uuid4().hex, user_id=user.id, employee_id=body.employee_id,
            month=body.month, year=body.year, base_salary=base_salary,
            total_allowances=total_allowances, total_deductions=total_deductions_no_advance,
            gross_before_tax=gross_before_tax, tax_amount=tax_amount, net_salary=net_salary,
            status="unpaid", notes=(body.notes or "").strip() or None,
        )
        db.add(payroll)
        db.flush()

        for label, amount in allow_lines:
            db.add(PayrollAllowance(payroll_id=payroll.id, label=label, amount=amount))
        for label, amount in ded_lines:
            db.add(PayrollDeduction(payroll_id=payroll.id, label=label, amount=amount))

        # تحديث حالة السلف/الاستقطاعات/العمولات/البدلات/الإجازات المستهلكة
        for adv in advances:
            installment = min(adv.installment_amount, adv.remaining_amount)
            if installment > 0:
                adv.remaining_amount = round(adv.remaining_amount - installment, 2)
                if adv.remaining_amount <= 0.009:
                    adv.remaining_amount = 0.0
                    adv.status = "completed"
        for d in deductions:
            d.applied = 1
            d.payroll_id = payroll.id
        for c in commissions:
            c.applied = 1
            c.payroll_id = payroll.id
        for aid in consumed_allowance_ids:
            a = db.query(AllowanceRecord).get(aid)
            if a:
                a.applied = 1
                a.payroll_id = payroll.id
        for lid in consumed_leave_ids:
            lv = db.query(LeaveRequest).get(lid)
            if lv:
                lv.applied = 1
                lv.payroll_id = payroll.id

        db.commit()
        log_event(db, "hr.payroll.create", user.id, {"payroll_id": payroll.id})
        return _payroll_out(db, payroll, employee.name)
    finally:
        db.close()


@router.post("/payrolls/{payroll_id}/pay")
def pay_payroll(payroll_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        p = db.query(Payroll).filter(Payroll.user_id == user.id, Payroll.id == payroll_id).first()
        if not p:
            raise HTTPException(404, "الراتب غير موجود")
        if p.status == "paid":
            raise HTTPException(400, "تم صرف هذا الراتب بالفعل")
        if p.status == "cancelled":
            raise HTTPException(400, "لا يمكن صرف راتب ملغى")
        p.status = "paid"
        p.paid_at = dt.datetime.utcnow()
        db.commit()
        log_event(db, "hr.payroll.pay", user.id, {"payroll_id": p.id})
        e = db.query(Employee).filter(Employee.id == p.employee_id).first()
        return _payroll_out(db, p, e.name if e else "")
    finally:
        db.close()


def _rollback_payroll_links(db, payroll_id: str):
    for d in db.query(DeductionRecord).filter(DeductionRecord.payroll_id == payroll_id).all():
        d.applied = 0
        d.payroll_id = None
    for c in db.query(CommissionRecord).filter(CommissionRecord.payroll_id == payroll_id).all():
        c.applied = 0
        c.payroll_id = None
    for a in db.query(AllowanceRecord).filter(AllowanceRecord.payroll_id == payroll_id).all():
        a.applied = 0
        a.payroll_id = None
    for lv in db.query(LeaveRequest).filter(LeaveRequest.payroll_id == payroll_id).all():
        lv.applied = 0
        lv.payroll_id = None
    for line in db.query(PayrollDeduction).filter(PayrollDeduction.payroll_id == payroll_id).all():
        if line.label.startswith("قسط سلفة"):
            adv = db.query(Advance).filter(
                Advance.employee_id == db.query(Payroll).get(payroll_id).employee_id, Advance.status.in_(["active", "completed"]),
            ).order_by(Advance.created_at.desc()).first()
            if adv:
                adv.remaining_amount = round(min(adv.amount, adv.remaining_amount + line.amount), 2)
                if adv.remaining_amount > 0:
                    adv.status = "active"


@router.delete("/payrolls/{payroll_id}")
def delete_payroll(payroll_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        p = db.query(Payroll).filter(Payroll.user_id == user.id, Payroll.id == payroll_id).first()
        if not p:
            raise HTTPException(404, "الراتب غير موجود")
        if p.status == "paid":
            raise HTTPException(400, "لا يمكن حذف راتب تم صرفه بالفعل — يمكن إلغاؤه فقط إن لم يُصرف")
        _rollback_payroll_links(db, payroll_id)
        db.query(PayrollAllowance).filter(PayrollAllowance.payroll_id == payroll_id).delete()
        db.query(PayrollDeduction).filter(PayrollDeduction.payroll_id == payroll_id).delete()
        db.delete(p)
        db.commit()
        log_event(db, "hr.payroll.delete", user.id, {"payroll_id": payroll_id})
        return {"deleted": True}
    finally:
        db.close()


@router.post("/payrolls/{payroll_id}/cancel")
def cancel_payroll(payroll_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        p = db.query(Payroll).filter(Payroll.user_id == user.id, Payroll.id == payroll_id).first()
        if not p:
            raise HTTPException(404, "الراتب غير موجود")
        if p.status == "paid":
            raise HTTPException(400, "لا يمكن إلغاء راتب تم صرفه")
        _rollback_payroll_links(db, payroll_id)
        p.status = "cancelled"
        db.commit()
        e = db.query(Employee).filter(Employee.id == p.employee_id).first()
        return _payroll_out(db, p, e.name if e else "")
    finally:
        db.close()


@router.get("/dashboard-summary")
def hr_dashboard_summary(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        now = dt.datetime.utcnow()
        employees_count = db.query(Employee).filter(Employee.user_id == user.id, Employee.is_active == 1).count()
        rows = db.query(Payroll).filter(Payroll.user_id == user.id, Payroll.month == now.month, Payroll.year == now.year).all()
        total = round(sum(float(p.net_salary or 0) for p in rows), 2)
        paid = round(sum(float(p.net_salary or 0) for p in rows if p.status == "paid"), 2)
        unpaid = round(sum(float(p.net_salary or 0) for p in rows if p.status == "unpaid"), 2)
        advances_active = db.query(Advance).filter(Advance.user_id == user.id, Advance.status == "active").count()
        leaves_pending = db.query(LeaveRequest).filter(LeaveRequest.user_id == user.id, LeaveRequest.status == "pending").count()
        paid_employee_ids = {p.employee_id for p in rows if p.status == "paid"}
        active_employees = db.query(Employee).filter(Employee.user_id == user.id, Employee.status == "active").all()
        not_paid_yet = sum(1 for e in active_employees if e.id not in paid_employee_ids)
        absent_today = db.query(Attendance).filter(
            Attendance.user_id == user.id, Attendance.is_absent == 1,
            Attendance.date >= dt.datetime(now.year, now.month, now.day),
        ).count()
        active_emps = db.query(Employee).filter(Employee.user_id == user.id, Employee.is_active == 1).all()
        residency_expired_count = sum(1 for e in active_emps if (_days_left(e.residency_expiry) or 999) < 0)
        contracts_expiring_count = sum(
            1 for c in db.query(EmployeeContract).filter(EmployeeContract.user_id == user.id, EmployeeContract.status == "active").all()
            if (_days_left(c.end_date) is not None and _days_left(c.end_date) <= 30)
        )
        return {
            "employees_count": employees_count,
            "total_this_month": total,
            "paid_amount": paid,
            "unpaid_amount": unpaid,
            "advances_active": advances_active,
            "leaves_pending": leaves_pending,
            "not_paid_yet": not_paid_yet,
            "absent_today": absent_today,
            "residency_expired_count": residency_expired_count,
            "contracts_expiring_count": contracts_expiring_count,
        }
    finally:
        db.close()



# ============================================================
# نهاية الخدمة
# ============================================================

class EndOfServiceCreate(BaseModel):
    employee_id: str
    end_date: str
    reason: str  # resigned|terminated|contract_end|retirement


EOS_REASON_LABELS = {"resigned": "مستقيل", "terminated": "مفصول", "contract_end": "انتهاء عقد", "retirement": "تقاعد"}


def _eos_out(r: EndOfService, employee_name: str = "") -> dict:
    return {
        "id": r.id, "employee_id": r.employee_id, "employee_name": employee_name,
        "end_date": _fmt_date(r.end_date), "reason": r.reason, "reason_label": EOS_REASON_LABELS.get(r.reason, r.reason),
        "remaining_salaries": round(float(r.remaining_salaries or 0.0), 2),
        "remaining_advances": round(float(r.remaining_advances or 0.0), 2),
        "deductions_total": round(float(r.deductions_total or 0.0), 2),
        "leave_balance_days": round(float(r.leave_balance_days or 0.0), 2),
        "leave_balance_value": round(float(r.leave_balance_value or 0.0), 2),
        "gratuity_amount": round(float(r.gratuity_amount or 0.0), 2),
        "total_dues": round(float(r.total_dues or 0.0), 2),
        "created_at": _fmt_dt(r.created_at),
    }


def _compute_gratuity(basic_salary: float, hire_date, end_date) -> float:
    """مكافأة نهاية خدمة مبسّطة على النمط السعودي: نصف شهر لكل سنة من أول 5 سنوات، وشهر كامل لكل سنة بعدها."""
    if not hire_date or not end_date or basic_salary <= 0:
        return 0.0
    years = max((end_date - hire_date).days / 365.25, 0)
    if years <= 5:
        return round(basic_salary * 0.5 * years, 2)
    return round(basic_salary * 0.5 * 5 + basic_salary * 1.0 * (years - 5), 2)


@router.get("/end-of-service")
def list_end_of_service(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(EndOfService, Employee).join(Employee, Employee.id == EndOfService.employee_id).filter(EndOfService.user_id == user.id).order_by(EndOfService.created_at.desc()).all()
        return {"items": [_eos_out(r, e.name) for r, e in rows]}
    finally:
        db.close()


@router.get("/employees/{employee_id}/end-of-service-preview")
def preview_end_of_service(employee_id: str, request: Request, end_date: str = Query(...)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        end_dt = _parse_simple_date(end_date, "تاريخ نهاية الخدمة")

        unpaid_payrolls = db.query(Payroll).filter(Payroll.employee_id == employee_id, Payroll.status == "unpaid").all()
        remaining_salaries = round(sum(float(p.net_salary or 0) for p in unpaid_payrolls), 2)

        active_advances = db.query(Advance).filter(Advance.employee_id == employee_id, Advance.status == "active").all()
        remaining_advances = round(sum(float(a.remaining_amount or 0) for a in active_advances), 2)

        unapplied_deductions = db.query(DeductionRecord).filter(DeductionRecord.employee_id == employee_id, DeductionRecord.applied == 0).all()
        deductions_total = round(sum(float(d.amount or 0) for d in unapplied_deductions), 2)

        contract = db.query(EmployeeContract).filter(EmployeeContract.employee_id == employee_id, EmployeeContract.status == "active").order_by(EmployeeContract.created_at.desc()).first()
        annual_leave_days = contract.annual_leave_days if contract else 21
        hire_date = employee.hire_date
        years = max((end_dt - hire_date).days / 365.25, 0) if hire_date else 0
        earned_leave_days = round(annual_leave_days * years, 1)
        taken_leave_days = sum(
            l.days_count for l in db.query(LeaveRequest).filter(LeaveRequest.employee_id == employee_id, LeaveRequest.status == "accepted").all()
        )
        leave_balance_days = max(round(earned_leave_days - taken_leave_days, 1), 0)
        daily_rate = (employee.basic_salary or 0.0) / 30.0
        leave_balance_value = round(leave_balance_days * daily_rate, 2)

        gratuity = _compute_gratuity(employee.basic_salary or 0.0, hire_date, end_dt)

        total_dues = round(remaining_salaries - remaining_advances - deductions_total + leave_balance_value + gratuity, 2)

        return {
            "remaining_salaries": remaining_salaries, "remaining_advances": remaining_advances,
            "deductions_total": deductions_total, "leave_balance_days": leave_balance_days,
            "leave_balance_value": leave_balance_value, "gratuity_amount": gratuity, "total_dues": total_dues,
        }
    finally:
        db.close()


@router.post("/employees/{employee_id}/end-of-service")
def create_end_of_service(employee_id: str, request: Request, body: EndOfServiceCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        end_dt = _parse_simple_date(body.end_date, "تاريخ نهاية الخدمة")
        unpaid_payrolls = db.query(Payroll).filter(Payroll.employee_id == employee_id, Payroll.status == "unpaid").all()
        remaining_salaries = round(sum(float(p.net_salary or 0) for p in unpaid_payrolls), 2)
        active_advances = db.query(Advance).filter(Advance.employee_id == employee_id, Advance.status == "active").all()
        remaining_advances = round(sum(float(a.remaining_amount or 0) for a in active_advances), 2)
        unapplied_deductions = db.query(DeductionRecord).filter(DeductionRecord.employee_id == employee_id, DeductionRecord.applied == 0).all()
        deductions_total = round(sum(float(d.amount or 0) for d in unapplied_deductions), 2)
        contract = db.query(EmployeeContract).filter(EmployeeContract.employee_id == employee_id, EmployeeContract.status == "active").order_by(EmployeeContract.created_at.desc()).first()
        annual_leave_days = contract.annual_leave_days if contract else 21
        hire_date = employee.hire_date
        years = max((end_dt - hire_date).days / 365.25, 0) if hire_date else 0
        earned_leave_days = round(annual_leave_days * years, 1)
        taken_leave_days = sum(l.days_count for l in db.query(LeaveRequest).filter(LeaveRequest.employee_id == employee_id, LeaveRequest.status == "accepted").all())
        leave_balance_days = max(round(earned_leave_days - taken_leave_days, 1), 0)
        daily_rate = (employee.basic_salary or 0.0) / 30.0
        leave_balance_value = round(leave_balance_days * daily_rate, 2)
        gratuity = _compute_gratuity(employee.basic_salary or 0.0, hire_date, end_dt)
        total_dues = round(remaining_salaries - remaining_advances - deductions_total + leave_balance_value + gratuity, 2)

        rec = EndOfService(
            id=uuid.uuid4().hex, user_id=user.id, employee_id=employee_id, end_date=end_dt, reason=body.reason,
            remaining_salaries=remaining_salaries, remaining_advances=remaining_advances,
            deductions_total=deductions_total, leave_balance_days=leave_balance_days,
            leave_balance_value=leave_balance_value, gratuity_amount=gratuity, total_dues=total_dues,
        )
        db.add(rec)
        employee.status = "terminated"
        employee.is_active = 0
        db.commit()
        log_event(db, "hr.end_of_service.create", user.id, {"employee_id": employee_id})
        return _eos_out(rec, employee.name)
    finally:
        db.close()

def _doc_status(expiry_date) -> dict:
    if not expiry_date:
        return {"status": "active", "status_label": "سارية", "severity": "none", "days_left": None}
    today = dt.datetime.utcnow().date()
    exp = expiry_date.date() if hasattr(expiry_date, "date") else expiry_date
    days_left = (exp - today).days
    if days_left < 0:
        return {"status": "expired", "status_label": "منتهية", "severity": "expired", "days_left": days_left}
    if days_left <= 7:
        return {"status": "expiring_7", "status_label": f"تنتهي خلال {days_left} يوم", "severity": "critical", "days_left": days_left}
    if days_left <= 15:
        return {"status": "expiring_15", "status_label": f"تنتهي خلال {days_left} يوم", "severity": "high", "days_left": days_left}
    if days_left <= 30:
        return {"status": "expiring_30", "status_label": f"تنتهي خلال {days_left} يوم", "severity": "medium", "days_left": days_left}
    if days_left <= 60:
        return {"status": "expiring_60", "status_label": f"تنتهي خلال {days_left} يوم", "severity": "notice", "days_left": days_left}
    return {"status": "active", "status_label": "سارية", "severity": "none", "days_left": days_left}


def _doc_out(d: EmployeeDocument) -> dict:
    out = {
        "id": d.id,
        "employee_id": d.employee_id,
        "doc_type": d.doc_type,
        "doc_number": d.doc_number or "",
        "issue_date": d.issue_date.strftime("%Y-%m-%d") if d.issue_date else "",
        "expiry_date": d.expiry_date.strftime("%Y-%m-%d") if d.expiry_date else "",
        "issuing_authority": d.issuing_authority or "",
        "notes": d.notes or "",
        "file_url": d.file_url or "",
        "updated_at": d.updated_at.strftime("%Y-%m-%d %H:%M") if d.updated_at else "",
    }
    out.update(_doc_status(d.expiry_date))
    return out

@router.get("/employees/{employee_id}/documents")
def list_employee_documents(employee_id: str, request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        rows = (
            db.query(EmployeeDocument)
            .filter(EmployeeDocument.user_id == user.id, EmployeeDocument.employee_id == employee_id)
            .order_by(EmployeeDocument.created_at.desc())
            .all()
        )
        items = [_doc_out(r) for r in rows]
        total = len(items)
        active = sum(1 for x in items if x["status"] == "active")
        expiring_30 = sum(1 for x in items if x["days_left"] is not None and 0 <= x["days_left"] <= 30)
        expired = sum(1 for x in items if x["status"] == "expired")
        last_updated = max((x["updated_at"] for x in items), default="")
        return {
            "items": items,
            "stats": {
                "total": total,
                "active": active,
                "expiring_30": expiring_30,
                "expired": expired,
                "last_updated": last_updated,
            },
        }
    finally:
        db.close()


@router.post("/employees/{employee_id}/documents")
async def create_employee_document(
    employee_id: str,
    request: Request,
    doc_type: str = Form(...),
    doc_number: str = Form(""),
    issue_date: str = Form(""),
    expiry_date: str = Form(""),
    issuing_authority: str = Form(""),
    notes: str = Form(""),
    file: Optional[UploadFile] = File(None),
):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        employee = db.query(Employee).filter(Employee.user_id == user.id, Employee.id == employee_id).first()
        if not employee:
            raise HTTPException(404, "الموظف غير موجود")
        if not (doc_type or "").strip():
            raise HTTPException(400, "نوع الوثيقة مطلوب")
        file_url = None
        if file is not None and getattr(file, "filename", ""):
            original = file.filename or "doc"
            lower = original.lower()
            if not any(lower.endswith(s) for s in _ALLOWED_DOC_SUFFIXES):
                raise HTTPException(400, "صيغة الملف يجب أن تكون PDF أو JPG أو PNG")
            content = await file.read()
            if len(content) > _MAX_DOC_MB * 1024 * 1024:
                raise HTTPException(400, f"حجم الملف أكبر من {_MAX_DOC_MB} ميجابايت")
            suffix = _Path(original).suffix.lower() or ".pdf"
            saved_name = f"{uuid.uuid4().hex}{suffix}"
            with open(_HR_DOCS_DIR / saved_name, "wb") as f:
                f.write(content)
            file_url = f"/uploads/hr_documents/{saved_name}"
        rec = EmployeeDocument(
            id=uuid.uuid4().hex,
            user_id=user.id,
            employee_id=employee_id,
            doc_type=doc_type.strip(),
            doc_number=(doc_number or "").strip() or None,
            issue_date=_parse_simple_date(issue_date, "تاريخ الإصدار"),
            expiry_date=_parse_simple_date(expiry_date, "تاريخ الانتهاء"),
            issuing_authority=(issuing_authority or "").strip() or None,
            notes=(notes or "").strip() or None,
            file_url=file_url,
        )
        db.add(rec)
        db.commit()
        log_event(db, "hr.document.create", user.id, {"document_id": rec.id, "employee_id": employee_id, "doc_type": rec.doc_type})
        return _doc_out(rec)
    finally:
        db.close()


class DocumentUpdate(BaseModel):
    doc_type: Optional[str] = None
    doc_number: Optional[str] = None
    issue_date: Optional[str] = None
    expiry_date: Optional[str] = None
    issuing_authority: Optional[str] = None
    notes: Optional[str] = None


@router.patch("/documents/{document_id}")
def update_document(document_id: str, request: Request, body: DocumentUpdate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(EmployeeDocument).filter(EmployeeDocument.user_id == user.id, EmployeeDocument.id == document_id).first()
        if not rec:
            raise HTTPException(404, "الوثيقة غير موجودة")
        if body.doc_type is not None and body.doc_type.strip():
            rec.doc_type = body.doc_type.strip()
        if body.doc_number is not None:
            rec.doc_number = body.doc_number.strip() or None
        if body.issue_date is not None:
            rec.issue_date = _parse_simple_date(body.issue_date, "تاريخ الإصدار")
        if body.expiry_date is not None:
            rec.expiry_date = _parse_simple_date(body.expiry_date, "تاريخ الانتهاء")
        if body.issuing_authority is not None:
            rec.issuing_authority = body.issuing_authority.strip() or None
        if body.notes is not None:
            rec.notes = body.notes.strip() or None
        db.commit()
        return _doc_out(rec)
    finally:
        db.close()


@router.post("/documents/{document_id}/file")
async def upload_document_file(document_id: str, request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(EmployeeDocument).filter(EmployeeDocument.user_id == user.id, EmployeeDocument.id == document_id).first()
        if not rec:
            raise HTTPException(404, "الوثيقة غير موجودة")
        original = file.filename or "doc"
        lower = original.lower()
        if not any(lower.endswith(s) for s in _ALLOWED_DOC_SUFFIXES):
            raise HTTPException(400, "صيغة الملف يجب أن تكون PDF أو JPG أو PNG")
        content = await file.read()
        if len(content) > _MAX_DOC_MB * 1024 * 1024:
            raise HTTPException(400, f"حجم الملف أكبر من {_MAX_DOC_MB} ميجابايت")
        suffix = _Path(original).suffix.lower() or ".pdf"
        saved_name = f"{uuid.uuid4().hex}{suffix}"
        with open(_HR_DOCS_DIR / saved_name, "wb") as f:
            f.write(content)
        rec.file_url = f"/uploads/hr_documents/{saved_name}"
        db.commit()
        log_event(db, "hr.document.file_upload", user.id, {"document_id": rec.id})
        return _doc_out(rec)
    finally:
        db.close()


@router.delete("/documents/{document_id}")
def delete_document(document_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(EmployeeDocument).filter(EmployeeDocument.user_id == user.id, EmployeeDocument.id == document_id).first()
        if not rec:
            raise HTTPException(404, "الوثيقة غير موجودة")
        db.query(EmployeeDocumentRenewal).filter(EmployeeDocumentRenewal.document_id == rec.id).delete()
        db.delete(rec)
        db.commit()
        log_event(db, "hr.document.delete", user.id, {"document_id": document_id})
        return {"deleted": True}
    finally:
        db.close()


class DocumentRenew(BaseModel):
    new_issue_date: str = ""
    new_expiry_date: str = ""
    notes: str = ""


@router.post("/documents/{document_id}/renew")
def renew_document(document_id: str, request: Request, body: DocumentRenew = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(EmployeeDocument).filter(EmployeeDocument.user_id == user.id, EmployeeDocument.id == document_id).first()
        if not rec:
            raise HTTPException(404, "الوثيقة غير موجودة")
        new_issue = _parse_simple_date(body.new_issue_date, "تاريخ الإصدار الجديد")
        new_expiry = _parse_simple_date(body.new_expiry_date, "تاريخ الانتهاء الجديد")
        if not new_expiry:
            raise HTTPException(400, "تاريخ الانتهاء الجديد مطلوب")
        history = EmployeeDocumentRenewal(
            id=uuid.uuid4().hex,
            document_id=rec.id,
            old_issue_date=rec.issue_date,
            old_expiry_date=rec.expiry_date,
            renewed_by_name=user.username,
            notes=(body.notes or "").strip() or None,
        )
        db.add(history)
        if new_issue:
            rec.issue_date = new_issue
        rec.expiry_date = new_expiry
        db.commit()
        log_event(db, "hr.document.renew", user.id, {"document_id": rec.id})
        return _doc_out(rec)
    finally:
        db.close()


@router.get("/documents/{document_id}/history")
def document_history(document_id: str, request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rec = db.query(EmployeeDocument).filter(EmployeeDocument.user_id == user.id, EmployeeDocument.id == document_id).first()
        if not rec:
            raise HTTPException(404, "الوثيقة غير موجودة")
        rows = (
            db.query(EmployeeDocumentRenewal)
            .filter(EmployeeDocumentRenewal.document_id == document_id)
            .order_by(EmployeeDocumentRenewal.renewed_at.desc())
            .all()
        )
        return {
            "items": [
                {
                    "old_issue_date": r.old_issue_date.strftime("%Y-%m-%d") if r.old_issue_date else "",
                    "old_expiry_date": r.old_expiry_date.strftime("%Y-%m-%d") if r.old_expiry_date else "",
                    "renewed_at": r.renewed_at.strftime("%Y-%m-%d %H:%M") if r.renewed_at else "",
                    "renewed_by_name": r.renewed_by_name or "",
                    "notes": r.notes or "",
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@router.get("/documents/alerts")
def documents_alerts(
    request: Request,
    doc_type: str = Query(""),
    department: str = Query(""),
    branch_name: str = Query(""),
    employee_id: str = Query(""),
    status: str = Query(""),
    q: str = Query(""),
    only_attention: bool = Query(False),
    limit: int = Query(500, ge=1, le=2000),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        query = (
            db.query(EmployeeDocument, Employee)
            .join(Employee, Employee.id == EmployeeDocument.employee_id)
            .filter(EmployeeDocument.user_id == user.id)
        )
        if (doc_type or "").strip():
            query = query.filter(EmployeeDocument.doc_type == doc_type.strip())
        if (department or "").strip():
            query = query.filter(Employee.department == department.strip())
        if (branch_name or "").strip():
            query = query.filter(Employee.branch_name == branch_name.strip())
        if (employee_id or "").strip():
            query = query.filter(EmployeeDocument.employee_id == employee_id.strip())
        rows = query.order_by(EmployeeDocument.expiry_date.asc().nullslast()).limit(limit).all()
        qn = (q or "").strip().lower()
        items = []
        for doc, emp in rows:
            out = _doc_out(doc)
            out["employee_name"] = emp.name
            out["department"] = emp.department or ""
            out["branch_name"] = emp.branch_name or ""
            if qn and qn not in (f"{emp.name} {doc.doc_number or ''} {doc.doc_type}").lower():
                continue
            if status and out["status"] != status:
                continue
            if only_attention and out["status"] == "active":
                continue
            items.append(out)
        return {"items": items}
    finally:
        db.close()



# ============================================================
# التقارير (Excel export لكل تقرير)
# ============================================================

MONTHS_AR = ["يناير","فبراير","مارس","أبريل","مايو","يونيو","يوليو","أغسطس","سبتمبر","أكتوبر","نوفمبر","ديسمبر"]


@router.get("/reports/payroll/export")
def report_payroll_export(request: Request, month: int = Query(0), year: int = Query(0)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Payroll, Employee).join(Employee, Employee.id == Payroll.employee_id).filter(Payroll.user_id == user.id)
        if month:
            q = q.filter(Payroll.month == month)
        if year:
            q = q.filter(Payroll.year == year)
        rows = q.order_by(Payroll.year.desc(), Payroll.month.desc()).all()
        data = [[e.name, MONTHS_AR[p.month-1], p.year, p.base_salary, p.total_allowances, p.total_deductions, p.tax_amount, p.net_salary, p.status] for p, e in rows]
        return _xlsx_response(["الموظف","الشهر","السنة","الأساسي","البدلات","الاستقطاعات","الضريبة","الصافي","الحالة"], data, "رواتب.xlsx")
    finally:
        db.close()


@router.get("/reports/advances/export")
def report_advances_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Advance, Employee).join(Employee, Employee.id == Advance.employee_id).filter(Advance.user_id == user.id).all()
        data = [[e.name, _fmt_date(a.date), a.amount, a.reason or "", a.installments_count, a.remaining_amount, a.status] for a, e in rows]
        return _xlsx_response(["الموظف","التاريخ","المبلغ","السبب","عدد الأقساط","المتبقي","الحالة"], data, "السلف.xlsx")
    finally:
        db.close()


@router.get("/reports/deductions/export")
def report_deductions_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(DeductionRecord, Employee).join(Employee, Employee.id == DeductionRecord.employee_id).filter(DeductionRecord.user_id == user.id).all()
        data = [[e.name, d.deduction_type or "", d.amount, d.reason or "", _fmt_date(d.date), "مطبّق" if d.applied else "بالانتظار"] for d, e in rows]
        return _xlsx_response(["الموظف","النوع","القيمة","السبب","التاريخ","الحالة"], data, "الاستقطاعات.xlsx")
    finally:
        db.close()


@router.get("/reports/commissions/export")
def report_commissions_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(CommissionRecord, Employee).join(Employee, Employee.id == CommissionRecord.employee_id).filter(CommissionRecord.user_id == user.id).all()
        data = [[e.name, c.sales_amount, c.percentage, c.commission_value, _fmt_date(c.date), "مطبّقة" if c.applied else "بالانتظار"] for c, e in rows]
        return _xlsx_response(["الموظف","المبيعات","النسبة %","قيمة العمولة","التاريخ","الحالة"], data, "العمولات.xlsx")
    finally:
        db.close()


@router.get("/reports/attendance/export")
def report_attendance_export(request: Request, month: int = Query(0), year: int = Query(0)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Attendance, Employee).join(Employee, Employee.id == Attendance.employee_id).filter(Attendance.user_id == user.id).all()
        if month:
            rows = [r for r in rows if r[0].date and r[0].date.month == month]
        if year:
            rows = [r for r in rows if r[0].date and r[0].date.year == year]
        data = [[e.name, _fmt_date(a.date), a.check_in or "", a.check_out or "", a.late_minutes, a.overtime_minutes, "غياب" if a.is_absent else "حضور"] for a, e in rows]
        return _xlsx_response(["الموظف","التاريخ","الدخول","الخروج","التأخير(دقيقة)","الإضافي(دقيقة)","الحالة"], data, "الحضور.xlsx")
    finally:
        db.close()


@router.get("/reports/leaves/export")
def report_leaves_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(LeaveRequest, Employee).join(Employee, Employee.id == LeaveRequest.employee_id).filter(LeaveRequest.user_id == user.id).all()
        data = [[e.name, l.leave_type or "", _fmt_date(l.start_date), _fmt_date(l.end_date), l.days_count, "مدفوعة" if l.paid else "غير مدفوعة", l.status] for l, e in rows]
        return _xlsx_response(["الموظف","النوع","من","إلى","الأيام","مدفوعة؟","الحالة"], data, "الإجازات.xlsx")
    finally:
        db.close()


@router.get("/reports/residency-expiry/export")
def report_residency_expiry_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id, Employee.residency_expiry.isnot(None)).all()
        data = [[e.name, e.national_id or "", _fmt_date(e.residency_expiry), _days_left(e.residency_expiry)] for e in rows]
        return _xlsx_response(["الموظف","رقم الهوية/الإقامة","تاريخ الانتهاء","الأيام المتبقية"], data, "انتهاء_الاقامات.xlsx")
    finally:
        db.close()


@router.get("/reports/passport-expiry/export")
def report_passport_expiry_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id, Employee.passport_expiry.isnot(None)).all()
        data = [[e.name, e.passport_number or "", _fmt_date(e.passport_expiry), _days_left(e.passport_expiry)] for e in rows]
        return _xlsx_response(["الموظف","رقم الجواز","تاريخ الانتهاء","الأيام المتبقية"], data, "انتهاء_الجوازات.xlsx")
    finally:
        db.close()


@router.get("/reports/contracts-expiry/export")
def report_contracts_expiry_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(EmployeeContract, Employee).join(Employee, Employee.id == EmployeeContract.employee_id).filter(EmployeeContract.user_id == user.id, EmployeeContract.status == "active").all()
        data = [[e.name, c.contract_type or "", _fmt_date(c.end_date), _days_left(c.end_date)] for c, e in rows]
        return _xlsx_response(["الموظف","نوع العقد","تاريخ الانتهاء","الأيام المتبقية"], data, "انتهاء_العقود.xlsx")
    finally:
        db.close()


@router.get("/reports/employee-cost/export")
def report_employee_cost_export(request: Request, month: int = Query(0), year: int = Query(0)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Payroll, Employee).join(Employee, Employee.id == Payroll.employee_id).filter(Payroll.user_id == user.id)
        if month:
            q = q.filter(Payroll.month == month)
        if year:
            q = q.filter(Payroll.year == year)
        rows = q.all()
        by_emp = {}
        for p, e in rows:
            by_emp.setdefault(e.name, 0.0)
            by_emp[e.name] += float(p.net_salary or 0.0)
        data = [[name, round(total, 2)] for name, total in sorted(by_emp.items(), key=lambda x: -x[1])]
        return _xlsx_response(["الموظف","إجمالي التكلفة"], data, "تكلفة_الموظفين.xlsx")
    finally:
        db.close()


@router.get("/reports/employees-by-branch/export")
def report_employees_by_branch_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id).all()
        by_branch = {}
        for e in rows:
            b = e.branch_name or "بدون فرع"
            by_branch.setdefault(b, 0)
            by_branch[b] += 1
        data = [[b, c] for b, c in sorted(by_branch.items(), key=lambda x: -x[1])]
        return _xlsx_response(["الفرع","عدد الموظفين"], data, "الموظفين_حسب_الفرع.xlsx")
    finally:
        db.close()


@router.get("/reports/employees-by-department/export")
def report_employees_by_department_export(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id).all()
        by_dept = {}
        for e in rows:
            d = e.department or "بدون قسم"
            by_dept.setdefault(d, 0)
            by_dept[d] += 1
        data = [[d, c] for d, c in sorted(by_dept.items(), key=lambda x: -x[1])]
        return _xlsx_response(["القسم","عدد الموظفين"], data, "الموظفين_حسب_القسم.xlsx")
    finally:
        db.close()


# ============================================================
# نفس التقارير أعلاه — نسخة PDF احترافية (شعار + اسم الشركة + تاريخ + ترقيم صفحات + دعم عربي كامل)
# ============================================================

@router.get("/reports/payroll/pdf")
def report_payroll_pdf(request: Request, month: int = Query(0), year: int = Query(0)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Payroll, Employee).join(Employee, Employee.id == Payroll.employee_id).filter(Payroll.user_id == user.id)
        if month:
            q = q.filter(Payroll.month == month)
        if year:
            q = q.filter(Payroll.year == year)
        rows = q.order_by(Payroll.year.desc(), Payroll.month.desc()).all()
        data = [[e.name, MONTHS_AR[p.month-1], p.year, p.base_salary, p.total_allowances, p.total_deductions, p.tax_amount, p.net_salary, p.status] for p, e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","الشهر","السنة","الأساسي","البدلات","الاستقطاعات","الضريبة","الصافي","الحالة"], data, "تقرير الرواتب", "رواتب.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/advances/pdf")
def report_advances_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Advance, Employee).join(Employee, Employee.id == Advance.employee_id).filter(Advance.user_id == user.id).all()
        data = [[e.name, _fmt_date(a.date), a.amount, a.reason or "", a.installments_count, a.remaining_amount, a.status] for a, e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","التاريخ","المبلغ","السبب","عدد الأقساط","المتبقي","الحالة"], data, "تقرير السلف", "السلف.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/deductions/pdf")
def report_deductions_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(DeductionRecord, Employee).join(Employee, Employee.id == DeductionRecord.employee_id).filter(DeductionRecord.user_id == user.id).all()
        data = [[e.name, d.deduction_type or "", d.amount, d.reason or "", _fmt_date(d.date), "مطبّق" if d.applied else "بالانتظار"] for d, e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","النوع","القيمة","السبب","التاريخ","الحالة"], data, "تقرير الاستقطاعات", "الاستقطاعات.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/commissions/pdf")
def report_commissions_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(CommissionRecord, Employee).join(Employee, Employee.id == CommissionRecord.employee_id).filter(CommissionRecord.user_id == user.id).all()
        data = [[e.name, c.sales_amount, c.percentage, c.commission_value, _fmt_date(c.date), "مطبّقة" if c.applied else "بالانتظار"] for c, e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","المبيعات","النسبة %","قيمة العمولة","التاريخ","الحالة"], data, "تقرير العمولات", "العمولات.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/attendance/pdf")
def report_attendance_pdf(request: Request, month: int = Query(0), year: int = Query(0)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Attendance, Employee).join(Employee, Employee.id == Attendance.employee_id).filter(Attendance.user_id == user.id).all()
        if month:
            rows = [r for r in rows if r[0].date and r[0].date.month == month]
        if year:
            rows = [r for r in rows if r[0].date and r[0].date.year == year]
        data = [[e.name, _fmt_date(a.date), a.check_in or "", a.check_out or "", a.late_minutes, a.overtime_minutes, "غياب" if a.is_absent else "حضور"] for a, e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","التاريخ","الدخول","الخروج","التأخير(دقيقة)","الإضافي(دقيقة)","الحالة"], data, "تقرير الحضور والانصراف", "الحضور.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/leaves/pdf")
def report_leaves_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(LeaveRequest, Employee).join(Employee, Employee.id == LeaveRequest.employee_id).filter(LeaveRequest.user_id == user.id).all()
        data = [[e.name, l.leave_type or "", _fmt_date(l.start_date), _fmt_date(l.end_date), l.days_count, "مدفوعة" if l.paid else "غير مدفوعة", l.status] for l, e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","النوع","من","إلى","الأيام","مدفوعة؟","الحالة"], data, "تقرير الإجازات", "الإجازات.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/residency-expiry/pdf")
def report_residency_expiry_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id, Employee.residency_expiry.isnot(None)).all()
        data = [[e.name, e.national_id or "", _fmt_date(e.residency_expiry), _days_left(e.residency_expiry)] for e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","رقم الهوية/الإقامة","تاريخ الانتهاء","الأيام المتبقية"], data, "تقرير انتهاء الإقامات", "انتهاء_الاقامات.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/passport-expiry/pdf")
def report_passport_expiry_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id, Employee.passport_expiry.isnot(None)).all()
        data = [[e.name, e.passport_number or "", _fmt_date(e.passport_expiry), _days_left(e.passport_expiry)] for e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","رقم الجواز","تاريخ الانتهاء","الأيام المتبقية"], data, "تقرير انتهاء الجوازات", "انتهاء_الجوازات.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/contracts-expiry/pdf")
def report_contracts_expiry_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(EmployeeContract, Employee).join(Employee, Employee.id == EmployeeContract.employee_id).filter(EmployeeContract.user_id == user.id, EmployeeContract.status == "active").all()
        data = [[e.name, c.contract_type or "", _fmt_date(c.end_date), _days_left(c.end_date)] for c, e in rows]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","نوع العقد","تاريخ الانتهاء","الأيام المتبقية"], data, "تقرير انتهاء العقود", "انتهاء_العقود.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/employee-cost/pdf")
def report_employee_cost_pdf(request: Request, month: int = Query(0), year: int = Query(0)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Payroll, Employee).join(Employee, Employee.id == Payroll.employee_id).filter(Payroll.user_id == user.id)
        if month:
            q = q.filter(Payroll.month == month)
        if year:
            q = q.filter(Payroll.year == year)
        rows = q.all()
        by_emp = {}
        for p, e in rows:
            by_emp.setdefault(e.name, 0.0)
            by_emp[e.name] += float(p.net_salary or 0.0)
        data = [[name, round(total, 2)] for name, total in sorted(by_emp.items(), key=lambda x: -x[1])]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الموظف","إجمالي التكلفة"], data, "تقرير تكلفة الموظفين", "تكلفة_الموظفين.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/employees-by-branch/pdf")
def report_employees_by_branch_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id).all()
        by_branch = {}
        for e in rows:
            b = e.branch_name or "بدون فرع"
            by_branch.setdefault(b, 0)
            by_branch[b] += 1
        data = [[b, c] for b, c in sorted(by_branch.items(), key=lambda x: -x[1])]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["الفرع","عدد الموظفين"], data, "تقرير الموظفين حسب الفرع", "الموظفين_حسب_الفرع.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()


@router.get("/reports/employees-by-department/pdf")
def report_employees_by_department_pdf(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id).all()
        by_dept = {}
        for e in rows:
            d = e.department or "بدون قسم"
            by_dept.setdefault(d, 0)
            by_dept[d] += 1
        data = [[d, c] for d, c in sorted(by_dept.items(), key=lambda x: -x[1])]
        company_name, logo_url, generated_by = _pdf_meta(db, user)
        return generate_pdf_report(["القسم","عدد الموظفين"], data, "تقرير الموظفين حسب القسم", "الموظفين_حسب_القسم.pdf", company_name, logo_url, generated_by)
    finally:
        db.close()



# ============================================================
# لوحة التنبيهات الموحّدة (أعلى الصفحة)
# ============================================================

def _tier_for_days(days_left, expired_label="منتهية"):
    if days_left is None:
        return None
    if days_left < 0:
        return ("critical", expired_label)
    if days_left <= 30:
        return ("high", f"تنتهي خلال {days_left} يوم")
    if days_left <= 90:
        return ("medium", f"تنتهي خلال {days_left} يوم")
    return None


@router.get("/alerts/feed")
def alerts_feed(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        employees = db.query(Employee).filter(Employee.user_id == user.id, Employee.is_active == 1).all()
        items = []
        today = dt.datetime.utcnow().date()

        for e in employees:
            # الإقامة
            tier = _tier_for_days(_days_left(e.residency_expiry), "الإقامة منتهية")
            if tier:
                sev, label = tier
                items.append({"type": "residency", "severity": sev, "employee_id": e.id, "employee_name": e.name, "message": f"إقامة {e.name} {label}"})
            # الجواز
            tier = _tier_for_days(_days_left(e.passport_expiry), "الجواز منتهي")
            if tier:
                sev, label = tier
                items.append({"type": "passport", "severity": sev, "employee_id": e.id, "employee_name": e.name, "message": f"جواز {e.name} {label}"})
            # عيد الميلاد (خلال 7 أيام القادمة)
            if e.birth_date:
                try:
                    next_bd = e.birth_date.replace(year=today.year)
                except ValueError:
                    next_bd = e.birth_date.replace(year=today.year, day=28)
                if next_bd.date() < today:
                    try:
                        next_bd = next_bd.replace(year=today.year + 1)
                    except ValueError:
                        next_bd = next_bd.replace(year=today.year + 1, day=28)
                bd_days = (next_bd.date() - today).days
                if 0 <= bd_days <= 7:
                    items.append({"type": "birthday", "severity": "notice", "employee_id": e.id, "employee_name": e.name, "message": f"🎂 عيد ميلاد {e.name} بعد {bd_days} يوم" if bd_days else f"🎂 اليوم عيد ميلاد {e.name}"})

        contracts = db.query(EmployeeContract, Employee).join(Employee, Employee.id == EmployeeContract.employee_id).filter(
            EmployeeContract.user_id == user.id, EmployeeContract.status == "active",
        ).all()
        for c, e in contracts:
            tier = _tier_for_days(_days_left(c.end_date), "العقد منتهي")
            if tier:
                sev, label = tier
                items.append({"type": "contract", "severity": sev, "employee_id": e.id, "employee_name": e.name, "message": f"عقد {e.name} {label}"})
            if c.start_date and c.probation_days:
                probation_end = c.start_date + dt.timedelta(days=c.probation_days)
                p_days = (probation_end.date() - today).days
                if 0 <= p_days <= 7:
                    items.append({"type": "probation", "severity": "notice", "employee_id": e.id, "employee_name": e.name, "message": f"تنتهي فترة تجربة {e.name} بعد {p_days} يوم"})

        if today.day >= 25:
            now = dt.datetime.utcnow()
            paid_ids = {
                p.employee_id for p in db.query(Payroll).filter(
                    Payroll.user_id == user.id, Payroll.month == now.month, Payroll.year == now.year, Payroll.status == "paid",
                ).all()
            }
            for e in employees:
                if e.id not in paid_ids:
                    items.append({"type": "salary_unpaid", "severity": "medium", "employee_id": e.id, "employee_name": e.name, "message": f"{e.name} لم يستلم راتب هذا الشهر بعد"})

        order = {"critical": 0, "high": 1, "medium": 2, "notice": 3}
        items.sort(key=lambda x: order.get(x["severity"], 9))
        return {"items": items, "count": len(items)}
    finally:
        db.close()

