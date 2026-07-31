from __future__ import annotations

import datetime as dt
import uuid
from typing import Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..auth_core import log_event, require_csrf, require_user
from ..db import SessionLocal
from ..models import Employee, Payroll, PayrollAllowance, PayrollDeduction

router = APIRouter(prefix="/api/hr", tags=["hr"])


class EmployeeCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    position: str = ""
    department: str = ""
    branch_name: str = ""
    notes: str = ""


class EmployeeUpdate(BaseModel):
    name: Optional[str] = None
    position: Optional[str] = None
    department: Optional[str] = None
    branch_name: Optional[str] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


class LineItemIn(BaseModel):
    label: str = Field(min_length=1, max_length=120)
    amount: float = Field(ge=0)


class PayrollCreate(BaseModel):
    employee_id: str
    month: int = Field(ge=1, le=12)
    year: int = Field(ge=2000, le=2100)
    base_salary: float = Field(ge=0)
    allowances: list[LineItemIn] = []
    deductions: list[LineItemIn] = []
    notes: str = ""


def _employee_out(e: Employee) -> dict:
    return {
        "id": e.id,
        "name": e.name,
        "position": e.position or "",
        "department": e.department or "",
        "branch_name": e.branch_name or "",
        "is_active": bool(int(e.is_active or 0)),
        "notes": e.notes or "",
    }


@router.get("/employees")
def list_employees(request: Request, q: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Employee).filter(Employee.user_id == user.id).order_by(Employee.created_at.desc()).all()
        qn = (q or "").strip().lower()
        if qn:
            rows = [r for r in rows if qn in (r.name or "").lower()]
        return {"items": [_employee_out(r) for r in rows]}
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
            name=body.name.strip(),
            position=(body.position or "").strip() or None,
            department=(body.department or "").strip() or None,
            branch_name=(body.branch_name or "").strip() or None,
            is_active=1,
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.commit()
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
        if body.name is not None and body.name.strip():
            rec.name = body.name.strip()
        if body.position is not None:
            rec.position = body.position.strip() or None
        if body.department is not None:
            rec.department = body.department.strip() or None
        if body.branch_name is not None:
            rec.branch_name = body.branch_name.strip() or None
        if body.is_active is not None:
            rec.is_active = 1 if body.is_active else 0
        if body.notes is not None:
            rec.notes = body.notes.strip() or None
        db.commit()
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
        has_payroll = db.query(Payroll).filter(Payroll.employee_id == rec.id).first()
        if has_payroll:
            raise HTTPException(400, "لا يمكن حذف الموظف لوجود رواتب مسجلة له")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


def _payroll_out(db, p: Payroll, employee_name: str) -> dict:
    return {
        "id": p.id,
        "employee_id": p.employee_id,
        "employee_name": employee_name,
        "month": p.month,
        "year": p.year,
        "base_salary": round(float(p.base_salary or 0.0), 2),
        "total_allowances": round(float(p.total_allowances or 0.0), 2),
        "total_deductions": round(float(p.total_deductions or 0.0), 2),
        "net_salary": round(float(p.net_salary or 0.0), 2),
        "status": p.status,
        "paid_at": p.paid_at.strftime("%Y-%m-%d %H:%M") if p.paid_at else "",
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
        if (status or "").strip():
            q = q.filter(Payroll.status == status.strip())
        if (employee_id or "").strip():
            q = q.filter(Payroll.employee_id == employee_id.strip())
        rows = q.order_by(Payroll.year.desc(), Payroll.month.desc(), Payroll.created_at.desc()).all()
        return {"items": [_payroll_out(db, p, e.name) for p, e in rows]}
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
            raise HTTPException(400, "الموظف غير صالح")
        total_allow = round(sum(abs(float(a.amount or 0.0)) for a in body.allowances), 2)
        total_ded = round(sum(abs(float(d.amount or 0.0)) for d in body.deductions), 2)
        base = round(abs(float(body.base_salary or 0.0)), 2)
        net = round(base + total_allow - total_ded, 2)
        rec = Payroll(
            id=uuid.uuid4().hex,
            user_id=user.id,
            employee_id=employee.id,
            month=int(body.month),
            year=int(body.year),
            base_salary=base,
            total_allowances=total_allow,
            total_deductions=total_ded,
            net_salary=net,
            status="unpaid",
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.flush()
        for a in body.allowances:
            if float(a.amount or 0.0) <= 0:
                continue
            db.add(PayrollAllowance(payroll_id=rec.id, label=a.label.strip(), amount=round(abs(float(a.amount)), 2)))
        for d in body.deductions:
            if float(d.amount or 0.0) <= 0:
                continue
            db.add(PayrollDeduction(payroll_id=rec.id, label=d.label.strip(), amount=round(abs(float(d.amount)), 2)))
        db.commit()
        log_event(db, "hr.payroll.create", user.id, {"payroll_id": rec.id, "employee_id": employee.id, "net_salary": net})
        return {"id": rec.id, "net_salary": net}
    finally:
        db.close()


@router.get("/payrolls/{payroll_id}")
def payroll_details(payroll_id: str, request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        p = db.query(Payroll).filter(Payroll.user_id == user.id, Payroll.id == payroll_id).first()
        if not p:
            raise HTTPException(404, "الراتب غير موجود")
        employee = db.query(Employee).filter(Employee.id == p.employee_id).first()
        allowances = db.query(PayrollAllowance).filter(PayrollAllowance.payroll_id == p.id).all()
        deductions = db.query(PayrollDeduction).filter(PayrollDeduction.payroll_id == p.id).all()
        out = _payroll_out(db, p, employee.name if employee else "")
        out["employee_position"] = (employee.position if employee else "") or ""
        out["allowances"] = [{"label": a.label, "amount": round(float(a.amount or 0.0), 2)} for a in allowances]
        out["deductions"] = [{"label": d.label, "amount": round(float(d.amount or 0.0), 2)} for d in deductions]
        out["notes"] = p.notes or ""
        return out
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
            raise HTTPException(400, "تم صرف هذا الراتب مسبقاً")
        p.status = "paid"
        p.paid_at = dt.datetime.utcnow()
        db.commit()
        log_event(db, "hr.payroll.pay", user.id, {"payroll_id": p.id})
        return {"ok": True, "paid_at": p.paid_at.strftime("%Y-%m-%d %H:%M")}
    finally:
        db.close()


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
            raise HTTPException(400, "لا يمكن حذف راتب تم صرفه")
        db.query(PayrollAllowance).filter(PayrollAllowance.payroll_id == p.id).delete()
        db.query(PayrollDeduction).filter(PayrollDeduction.payroll_id == p.id).delete()
        db.delete(p)
        db.commit()
        return {"deleted": True}
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
        total = round(sum(float(p.net_salary or 0.0) for p in rows), 2)
        paid = round(sum(float(p.net_salary or 0.0) for p in rows if p.status == "paid"), 2)
        unpaid = round(sum(float(p.net_salary or 0.0) for p in rows if p.status != "paid"), 2)
        return {
            "employees_count": employees_count,
            "total_this_month": total,
            "paid_amount": paid,
            "unpaid_amount": unpaid,
        }
    finally:
        db.close()
# ============================================================
# الوثائق الرسمية للموظفين + التنبيهات الذكية + سجل التجديد
# ============================================================

import os as _os
from pathlib import Path as _Path

from fastapi import Form, File as _FileParam, UploadFile as _UploadFileParam

from ..models import EmployeeDocument, EmployeeDocumentRenewal

_BASE_DIR = _Path(__file__).resolve().parents[3]
_data_root = (_os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
_UPLOAD_DIR = (_Path(_data_root) / "uploads") if _data_root else (_BASE_DIR / "uploads")
_HR_DOCS_DIR = _UPLOAD_DIR / "hr_documents"
_HR_DOCS_DIR.mkdir(parents=True, exist_ok=True)
_ALLOWED_DOC_SUFFIXES = (".pdf", ".png", ".jpg", ".jpeg")
_MAX_DOC_MB = 10

_DOC_TYPE_MATCHERS = {
    "residency": ["اقامة", "إقامة"],
    "passport": ["جواز"],
    "work_license": ["رخصة عمل", "رخصة العمل"],
}


def _parse_simple_date(s: str, field_name: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, f"{field_name} يجب أن يكون بصيغة YYYY-MM-DD")


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
    file: Optional[_UploadFileParam] = _FileParam(None),
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
async def upload_document_file(document_id: str, request: Request, file: _UploadFileParam = _FileParam(...)):
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


@router.get("/documents/dashboard-summary")
def documents_dashboard_summary(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = (
            db.query(EmployeeDocument)
            .filter(EmployeeDocument.user_id == user.id)
            .all()
        )
        def matches(doc, keys):
            t = (doc.doc_type or "").lower()
            return any(k.lower() in t for k in keys)

        residency_soon = 0
        passport_expired = 0
        work_license_soon = 0
        needs_update = 0
        for d in rows:
            st = _doc_status(d.expiry_date)
            if st["days_left"] is not None and 0 <= st["days_left"] <= 30 and matches(d, _DOC_TYPE_MATCHERS["residency"]):
                residency_soon += 1
            if st["status"] == "expired" and matches(d, _DOC_TYPE_MATCHERS["passport"]):
                passport_expired += 1
            if st["days_left"] is not None and 0 <= st["days_left"] <= 30 and matches(d, _DOC_TYPE_MATCHERS["work_license"]):
                work_license_soon += 1
            if st["status"] != "active":
                needs_update += 1
        return {
            "residency_expiring_soon": residency_soon,
            "passports_expired": passport_expired,
            "work_licenses_expiring_soon": work_license_soon,
            "contracts_expiring_soon": 0,
            "documents_need_update": needs_update,
        }
    finally:
        db.close()

