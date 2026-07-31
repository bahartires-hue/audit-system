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
    notes: str = ""


class EmployeeUpdate(BaseModel):
    name: Optional[str] = None
    position: Optional[str] = None
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
