from __future__ import annotations

import datetime as dt
import uuid
from typing import Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..auth_core import log_event, require_csrf, require_user
from ..db import SessionLocal
from ..models import AppSetting, Branch, Category, Customer, Expense, Item, Purchase, Sale, Supplier, Unit, User

router = APIRouter(prefix="/api/cashierko", tags=["cashierko"])


def _parse_date(v: str, field_name: str) -> dt.datetime:
    try:
        return dt.datetime.strptime((v or "").strip(), "%Y-%m-%d")
    except Exception:
        raise HTTPException(400, f"{field_name} يجب أن يكون بصيغة YYYY-MM-DD")


def _role_ok(user, roles: set[str]) -> bool:
    if int(getattr(user, "is_admin", 0) or 0) == 1:
        return True
    role = (getattr(user, "role_name", "") or "user").strip().lower()
    return role in roles


def _require_roles(user, roles: set[str]) -> None:
    if not _role_ok(user, roles):
        raise HTTPException(403, "ليس لديك صلاحية لهذه العملية")


def _permission_map(role: str) -> dict[str, bool]:
    role = (role or "user").strip().lower()
    base = {
        "dashboard.view": True,
        "items.view": True,
        "items.edit": False,
        "sales.create": False,
        "sales.edit": False,
        "purchases.create": False,
        "inventory.adjust": False,
        "returns.manage": False,
        "reports.view": True,
        "settings.manage": False,
        "users.manage": False,
        "transfers.manage": False,
    }
    if role in {"cashier"}:
        base.update({"sales.create": True, "sales.edit": True})
    if role in {"inventory"}:
        base.update({"items.edit": True, "inventory.adjust": True, "transfers.manage": True})
    if role in {"accountant"}:
        base.update({"purchases.create": True, "returns.manage": True, "reports.view": True})
    if role in {"manager"}:
        for k in list(base.keys()):
            if k != "users.manage":
                base[k] = True
    if role in {"admin"}:
        for k in list(base.keys()):
            base[k] = True
    return base


class BranchIn(BaseModel):
    code: str = Field(min_length=1, max_length=60)
    name: str = Field(min_length=1, max_length=200)
    city: str = ""
    address: str = ""
    is_main: bool = False
    is_active: bool = True


class CategoryIn(BaseModel):
    code: str = Field(min_length=1, max_length=60)
    name: str = Field(min_length=1, max_length=200)
    notes: str = ""


class UnitIn(BaseModel):
    code: str = Field(min_length=1, max_length=60)
    name: str = Field(min_length=1, max_length=120)
    notes: str = ""


class PartyIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    phone: str = ""
    city: str = ""
    address: str = ""
    opening_balance: float = 0.0
    notes: str = ""


class ExpenseIn(BaseModel):
    expense_date: str
    expense_type: str = Field(min_length=1, max_length=120)
    amount: float = Field(gt=0)
    payment_type: str = "cash"
    branch_id: str = ""
    description: str = ""
    notes: str = ""


class BranchPatch(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    is_main: Optional[bool] = None
    is_active: Optional[bool] = None


class CategoryPatch(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    notes: Optional[str] = None


class UnitPatch(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    notes: Optional[str] = None


class PartyPatch(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    opening_balance: Optional[float] = None
    notes: Optional[str] = None


class CashierkoSettingsPatch(BaseModel):
    shop_name: Optional[str] = None
    logo_url: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    tax_number: Optional[str] = None
    commercial_register: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    currency: Optional[str] = None
    tax_rate: Optional[float] = None
    invoice_prefix: Optional[str] = None
    print_size: Optional[str] = None
    thermal_header: Optional[str] = None


class UserRolePatch(BaseModel):
    user_id: str
    role_name: Optional[str] = None
    is_active: Optional[bool] = None


@router.get("/dashboard/overview")
def dashboard_overview(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        now = dt.datetime.utcnow()
        today = now.date()
        month_start = dt.datetime(now.year, now.month, 1)
        next_month = dt.datetime(now.year + (1 if now.month == 12 else 0), 1 if now.month == 12 else now.month + 1, 1)

        sales = db.query(Sale).filter(Sale.user_id == user.id).all()
        purchases = db.query(Purchase).filter(Purchase.user_id == user.id).all()
        items = db.query(Item).filter(Item.user_id == user.id).all()

        sales_today = [x for x in sales if x.sale_date and x.sale_date.date() == today]
        sales_month = [x for x in sales if x.sale_date and month_start <= x.sale_date < next_month]
        purchases_month = [x for x in purchases if x.purchase_date and month_start <= x.purchase_date < next_month]

        total_sales_today = round(sum(float(x.total_amount or 0.0) for x in sales_today), 2)
        total_sales_month = round(sum(float(x.total_amount or 0.0) for x in sales_month), 2)
        total_purchases_month = round(sum(float(x.total_amount or 0.0) for x in purchases_month), 2)
        inventory_value = round(sum(float(x.quantity or 0.0) * float(x.last_cost or 0.0) for x in items), 2)
        low_stock_count = sum(1 for x in items if float(x.quantity or 0.0) <= float(x.min_qty or 0.0))
        total_qty_sold_today = round(sum(float(ln.qty or 0.0) for s in sales_today for ln in (s.__dict__.get("_sale_lines_cache", []) or [])), 4)

        recent_sales = (
            db.query(Sale)
            .filter(Sale.user_id == user.id)
            .order_by(Sale.sale_date.desc(), Sale.created_at.desc())
            .limit(7)
            .all()
        )
        recent_purchases = (
            db.query(Purchase)
            .filter(Purchase.user_id == user.id)
            .order_by(Purchase.purchase_date.desc(), Purchase.created_at.desc())
            .limit(7)
            .all()
        )
        return {
            "totals": {
                "sales_today": total_sales_today,
                "sales_month": total_sales_month,
                "sales_invoices_today": len(sales_today),
                "items_sold_today": total_qty_sold_today,
                "purchases_month": total_purchases_month,
                "inventory_value": inventory_value,
                "low_stock_items": low_stock_count,
            },
            "recent_sales": [
                {
                    "id": x.id,
                    "invoice_no": x.invoice_no,
                    "customer_name": x.customer_name,
                    "sale_date": x.sale_date.strftime("%Y-%m-%d") if x.sale_date else "",
                    "total_amount": round(float(x.total_amount or 0.0), 2),
                }
                for x in recent_sales
            ],
            "recent_purchases": [
                {
                    "id": x.id,
                    "invoice_no": x.invoice_no,
                    "supplier_name": x.supplier_name,
                    "purchase_date": x.purchase_date.strftime("%Y-%m-%d") if x.purchase_date else "",
                    "total_amount": round(float(x.total_amount or 0.0), 2),
                }
                for x in recent_purchases
            ],
        }
    finally:
        db.close()


@router.get("/branches")
def list_branches(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Branch).filter(Branch.user_id == user.id).order_by(Branch.created_at.desc()).all()
        return {
            "items": [
                {
                    "id": x.id,
                    "code": x.code,
                    "name": x.name,
                    "city": x.city or "",
                    "address": x.address or "",
                    "is_main": bool(int(x.is_main or 0)),
                    "is_active": bool(int(x.is_active or 0)),
                }
                for x in rows
            ]
        }
    finally:
        db.close()


@router.post("/branches")
def create_branch(request: Request, body: BranchIn = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager"})
        rec = Branch(
            id=uuid.uuid4().hex,
            user_id=user.id,
            code=body.code.strip(),
            name=body.name.strip(),
            city=(body.city or "").strip() or None,
            address=(body.address or "").strip() or None,
            is_main=1 if body.is_main else 0,
            is_active=1 if body.is_active else 0,
        )
        db.add(rec)
        db.commit()
        log_event(db, "cashierko.branch.create", user.id, {"branch_id": rec.id})
        return {"id": rec.id}
    finally:
        db.close()


@router.patch("/branches/{branch_id}")
def patch_branch(branch_id: str, request: Request, body: BranchPatch = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager"})
        rec = db.query(Branch).filter(Branch.user_id == user.id, Branch.id == branch_id).first()
        if not rec:
            raise HTTPException(404, "الفرع غير موجود")
        if body.code is not None:
            rec.code = body.code.strip()
        if body.name is not None:
            rec.name = body.name.strip()
        if body.city is not None:
            rec.city = (body.city or "").strip() or None
        if body.address is not None:
            rec.address = (body.address or "").strip() or None
        if body.is_main is not None:
            rec.is_main = 1 if body.is_main else 0
        if body.is_active is not None:
            rec.is_active = 1 if body.is_active else 0
        db.commit()
        log_event(db, "cashierko.branch.patch", user.id, {"branch_id": rec.id})
        return {"ok": True}
    finally:
        db.close()


@router.delete("/branches/{branch_id}")
def delete_branch(branch_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager"})
        rec = db.query(Branch).filter(Branch.user_id == user.id, Branch.id == branch_id).first()
        if not rec:
            raise HTTPException(404, "الفرع غير موجود")
        db.delete(rec)
        db.commit()
        log_event(db, "cashierko.branch.delete", user.id, {"branch_id": branch_id})
        return {"deleted": True}
    finally:
        db.close()


@router.get("/categories")
def list_categories(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Category).filter(Category.user_id == user.id).order_by(Category.created_at.desc()).all()
        return {"items": [{"id": x.id, "code": x.code, "name": x.name, "notes": x.notes or ""} for x in rows]}
    finally:
        db.close()


@router.get("/units")
def list_units(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Unit).filter(Unit.user_id == user.id).order_by(Unit.created_at.desc()).all()
        return {"items": [{"id": x.id, "code": x.code, "name": x.name, "notes": x.notes or ""} for x in rows]}
    finally:
        db.close()


@router.post("/units")
def create_unit(request: Request, body: UnitIn = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "inventory"})
        rec = Unit(
            id=uuid.uuid4().hex,
            user_id=user.id,
            code=body.code.strip(),
            name=body.name.strip(),
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.commit()
        log_event(db, "cashierko.unit.create", user.id, {"unit_id": rec.id})
        return {"id": rec.id}
    finally:
        db.close()


@router.patch("/units/{unit_id}")
def patch_unit(unit_id: str, request: Request, body: UnitPatch = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "inventory"})
        rec = db.query(Unit).filter(Unit.user_id == user.id, Unit.id == unit_id).first()
        if not rec:
            raise HTTPException(404, "الوحدة غير موجودة")
        old_name = rec.name
        if body.code is not None:
            rec.code = body.code.strip()
        if body.name is not None:
            rec.name = body.name.strip()
        if body.notes is not None:
            rec.notes = (body.notes or "").strip() or None
        if rec.name != old_name:
            linked_items = db.query(Item).filter(Item.user_id == user.id, Item.unit == old_name).all()
            for it in linked_items:
                it.unit = rec.name
        db.commit()
        log_event(db, "cashierko.unit.patch", user.id, {"unit_id": rec.id})
        return {"ok": True}
    finally:
        db.close()


@router.delete("/units/{unit_id}")
def delete_unit(unit_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "inventory"})
        rec = db.query(Unit).filter(Unit.user_id == user.id, Unit.id == unit_id).first()
        if not rec:
            raise HTTPException(404, "الوحدة غير موجودة")
        linked = db.query(Item).filter(Item.user_id == user.id, Item.unit == rec.name).first()
        if linked:
            raise HTTPException(400, "لا يمكن حذف وحدة مرتبطة بأصناف")
        db.delete(rec)
        db.commit()
        log_event(db, "cashierko.unit.delete", user.id, {"unit_id": unit_id})
        return {"deleted": True}
    finally:
        db.close()


@router.post("/categories")
def create_category(request: Request, body: CategoryIn = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "inventory"})
        rec = Category(
            id=uuid.uuid4().hex,
            user_id=user.id,
            code=body.code.strip(),
            name=body.name.strip(),
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.commit()
        log_event(db, "cashierko.category.create", user.id, {"category_id": rec.id})
        return {"id": rec.id}
    finally:
        db.close()


@router.patch("/categories/{category_id}")
def patch_category(category_id: str, request: Request, body: CategoryPatch = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "inventory"})
        rec = db.query(Category).filter(Category.user_id == user.id, Category.id == category_id).first()
        if not rec:
            raise HTTPException(404, "التصنيف غير موجود")
        if body.code is not None:
            rec.code = body.code.strip()
        if body.name is not None:
            rec.name = body.name.strip()
        if body.notes is not None:
            rec.notes = (body.notes or "").strip() or None
        db.commit()
        log_event(db, "cashierko.category.patch", user.id, {"category_id": rec.id})
        return {"ok": True}
    finally:
        db.close()


@router.delete("/categories/{category_id}")
def delete_category(category_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "inventory"})
        rec = db.query(Category).filter(Category.user_id == user.id, Category.id == category_id).first()
        if not rec:
            raise HTTPException(404, "التصنيف غير موجود")
        db.delete(rec)
        db.commit()
        log_event(db, "cashierko.category.delete", user.id, {"category_id": category_id})
        return {"deleted": True}
    finally:
        db.close()


@router.get("/suppliers")
def list_suppliers(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Supplier).filter(Supplier.user_id == user.id).order_by(Supplier.created_at.desc()).all()
        items = []
        for x in rows:
            p_total = sum(float(p.total_amount or 0.0) for p in db.query(Purchase).filter(Purchase.user_id == user.id, Purchase.supplier_name == x.name).all())
            items.append(
                {
                    "id": x.id,
                    "name": x.name,
                    "phone": x.phone or "",
                    "city": x.city or "",
                    "address": x.address or "",
                    "opening_balance": round(float(x.opening_balance or 0.0), 2),
                    "total_purchases": round(p_total, 2),
                    "balance_due": round(float(x.opening_balance or 0.0) + p_total, 2),
                    "notes": x.notes or "",
                }
            )
        return {"items": items}
    finally:
        db.close()


@router.post("/suppliers")
def create_supplier(request: Request, body: PartyIn = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "accountant"})
        rec = Supplier(
            id=uuid.uuid4().hex,
            user_id=user.id,
            name=body.name.strip(),
            phone=(body.phone or "").strip() or None,
            city=(body.city or "").strip() or None,
            address=(body.address or "").strip() or None,
            opening_balance=round(float(body.opening_balance or 0.0), 2),
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.commit()
        log_event(db, "cashierko.supplier.create", user.id, {"supplier_id": rec.id})
        return {"id": rec.id}
    finally:
        db.close()


@router.patch("/suppliers/{supplier_id}")
def patch_supplier(supplier_id: str, request: Request, body: PartyPatch = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "accountant"})
        rec = db.query(Supplier).filter(Supplier.user_id == user.id, Supplier.id == supplier_id).first()
        if not rec:
            raise HTTPException(404, "المورد غير موجود")
        if body.name is not None:
            rec.name = body.name.strip()
        if body.phone is not None:
            rec.phone = (body.phone or "").strip() or None
        if body.city is not None:
            rec.city = (body.city or "").strip() or None
        if body.address is not None:
            rec.address = (body.address or "").strip() or None
        if body.opening_balance is not None:
            rec.opening_balance = round(float(body.opening_balance or 0.0), 2)
        if body.notes is not None:
            rec.notes = (body.notes or "").strip() or None
        db.commit()
        log_event(db, "cashierko.supplier.patch", user.id, {"supplier_id": rec.id})
        return {"ok": True}
    finally:
        db.close()


@router.delete("/suppliers/{supplier_id}")
def delete_supplier(supplier_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "accountant"})
        rec = db.query(Supplier).filter(Supplier.user_id == user.id, Supplier.id == supplier_id).first()
        if not rec:
            raise HTTPException(404, "المورد غير موجود")
        db.delete(rec)
        db.commit()
        log_event(db, "cashierko.supplier.delete", user.id, {"supplier_id": supplier_id})
        return {"deleted": True}
    finally:
        db.close()


@router.get("/customers")
def list_customers(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Customer).filter(Customer.user_id == user.id).order_by(Customer.created_at.desc()).all()
        items = []
        for x in rows:
            s_total = sum(float(s.total_amount or 0.0) for s in db.query(Sale).filter(Sale.user_id == user.id, Sale.customer_name == x.name).all())
            items.append(
                {
                    "id": x.id,
                    "name": x.name,
                    "phone": x.phone or "",
                    "city": x.city or "",
                    "address": x.address or "",
                    "opening_balance": round(float(x.opening_balance or 0.0), 2),
                    "total_sales": round(s_total, 2),
                    "balance_due": round(float(x.opening_balance or 0.0) + s_total, 2),
                    "notes": x.notes or "",
                }
            )
        return {"items": items}
    finally:
        db.close()


@router.post("/customers")
def create_customer(request: Request, body: PartyIn = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "cashier", "accountant"})
        rec = Customer(
            id=uuid.uuid4().hex,
            user_id=user.id,
            name=body.name.strip(),
            phone=(body.phone or "").strip() or None,
            city=(body.city or "").strip() or None,
            address=(body.address or "").strip() or None,
            opening_balance=round(float(body.opening_balance or 0.0), 2),
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.commit()
        log_event(db, "cashierko.customer.create", user.id, {"customer_id": rec.id})
        return {"id": rec.id}
    finally:
        db.close()


@router.patch("/customers/{customer_id}")
def patch_customer(customer_id: str, request: Request, body: PartyPatch = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "cashier", "accountant"})
        rec = db.query(Customer).filter(Customer.user_id == user.id, Customer.id == customer_id).first()
        if not rec:
            raise HTTPException(404, "العميل غير موجود")
        if body.name is not None:
            rec.name = body.name.strip()
        if body.phone is not None:
            rec.phone = (body.phone or "").strip() or None
        if body.city is not None:
            rec.city = (body.city or "").strip() or None
        if body.address is not None:
            rec.address = (body.address or "").strip() or None
        if body.opening_balance is not None:
            rec.opening_balance = round(float(body.opening_balance or 0.0), 2)
        if body.notes is not None:
            rec.notes = (body.notes or "").strip() or None
        db.commit()
        log_event(db, "cashierko.customer.patch", user.id, {"customer_id": rec.id})
        return {"ok": True}
    finally:
        db.close()


@router.delete("/customers/{customer_id}")
def delete_customer(customer_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "cashier", "accountant"})
        rec = db.query(Customer).filter(Customer.user_id == user.id, Customer.id == customer_id).first()
        if not rec:
            raise HTTPException(404, "العميل غير موجود")
        db.delete(rec)
        db.commit()
        log_event(db, "cashierko.customer.delete", user.id, {"customer_id": customer_id})
        return {"deleted": True}
    finally:
        db.close()


@router.get("/expenses")
def list_expenses(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    expense_type: str = Query(""),
    limit: int = Query(200, ge=1, le=1000),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Expense).filter(Expense.user_id == user.id)
        if (date_from or "").strip():
            q = q.filter(Expense.expense_date >= _parse_date(date_from, "date_from"))
        if (date_to or "").strip():
            q = q.filter(Expense.expense_date <= _parse_date(date_to, "date_to"))
        if (expense_type or "").strip():
            q = q.filter(Expense.expense_type == expense_type.strip())
        rows = q.order_by(Expense.expense_date.desc(), Expense.created_at.desc()).limit(limit).all()
        total = round(sum(float(x.amount or 0.0) for x in rows), 2)
        return {
            "items": [
                {
                    "id": x.id,
                    "expense_date": x.expense_date.strftime("%Y-%m-%d"),
                    "expense_type": x.expense_type,
                    "amount": round(float(x.amount or 0.0), 2),
                    "payment_type": x.payment_type,
                    "description": x.description or "",
                    "notes": x.notes or "",
                }
                for x in rows
            ],
            "totals": {"amount": total},
        }
    finally:
        db.close()


@router.post("/expenses")
def create_expense(request: Request, body: ExpenseIn = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "accountant"})
        rec = Expense(
            id=uuid.uuid4().hex,
            user_id=user.id,
            branch_id=(body.branch_id or "").strip() or None,
            expense_date=_parse_date(body.expense_date, "expense_date"),
            expense_type=body.expense_type.strip(),
            amount=round(abs(float(body.amount or 0.0)), 2),
            payment_type=(body.payment_type or "cash").strip().lower(),
            description=(body.description or "").strip() or None,
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.commit()
        log_event(db, "cashierko.expense.create", user.id, {"expense_id": rec.id, "amount": rec.amount})
        return {"id": rec.id}
    finally:
        db.close()


@router.delete("/expenses/{expense_id}")
def delete_expense(expense_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "accountant"})
        rec = db.query(Expense).filter(Expense.user_id == user.id, Expense.id == expense_id).first()
        if not rec:
            raise HTTPException(404, "المصروف غير موجود")
        db.delete(rec)
        db.commit()
        log_event(db, "cashierko.expense.delete", user.id, {"expense_id": expense_id})
        return {"deleted": True}
    finally:
        db.close()


@router.get("/settings")
def get_cashierko_settings(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        key = f"cashierko_settings:{user.id}"
        row = db.query(AppSetting).filter(AppSetting.key == key).first()
        defaults = {
            "shop_name": "SmartPOS",
            "logo_url": "",
            "phone": "",
            "address": "",
            "tax_number": "",
            "commercial_register": "",
            "postal_code": "",
            "country": "",
            "city": "",
            "currency": "SAR",
            "tax_rate": 0.0,
            "invoice_prefix": "INV",
            "print_size": "A4",
            "thermal_header": "شكراً لزيارتكم",
        }
        data = row.value_json if row and isinstance(row.value_json, dict) else {}
        out = {**defaults, **data}
        return {"settings": out}
    finally:
        db.close()


@router.get("/permissions/me")
def get_my_permissions(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        role = "admin" if int(user.is_admin or 0) == 1 else (user.role_name or "user")
        return {"role_name": role, "permissions": _permission_map(role)}
    finally:
        db.close()


@router.get("/users")
def list_users_admin(request: Request, limit: int = Query(200, ge=1, le=1000)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        _require_roles(user, {"admin"})
        rows = db.query(User).order_by(User.created_at.desc()).limit(limit).all()
        return {
            "items": [
                {
                    "id": u.id,
                    "username": u.username,
                    "email": u.email or "",
                    "role_name": u.role_name or ("admin" if int(u.is_admin or 0) else "user"),
                    "is_active": bool(int(u.is_active or 0)),
                    "plan_name": u.plan_name or "free",
                }
                for u in rows
            ]
        }
    finally:
        db.close()


@router.patch("/users")
def patch_user_admin(request: Request, body: UserRolePatch = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin"})
        target = db.query(User).filter(User.id == body.user_id).first()
        if not target:
            raise HTTPException(404, "المستخدم غير موجود")
        if body.role_name is not None:
            role = (body.role_name or "user").strip().lower()
            allowed = {"user", "cashier", "inventory", "accountant", "manager", "admin"}
            if role not in allowed:
                raise HTTPException(400, "الدور غير مدعوم")
            target.role_name = role
            target.is_admin = 1 if role == "admin" else 0
        if body.is_active is not None:
            target.is_active = 1 if body.is_active else 0
        db.commit()
        log_event(db, "cashierko.user.patch", user.id, {"target_user_id": target.id, "role_name": target.role_name, "is_active": int(target.is_active or 0)})
        return {"ok": True}
    finally:
        db.close()


@router.patch("/settings")
def patch_cashierko_settings(request: Request, body: CashierkoSettingsPatch = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _require_roles(user, {"admin", "manager", "accountant"})
        key = f"cashierko_settings:{user.id}"
        row = db.query(AppSetting).filter(AppSetting.key == key).first()
        cur = row.value_json if row and isinstance(row.value_json, dict) else {}
        patch = body.model_dump(exclude_unset=True)
        if "tax_rate" in patch and patch["tax_rate"] is not None:
            patch["tax_rate"] = round(abs(float(patch["tax_rate"] or 0.0)), 4)
        merged = {**cur, **patch}
        if not row:
            row = AppSetting(key=key, value_json=merged, updated_at=dt.datetime.utcnow())
            db.add(row)
        else:
            row.value_json = merged
            row.updated_at = dt.datetime.utcnow()
        db.commit()
        log_event(db, "cashierko.settings.patch", user.id, {"keys": list(patch.keys())})
        return {"settings": merged}
    finally:
        db.close()


@router.get("/customers/{customer_id}/statement")
def customer_statement(customer_id: str, request: Request, date_from: str = Query(""), date_to: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        customer = db.query(Customer).filter(Customer.user_id == user.id, Customer.id == customer_id).first()
        if not customer:
            raise HTTPException(404, "العميل غير موجود")
        q = db.query(Sale).filter(Sale.user_id == user.id, Sale.customer_name == customer.name)
        if (date_from or "").strip():
            q = q.filter(Sale.sale_date >= _parse_date(date_from, "date_from"))
        if (date_to or "").strip():
            q = q.filter(Sale.sale_date <= _parse_date(date_to, "date_to"))
        rows = q.order_by(Sale.sale_date.asc(), Sale.created_at.asc()).all()
        items = []
        for x in rows:
            total = round(float(x.total_amount or 0.0), 2)
            paid = round(float(x.paid_amount or 0.0), 2)
            due = round(float(x.due_amount or max(0.0, total - paid)), 2)
            items.append(
                {
                    "date": x.sale_date.strftime("%Y-%m-%d"),
                    "invoice_no": x.invoice_no,
                    "description": "فاتورة بيع",
                    "debit": total,
                    "credit": paid,
                    "balance_delta": due,
                }
            )
        opening = round(float(customer.opening_balance or 0.0), 2)
        closing = round(opening + sum(float(i["balance_delta"]) for i in items), 2)
        return {"customer": {"id": customer.id, "name": customer.name}, "opening_balance": opening, "closing_balance": closing, "items": items}
    finally:
        db.close()


@router.get("/suppliers/{supplier_id}/statement")
def supplier_statement(supplier_id: str, request: Request, date_from: str = Query(""), date_to: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        supplier = db.query(Supplier).filter(Supplier.user_id == user.id, Supplier.id == supplier_id).first()
        if not supplier:
            raise HTTPException(404, "المورد غير موجود")
        q = db.query(Purchase).filter(Purchase.user_id == user.id, Purchase.supplier_name == supplier.name)
        if (date_from or "").strip():
            q = q.filter(Purchase.purchase_date >= _parse_date(date_from, "date_from"))
        if (date_to or "").strip():
            q = q.filter(Purchase.purchase_date <= _parse_date(date_to, "date_to"))
        rows = q.order_by(Purchase.purchase_date.asc(), Purchase.created_at.asc()).all()
        items = []
        for x in rows:
            total = round(float(x.total_amount or 0.0), 2)
            paid = round(float(x.paid_amount or 0.0), 2)
            due = round(float(x.due_amount or max(0.0, total - paid)), 2)
            items.append(
                {
                    "date": x.purchase_date.strftime("%Y-%m-%d"),
                    "invoice_no": x.invoice_no,
                    "description": "فاتورة شراء",
                    "debit": paid,
                    "credit": total,
                    "balance_delta": due,
                }
            )
        opening = round(float(supplier.opening_balance or 0.0), 2)
        closing = round(opening + sum(float(i["balance_delta"]) for i in items), 2)
        return {"supplier": {"id": supplier.id, "name": supplier.name}, "opening_balance": opening, "closing_balance": closing, "items": items}
    finally:
        db.close()

