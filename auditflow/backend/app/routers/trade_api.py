from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..auth_core import require_csrf, require_user
from ..db import SessionLocal
from ..models import BranchTransfer, BranchTransferLine, Item, Purchase, PurchaseLine, Sale, SaleLine, StockAdjustment, StockMovement
from ..models import Branch, Customer, Supplier
from ..models import SuspendedSale, SuspendedSaleLine
from ..models import ReturnLine, ReturnTxn

router = APIRouter(prefix="/api/trade", tags=["trade"])


class ItemCreate(BaseModel):
    code: str = Field(min_length=1, max_length=60)
    name: str = Field(min_length=1, max_length=200)
    category: str = "rim"
    brand: str = ""
    size: str = ""
    pcd: str = ""
    color: str = ""
    item_condition: str = ""
    location: str = ""
    is_set: bool = False
    default_sale_price: float = 0.0
    notes: str = ""
    image_url: str = ""


class ItemUpdate(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    category: Optional[str] = None
    brand: Optional[str] = None
    size: Optional[str] = None
    pcd: Optional[str] = None
    color: Optional[str] = None
    item_condition: Optional[str] = None
    location: Optional[str] = None
    is_set: Optional[bool] = None
    default_sale_price: Optional[float] = None
    notes: Optional[str] = None
    image_url: Optional[str] = None


class PurchaseLineIn(BaseModel):
    item_id: str
    qty: float = Field(gt=0)
    unit_cost: float = Field(ge=0)
    extra_cost: float = Field(ge=0, default=0)


class PurchaseCreate(BaseModel):
    invoice_no: str = Field(min_length=1, max_length=80)
    supplier_name: str = Field(min_length=1, max_length=200)
    purchase_date: str
    branch_id: str = ""
    supplier_id: str = ""
    payment_type: str = "cash"
    tax_amount: float = Field(ge=0, default=0)
    discount: float = Field(ge=0, default=0)
    paid_amount: float = Field(ge=0, default=0)
    notes: str = ""
    lines: list[PurchaseLineIn]


class SaleLineIn(BaseModel):
    item_id: str
    qty: float = Field(gt=0)
    sale_price: float = Field(ge=0)
    tax_amount: float = Field(ge=0, default=0)


class SaleCreate(BaseModel):
    invoice_no: str = Field(min_length=1, max_length=80)
    customer_name: str = Field(min_length=1, max_length=200)
    sale_date: str
    payment_type: str = "cash"
    customer_id: str = ""
    branch_id: str = ""
    discount: float = Field(ge=0, default=0)
    paid_amount: float = Field(ge=0, default=0)
    notes: str = ""
    seller_name: str = ""
    branch_name: str = ""
    lines: list[SaleLineIn]


class SaleUpdate(SaleCreate):
    pass


class SaleSuspendCreate(SaleCreate):
    pass


class ReturnLineIn(BaseModel):
    item_id: str
    qty: float = Field(gt=0)
    unit_price: float = Field(ge=0, default=0)
    unit_cost: float = Field(ge=0, default=0)


class SaleReturnCreate(BaseModel):
    sale_id: str
    return_date: str
    reason: str = ""
    lines: list[ReturnLineIn]


class PurchaseReturnCreate(BaseModel):
    purchase_id: str
    return_date: str
    reason: str = ""
    lines: list[ReturnLineIn]


class StockAdjustCreate(BaseModel):
    item_id: str
    adjust_date: str
    qty_after: float
    reason: str = Field(min_length=1, max_length=500)


class BranchTransferLineIn(BaseModel):
    from_item_id: str
    to_item_id: str
    qty: float = Field(gt=0)


class BranchTransferCreate(BaseModel):
    transfer_no: str = Field(min_length=1, max_length=80)
    transfer_date: str
    from_branch_id: str
    to_branch_id: str
    notes: str = ""
    lines: list[BranchTransferLineIn]


def _parse_date(v: str, field_name: str) -> dt.datetime:
    try:
        return dt.datetime.strptime((v or "").strip(), "%Y-%m-%d")
    except Exception:
        raise HTTPException(400, f"{field_name} يجب أن يكون بصيغة YYYY-MM-DD")


def _available_qty(db: Any, user_id: str, item_id: str) -> float:
    rows = (
        db.query(StockMovement)
        .filter(StockMovement.user_id == user_id, StockMovement.item_id == item_id)
        .all()
    )
    return round(sum(float(x.qty_in or 0.0) - float(x.qty_out or 0.0) for x in rows), 4)


def _avg_cost(db: Any, user_id: str, item_id: str) -> float:
    rows = (
        db.query(StockMovement)
        .filter(
            StockMovement.user_id == user_id,
            StockMovement.item_id == item_id,
            StockMovement.qty_in > 0,
        )
        .all()
    )
    qty = sum(float(x.qty_in or 0.0) for x in rows)
    amt = sum(float(x.qty_in or 0.0) * float(x.unit_cost or 0.0) for x in rows)
    if qty <= 0:
        return 0.0
    return round(amt / qty, 4)


def _returned_qty(db: Any, user_id: str, reference_type: str, reference_id: str, item_id: str) -> float:
    rows = (
        db.query(ReturnLine, ReturnTxn)
        .join(ReturnTxn, ReturnTxn.id == ReturnLine.return_id)
        .filter(
            ReturnTxn.user_id == user_id,
            ReturnTxn.reference_type == reference_type,
            ReturnTxn.reference_id == reference_id,
            ReturnLine.item_id == item_id,
        )
        .all()
    )
    return round(sum(float(ln.qty or 0.0) for ln, _ in rows), 4)


def _reverse_sale_effects(db: Any, user_id: str, sale_id: str) -> None:
    sale_lines = db.query(SaleLine).filter(SaleLine.sale_id == sale_id).all()
    item_ids = {x.item_id for x in sale_lines}
    items = db.query(Item).filter(Item.user_id == user_id, Item.id.in_(list(item_ids))).all() if item_ids else []
    item_by_id = {x.id: x for x in items}
    for ln in sale_lines:
        item = item_by_id.get(ln.item_id)
        if item:
            item.quantity = round(float(item.quantity or 0.0) + float(ln.qty or 0.0), 4)
    for mv in db.query(StockMovement).filter(
        StockMovement.user_id == user_id,
        StockMovement.reference_type == "sale",
        StockMovement.reference_id == sale_id,
    ).all():
        db.delete(mv)
    for ln in sale_lines:
        db.delete(ln)


@router.get("/items")
def list_items(request: Request, q: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        query = db.query(Item).filter(Item.user_id == user.id)
        qn = (q or "").strip().lower()
        if qn:
            rows = query.order_by(Item.created_at.desc()).all()
            rows = [r for r in rows if qn in (f"{r.code} {r.name} {r.brand or ''} {r.size or ''}").lower()]
        else:
            rows = query.order_by(Item.created_at.desc()).all()
        return {
            "items": [
                {
                    "id": r.id,
                    "code": r.code,
                    "name": r.name,
                    "category": r.category,
                    "brand": r.brand or "",
                    "size": r.size or "",
                    "pcd": r.pcd or "",
                    "color": r.color or "",
                    "item_condition": r.item_condition or "",
                    "location": r.location or "",
                    "branch_id": r.branch_id or "",
                    "category_id": r.category_id or "",
                    "is_set": bool(int(r.is_set or 0)),
                    "is_unique": bool(int(r.is_unique or 0)),
                    "needs_service": bool(int(r.needs_service or 0)),
                    "quantity": round(float(r.quantity or 0.0), 4),
                    "min_qty": round(float(r.min_qty or 0.0), 4),
                    "default_sale_price": round(float(r.default_sale_price or 0.0), 2),
                    "last_cost": round(float(r.last_cost or 0.0), 2),
                    "notes": r.notes or "",
                    "image_url": r.image_url or "",
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@router.post("/items")
def create_item(request: Request, body: ItemCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        code = body.code.strip()
        if db.query(Item).filter(Item.user_id == user.id, Item.code == code).first():
            raise HTTPException(400, "كود الصنف مستخدم مسبقاً")
        rec = Item(
            id=uuid.uuid4().hex,
            user_id=user.id,
            code=code,
            name=body.name.strip(),
            category=(body.category or "rim").strip().lower(),
            brand=(body.brand or "").strip() or None,
            size=(body.size or "").strip() or None,
            pcd=(body.pcd or "").strip() or None,
            color=(body.color or "").strip() or None,
            item_condition=(body.item_condition or "").strip() or None,
            location=(body.location or "").strip() or None,
            is_set=1 if body.is_set else 0,
            quantity=0.0,
            default_sale_price=round(abs(float(body.default_sale_price or 0.0)), 2),
            last_cost=0.0,
            notes=(body.notes or "").strip() or None,
            image_url=(body.image_url or "").strip() or None,
        )
        db.add(rec)
        db.commit()
        return {"id": rec.id, "code": rec.code, "name": rec.name}
    finally:
        db.close()


@router.patch("/items/{item_id}")
def update_item(item_id: str, request: Request, body: ItemUpdate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(Item).filter(Item.user_id == user.id, Item.id == item_id).first()
        if not rec:
            raise HTTPException(404, "الصنف غير موجود")
        if body.code is not None:
            code = body.code.strip()
            if not code:
                raise HTTPException(400, "code لا يمكن أن يكون فارغاً")
            dup = db.query(Item).filter(Item.user_id == user.id, Item.code == code, Item.id != rec.id).first()
            if dup:
                raise HTTPException(400, "كود الصنف مستخدم مسبقاً")
            rec.code = code
        for k in ("name", "category", "brand", "size", "pcd", "color", "item_condition", "location", "notes", "image_url"):
            v = getattr(body, k)
            if v is not None:
                vv = str(v).strip()
                setattr(rec, k, vv if vv else None)
        if body.name is not None and not (body.name or "").strip():
            raise HTTPException(400, "name لا يمكن أن يكون فارغاً")
        if body.name is not None:
            rec.name = body.name.strip()
        if body.is_set is not None:
            rec.is_set = 1 if body.is_set else 0
        if body.default_sale_price is not None:
            rec.default_sale_price = round(abs(float(body.default_sale_price or 0.0)), 2)
        db.commit()
        return {"ok": True}
    finally:
        db.close()


@router.delete("/items/{item_id}")
def delete_item(item_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec = db.query(Item).filter(Item.user_id == user.id, Item.id == item_id).first()
        if not rec:
            raise HTTPException(404, "الصنف غير موجود")
        has_purchase = db.query(PurchaseLine).filter(PurchaseLine.item_id == rec.id).first()
        has_sale = db.query(SaleLine).filter(SaleLine.item_id == rec.id).first()
        if has_purchase or has_sale:
            raise HTTPException(400, "لا يمكن حذف الصنف لوجود حركات مرتبطة")
        db.delete(rec)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


@router.post("/purchases")
def create_purchase(request: Request, body: PurchaseCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        p_date = _parse_date(body.purchase_date, "purchase_date")
        if not body.lines:
            raise HTTPException(400, "الفاتورة تحتاج بنوداً")
        item_ids = {x.item_id for x in body.lines}
        items = db.query(Item).filter(Item.user_id == user.id, Item.id.in_(list(item_ids))).all()
        if len(items) != len(item_ids):
            raise HTTPException(400, "يوجد صنف غير صالح")
        item_by_id = {x.id: x for x in items}
        supplier_name = body.supplier_name.strip()
        if (body.supplier_id or "").strip():
            s = db.query(Supplier).filter(Supplier.user_id == user.id, Supplier.id == body.supplier_id.strip()).first()
            if s:
                supplier_name = s.name
        branch_id = (body.branch_id or "").strip() or None
        if branch_id and not db.query(Branch).filter(Branch.user_id == user.id, Branch.id == branch_id).first():
            raise HTTPException(400, "الفرع غير صالح")

        rec = Purchase(
            id=uuid.uuid4().hex,
            user_id=user.id,
            branch_id=branch_id,
            supplier_id=(body.supplier_id or "").strip() or None,
            invoice_no=body.invoice_no.strip(),
            supplier_name=supplier_name,
            purchase_date=p_date,
            payment_type=(body.payment_type or "cash").strip().lower(),
            tax_amount=round(abs(float(body.tax_amount or 0.0)), 2),
            discount=round(abs(float(body.discount or 0.0)), 2),
            paid_amount=round(abs(float(body.paid_amount or 0.0)), 2),
            due_amount=0.0,
            notes=(body.notes or "").strip() or None,
            total_amount=0.0,
        )
        db.add(rec)
        db.flush()
        total = 0.0
        for ln in body.lines:
            qty = round(abs(float(ln.qty or 0.0)), 4)
            unit_cost = round(abs(float(ln.unit_cost or 0.0)), 4)
            extra = round(abs(float(ln.extra_cost or 0.0)), 4)
            total_cost = round(qty * unit_cost + extra, 2)
            if qty <= 0:
                continue
            db.add(PurchaseLine(purchase_id=rec.id, item_id=ln.item_id, qty=qty, unit_cost=unit_cost, extra_cost=extra, total_cost=total_cost))
            eff_unit = round(total_cost / qty, 4)
            db.add(
                StockMovement(
                    id=uuid.uuid4().hex,
                    user_id=user.id,
                    item_id=ln.item_id,
                    movement_type="purchase",
                    qty_in=qty,
                    qty_out=0.0,
                    unit_cost=eff_unit,
                    reference_type="purchase",
                    reference_id=rec.id,
                    movement_date=p_date,
                )
            )
            item = item_by_id[ln.item_id]
            item.quantity = round(float(item.quantity or 0.0) + qty, 4)
            item.last_cost = eff_unit
            total += total_cost
        gross = round(total + rec.tax_amount, 2)
        rec.total_amount = round(max(0.0, gross - rec.discount), 2)
        rec.due_amount = round(max(0.0, rec.total_amount - rec.paid_amount), 2)
        db.commit()
        return {"id": rec.id, "total_amount": rec.total_amount, "due_amount": rec.due_amount}
    finally:
        db.close()


@router.get("/purchases")
def list_purchases(request: Request, limit: int = Query(200, ge=1, le=1000)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Purchase).filter(Purchase.user_id == user.id).order_by(Purchase.purchase_date.desc(), Purchase.created_at.desc()).limit(limit).all()
        return {
            "items": [
                {
                    "id": x.id,
                    "invoice_no": x.invoice_no,
                    "supplier_name": x.supplier_name,
                    "purchase_date": x.purchase_date.strftime("%Y-%m-%d"),
                    "payment_type": x.payment_type or "cash",
                    "tax_amount": round(float(x.tax_amount or 0.0), 2),
                    "discount": round(float(x.discount or 0.0), 2),
                    "paid_amount": round(float(x.paid_amount or 0.0), 2),
                    "due_amount": round(float(x.due_amount or 0.0), 2),
                    "total_amount": round(float(x.total_amount or 0.0), 2),
                }
                for x in rows
            ]
        }
    finally:
        db.close()


@router.post("/sales")
def create_sale(request: Request, body: SaleCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        s_date = _parse_date(body.sale_date, "sale_date")
        if not body.lines:
            raise HTTPException(400, "فاتورة البيع تحتاج بنوداً")
        item_ids = {x.item_id for x in body.lines}
        items = db.query(Item).filter(Item.user_id == user.id, Item.id.in_(list(item_ids))).all()
        if len(items) != len(item_ids):
            raise HTTPException(400, "يوجد صنف غير صالح")
        item_by_id = {x.id: x for x in items}
        customer_name = body.customer_name.strip()
        if (body.customer_id or "").strip():
            c = db.query(Customer).filter(Customer.user_id == user.id, Customer.id == body.customer_id.strip()).first()
            if c:
                customer_name = c.name
        branch_id = (body.branch_id or "").strip() or None
        if branch_id and not db.query(Branch).filter(Branch.user_id == user.id, Branch.id == branch_id).first():
            raise HTTPException(400, "الفرع غير صالح")

        rec = Sale(
            id=uuid.uuid4().hex,
            user_id=user.id,
            branch_id=branch_id,
            customer_id=(body.customer_id or "").strip() or None,
            invoice_no=body.invoice_no.strip(),
            customer_name=customer_name,
            sale_date=s_date,
            payment_type=(body.payment_type or "cash").strip().lower(),
            discount=round(abs(float(body.discount or 0.0)), 2),
            paid_amount=round(abs(float(body.paid_amount or 0.0)), 2),
            tax_amount=0.0,
            due_amount=0.0,
            notes=(body.notes or "").strip() or None,
            seller_name=(body.seller_name or "").strip() or None,
            branch_name=(body.branch_name or "").strip() or None,
            total_amount=0.0,
        )
        db.add(rec)
        db.flush()
        total = 0.0
        for ln in body.lines:
            qty = round(abs(float(ln.qty or 0.0)), 4)
            sale_price = round(abs(float(ln.sale_price or 0.0)), 4)
            tax = round(abs(float(ln.tax_amount or 0.0)), 2)
            if qty <= 0:
                continue
            item = item_by_id[ln.item_id]
            available = round(float(item.quantity or 0.0), 4)
            if available + 0.0001 < qty:
                raise HTTPException(400, f"المخزون غير كافٍ للصنف: {item.name}")
            unit_cost = _avg_cost(db, user.id, ln.item_id)
            cost_total = round(unit_cost * qty, 2)
            line_total = round(sale_price * qty + tax, 2)
            profit = round((sale_price * qty) - cost_total, 2)
            db.add(SaleLine(sale_id=rec.id, item_id=ln.item_id, qty=qty, sale_price=sale_price, tax_amount=tax, cost_price=cost_total, profit=profit))
            db.add(
                StockMovement(
                    id=uuid.uuid4().hex,
                    user_id=user.id,
                    item_id=ln.item_id,
                    movement_type="sale",
                    qty_in=0.0,
                    qty_out=qty,
                    unit_cost=unit_cost,
                    reference_type="sale",
                    reference_id=rec.id,
                    movement_date=s_date,
                )
            )
            item.quantity = round(available - qty, 4)
            total += line_total
        rec.total_amount = round(max(0.0, total - rec.discount), 2)
        rec.tax_amount = round(sum(float(x.tax_amount or 0.0) for x in db.query(SaleLine).filter(SaleLine.sale_id == rec.id).all()), 2)
        rec.due_amount = round(max(0.0, rec.total_amount - rec.paid_amount), 2)
        db.commit()
        return {"id": rec.id, "total_amount": rec.total_amount}
    finally:
        db.close()


@router.get("/sales")
def list_sales(request: Request, limit: int = Query(200, ge=1, le=1000)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Sale).filter(Sale.user_id == user.id).order_by(Sale.sale_date.desc(), Sale.created_at.desc()).limit(limit).all()
        return {
            "items": [
                {
                    "id": x.id,
                    "invoice_no": x.invoice_no,
                    "customer_name": x.customer_name,
                    "sale_date": x.sale_date.strftime("%Y-%m-%d"),
                    "payment_type": x.payment_type,
                    "paid_amount": round(float(x.paid_amount or 0.0), 2),
                    "due_amount": round(float(x.due_amount or 0.0), 2),
                    "total_amount": round(float(x.total_amount or 0.0), 2),
                }
                for x in rows
            ]
        }
    finally:
        db.close()


@router.get("/sales/suspended")
def list_suspended_sales(request: Request, limit: int = Query(200, ge=1, le=1000)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = (
            db.query(SuspendedSale)
            .filter(SuspendedSale.user_id == user.id)
            .order_by(SuspendedSale.sale_date.desc(), SuspendedSale.created_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "items": [
                {
                    "id": x.id,
                    "invoice_no": x.invoice_no,
                    "customer_name": x.customer_name,
                    "sale_date": x.sale_date.strftime("%Y-%m-%d"),
                    "payment_type": x.payment_type,
                    "discount": round(float(x.discount or 0.0), 2),
                    "paid_amount": round(float(x.paid_amount or 0.0), 2),
                }
                for x in rows
            ]
        }
    finally:
        db.close()


@router.post("/sales/suspend")
def suspend_sale(request: Request, body: SaleSuspendCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        s_date = _parse_date(body.sale_date, "sale_date")
        if not body.lines:
            raise HTTPException(400, "الفاتورة تحتاج بنوداً")
        rec = SuspendedSale(
            id=uuid.uuid4().hex,
            user_id=user.id,
            invoice_no=body.invoice_no.strip(),
            customer_name=body.customer_name.strip(),
            sale_date=s_date,
            payment_type=(body.payment_type or "cash").strip().lower(),
            discount=round(abs(float(body.discount or 0.0)), 2),
            paid_amount=round(abs(float(body.paid_amount or 0.0)), 2),
            notes=(body.notes or "").strip() or None,
            seller_name=(body.seller_name or "").strip() or None,
            branch_name=(body.branch_name or "").strip() or None,
        )
        db.add(rec)
        db.flush()
        for ln in body.lines:
            db.add(
                SuspendedSaleLine(
                    suspended_sale_id=rec.id,
                    item_id=ln.item_id,
                    qty=round(abs(float(ln.qty or 0.0)), 4),
                    sale_price=round(abs(float(ln.sale_price or 0.0)), 4),
                    tax_amount=round(abs(float(ln.tax_amount or 0.0)), 2),
                )
            )
        db.commit()
        return {"id": rec.id, "suspended": True}
    finally:
        db.close()


@router.post("/sales/suspended/{suspended_id}/checkout")
def checkout_suspended_sale(suspended_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        s = db.query(SuspendedSale).filter(SuspendedSale.user_id == user.id, SuspendedSale.id == suspended_id).first()
        if not s:
            raise HTTPException(404, "الفاتورة المعلقة غير موجودة")
        lines = db.query(SuspendedSaleLine).filter(SuspendedSaleLine.suspended_sale_id == s.id).all()
        body = SaleCreate(
            invoice_no=s.invoice_no,
            customer_name=s.customer_name,
            sale_date=s.sale_date.strftime("%Y-%m-%d"),
            payment_type=s.payment_type,
            customer_id="",
            branch_id="",
            discount=round(float(s.discount or 0.0), 2),
            paid_amount=round(float(s.paid_amount or 0.0), 2),
            notes=s.notes or "",
            seller_name=s.seller_name or "",
            branch_name=s.branch_name or "",
            lines=[
                SaleLineIn(
                    item_id=x.item_id,
                    qty=round(float(x.qty or 0.0), 4),
                    sale_price=round(float(x.sale_price or 0.0), 4),
                    tax_amount=round(float(x.tax_amount or 0.0), 2),
                )
                for x in lines
            ],
        )
        res = create_sale(request, body)
        for x in lines:
            db.delete(x)
        db.delete(s)
        db.commit()
        return {"checked_out": True, "sale_id": res.get("id"), "total_amount": res.get("total_amount")}
    finally:
        db.close()


@router.delete("/sales/suspended/{suspended_id}")
def delete_suspended_sale(suspended_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        s = db.query(SuspendedSale).filter(SuspendedSale.user_id == user.id, SuspendedSale.id == suspended_id).first()
        if not s:
            raise HTTPException(404, "الفاتورة المعلقة غير موجودة")
        for x in db.query(SuspendedSaleLine).filter(SuspendedSaleLine.suspended_sale_id == s.id).all():
            db.delete(x)
        db.delete(s)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


@router.get("/sales/{sale_id}")
def sale_details(sale_id: str, request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        sale = db.query(Sale).filter(Sale.user_id == user.id, Sale.id == sale_id).first()
        if not sale:
            raise HTTPException(404, "فاتورة البيع غير موجودة")
        lines = (
            db.query(SaleLine, Item)
            .join(Item, Item.id == SaleLine.item_id)
            .filter(SaleLine.sale_id == sale.id)
            .order_by(SaleLine.id.asc())
            .all()
        )
        return {
            "id": sale.id,
            "invoice_no": sale.invoice_no,
            "customer_name": sale.customer_name,
            "sale_date": sale.sale_date.strftime("%Y-%m-%d"),
            "payment_type": sale.payment_type,
            "discount": round(float(sale.discount or 0.0), 2),
            "paid_amount": round(float(sale.paid_amount or 0.0), 2),
            "due_amount": round(float(sale.due_amount or 0.0), 2),
            "total_amount": round(float(sale.total_amount or 0.0), 2),
            "seller_name": sale.seller_name or "",
            "branch_name": sale.branch_name or "",
            "lines": [
                {
                    "item_id": item.id,
                    "item_code": item.code,
                    "item_name": item.name,
                    "qty": round(float(ln.qty or 0.0), 4),
                    "sale_price": round(float(ln.sale_price or 0.0), 2),
                    "tax_amount": round(float(ln.tax_amount or 0.0), 2),
                    "line_total": round(float(ln.sale_price or 0.0) * float(ln.qty or 0.0) + float(ln.tax_amount or 0.0), 2),
                }
                for ln, item in lines
            ],
        }
    finally:
        db.close()


@router.get("/returns")
def list_returns(request: Request, limit: int = Query(200, ge=1, le=1000)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = (
            db.query(ReturnTxn)
            .filter(ReturnTxn.user_id == user.id)
            .order_by(ReturnTxn.return_date.desc(), ReturnTxn.created_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "items": [
                {
                    "id": x.id,
                    "return_type": x.return_type,
                    "invoice_no": x.invoice_no,
                    "return_date": x.return_date.strftime("%Y-%m-%d"),
                    "customer_name": x.customer_name or "",
                    "supplier_name": x.supplier_name or "",
                    "reason": x.reason or "",
                    "total_amount": round(float(x.total_amount or 0.0), 2),
                }
                for x in rows
            ]
        }
    finally:
        db.close()


@router.post("/returns/sale")
def create_sale_return(request: Request, body: SaleReturnCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec_sale = db.query(Sale).filter(Sale.user_id == user.id, Sale.id == body.sale_id).first()
        if not rec_sale:
            raise HTTPException(404, "فاتورة البيع غير موجودة")
        r_date = _parse_date(body.return_date, "return_date")
        sale_lines = db.query(SaleLine).filter(SaleLine.sale_id == rec_sale.id).all()
        sold_by_item = {}
        for ln in sale_lines:
            sold_by_item[ln.item_id] = round(float(sold_by_item.get(ln.item_id, 0.0)) + float(ln.qty or 0.0), 4)
        ret = ReturnTxn(
            id=uuid.uuid4().hex,
            user_id=user.id,
            return_type="sale_return",
            reference_type="sale",
            reference_id=rec_sale.id,
            invoice_no=f"SR-{int(dt.datetime.utcnow().timestamp())}",
            return_date=r_date,
            customer_name=rec_sale.customer_name,
            reason=(body.reason or "").strip() or None,
            total_amount=0.0,
        )
        db.add(ret)
        db.flush()
        total = 0.0
        for ln in body.lines:
            sold_qty = round(float(sold_by_item.get(ln.item_id, 0.0)), 4)
            already = _returned_qty(db, user.id, "sale", rec_sale.id, ln.item_id)
            can_return = round(max(0.0, sold_qty - already), 4)
            qty = round(abs(float(ln.qty or 0.0)), 4)
            if qty <= 0:
                continue
            if can_return + 0.0001 < qty:
                raise HTTPException(400, "كمية المرتجع أكبر من المتاح للصنف")
            item = db.query(Item).filter(Item.user_id == user.id, Item.id == ln.item_id).first()
            if not item:
                raise HTTPException(400, "صنف المرتجع غير صالح")
            unit_cost = round(abs(float(ln.unit_cost or _avg_cost(db, user.id, ln.item_id))), 4)
            unit_price = round(abs(float(ln.unit_price or 0.0)), 4)
            line_total = round(qty * unit_price, 2)
            db.add(ReturnLine(return_id=ret.id, item_id=ln.item_id, qty=qty, unit_price=unit_price, unit_cost=unit_cost, line_total=line_total))
            db.add(
                StockMovement(
                    id=uuid.uuid4().hex,
                    user_id=user.id,
                    item_id=ln.item_id,
                    movement_type="sale_return",
                    qty_in=qty,
                    qty_out=0.0,
                    unit_cost=unit_cost,
                    reference_type="sale_return",
                    reference_id=ret.id,
                    movement_date=r_date,
                    notes=ret.reason or None,
                )
            )
            item.quantity = round(float(item.quantity or 0.0) + qty, 4)
            total += line_total
        ret.total_amount = round(total, 2)
        db.commit()
        return {"id": ret.id, "total_amount": ret.total_amount}
    finally:
        db.close()


@router.post("/returns/purchase")
def create_purchase_return(request: Request, body: PurchaseReturnCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        rec_purchase = db.query(Purchase).filter(Purchase.user_id == user.id, Purchase.id == body.purchase_id).first()
        if not rec_purchase:
            raise HTTPException(404, "فاتورة الشراء غير موجودة")
        r_date = _parse_date(body.return_date, "return_date")
        p_lines = db.query(PurchaseLine).filter(PurchaseLine.purchase_id == rec_purchase.id).all()
        bought_by_item = {}
        for ln in p_lines:
            bought_by_item[ln.item_id] = round(float(bought_by_item.get(ln.item_id, 0.0)) + float(ln.qty or 0.0), 4)
        ret = ReturnTxn(
            id=uuid.uuid4().hex,
            user_id=user.id,
            return_type="purchase_return",
            reference_type="purchase",
            reference_id=rec_purchase.id,
            invoice_no=f"PR-{int(dt.datetime.utcnow().timestamp())}",
            return_date=r_date,
            supplier_name=rec_purchase.supplier_name,
            reason=(body.reason or "").strip() or None,
            total_amount=0.0,
        )
        db.add(ret)
        db.flush()
        total = 0.0
        for ln in body.lines:
            bought_qty = round(float(bought_by_item.get(ln.item_id, 0.0)), 4)
            already = _returned_qty(db, user.id, "purchase", rec_purchase.id, ln.item_id)
            can_return = round(max(0.0, bought_qty - already), 4)
            qty = round(abs(float(ln.qty or 0.0)), 4)
            if qty <= 0:
                continue
            if can_return + 0.0001 < qty:
                raise HTTPException(400, "كمية مرتجع الشراء أكبر من المتاح")
            item = db.query(Item).filter(Item.user_id == user.id, Item.id == ln.item_id).first()
            if not item:
                raise HTTPException(400, "صنف المرتجع غير صالح")
            available = round(float(item.quantity or 0.0), 4)
            if available + 0.0001 < qty:
                raise HTTPException(400, "المخزون الحالي لا يسمح بمرتجع الشراء")
            unit_cost = round(abs(float(ln.unit_cost or item.last_cost or 0.0)), 4)
            unit_price = round(abs(float(ln.unit_price or unit_cost)), 4)
            line_total = round(qty * unit_price, 2)
            db.add(ReturnLine(return_id=ret.id, item_id=ln.item_id, qty=qty, unit_price=unit_price, unit_cost=unit_cost, line_total=line_total))
            db.add(
                StockMovement(
                    id=uuid.uuid4().hex,
                    user_id=user.id,
                    item_id=ln.item_id,
                    movement_type="purchase_return",
                    qty_in=0.0,
                    qty_out=qty,
                    unit_cost=unit_cost,
                    reference_type="purchase_return",
                    reference_id=ret.id,
                    movement_date=r_date,
                    notes=ret.reason or None,
                )
            )
            item.quantity = round(available - qty, 4)
            total += line_total
        ret.total_amount = round(total, 2)
        db.commit()
        return {"id": ret.id, "total_amount": ret.total_amount}
    finally:
        db.close()


@router.put("/sales/{sale_id}")
def update_sale(sale_id: str, request: Request, body: SaleUpdate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        sale = db.query(Sale).filter(Sale.user_id == user.id, Sale.id == sale_id).first()
        if not sale:
            raise HTTPException(404, "فاتورة البيع غير موجودة")
        s_date = _parse_date(body.sale_date, "sale_date")
        if not body.lines:
            raise HTTPException(400, "فاتورة البيع تحتاج بنوداً")

        _reverse_sale_effects(db, user.id, sale.id)

        item_ids = {x.item_id for x in body.lines}
        items = db.query(Item).filter(Item.user_id == user.id, Item.id.in_(list(item_ids))).all()
        if len(items) != len(item_ids):
            raise HTTPException(400, "يوجد صنف غير صالح")
        item_by_id = {x.id: x for x in items}

        sale.invoice_no = body.invoice_no.strip()
        sale.customer_name = body.customer_name.strip()
        if (body.customer_id or "").strip():
            c = db.query(Customer).filter(Customer.user_id == user.id, Customer.id == body.customer_id.strip()).first()
            if c:
                sale.customer_name = c.name
                sale.customer_id = c.id
        sale.sale_date = s_date
        sale.payment_type = (body.payment_type or "cash").strip().lower()
        sale.discount = round(abs(float(body.discount or 0.0)), 2)
        sale.paid_amount = round(abs(float(body.paid_amount or 0.0)), 2)
        sale.notes = (body.notes or "").strip() or None
        sale.seller_name = (body.seller_name or "").strip() or None
        sale.branch_name = (body.branch_name or "").strip() or None
        sale.branch_id = (body.branch_id or "").strip() or None

        total = 0.0
        for ln in body.lines:
            qty = round(abs(float(ln.qty or 0.0)), 4)
            sale_price = round(abs(float(ln.sale_price or 0.0)), 4)
            tax = round(abs(float(ln.tax_amount or 0.0)), 2)
            if qty <= 0:
                continue
            item = item_by_id[ln.item_id]
            available = round(float(item.quantity or 0.0), 4)
            if available + 0.0001 < qty:
                raise HTTPException(400, f"المخزون غير كافٍ للصنف: {item.name}")
            unit_cost = _avg_cost(db, user.id, ln.item_id)
            cost_total = round(unit_cost * qty, 2)
            line_total = round(sale_price * qty + tax, 2)
            profit = round((sale_price * qty) - cost_total, 2)
            db.add(
                SaleLine(
                    sale_id=sale.id,
                    item_id=ln.item_id,
                    qty=qty,
                    sale_price=sale_price,
                    tax_amount=tax,
                    cost_price=cost_total,
                    profit=profit,
                )
            )
            db.add(
                StockMovement(
                    id=uuid.uuid4().hex,
                    user_id=user.id,
                    item_id=ln.item_id,
                    movement_type="sale",
                    qty_in=0.0,
                    qty_out=qty,
                    unit_cost=unit_cost,
                    reference_type="sale",
                    reference_id=sale.id,
                    movement_date=s_date,
                )
            )
            item.quantity = round(available - qty, 4)
            total += line_total
        sale.total_amount = round(max(0.0, total - sale.discount), 2)
        sale.tax_amount = round(sum(float(x.tax_amount or 0.0) for x in db.query(SaleLine).filter(SaleLine.sale_id == sale.id).all()), 2)
        sale.due_amount = round(max(0.0, sale.total_amount - sale.paid_amount), 2)
        db.commit()
        return {"id": sale.id, "total_amount": sale.total_amount, "updated": True}
    finally:
        db.close()


@router.delete("/sales/{sale_id}")
def delete_sale(sale_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        sale = db.query(Sale).filter(Sale.user_id == user.id, Sale.id == sale_id).first()
        if not sale:
            raise HTTPException(404, "فاتورة البيع غير موجودة")
        _reverse_sale_effects(db, user.id, sale.id)
        db.delete(sale)
        db.commit()
        return {"deleted": True}
    finally:
        db.close()


@router.get("/inventory")
def inventory_report(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(Item).filter(Item.user_id == user.id).order_by(Item.code.asc(), Item.created_at.asc()).all()
        out = []
        for x in rows:
            qty = round(float(x.quantity or 0.0), 4)
            cost = round(float(x.last_cost or 0.0), 2)
            out.append(
                {
                    "id": x.id,
                    "code": x.code,
                    "name": x.name,
                    "category": x.category,
                    "item_condition": x.item_condition or "",
                    "location": x.location or "",
                    "quantity": qty,
                    "cost_price": cost,
                    "sale_price": round(float(x.default_sale_price or 0.0), 2),
                    "stock_value": round(qty * cost, 2),
                }
            )
        return {"items": out}
    finally:
        db.close()


@router.post("/inventory/adjust")
def inventory_adjust(request: Request, body: StockAdjustCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        item = db.query(Item).filter(Item.user_id == user.id, Item.id == body.item_id).first()
        if not item:
            raise HTTPException(404, "الصنف غير موجود")
        adj_date = _parse_date(body.adjust_date, "adjust_date")
        before = round(float(item.quantity or 0.0), 4)
        after = round(float(body.qty_after or 0.0), 4)
        diff = round(after - before, 4)
        if abs(diff) < 0.0001:
            raise HTTPException(400, "لا يوجد فرق لتسجيله")
        item.quantity = after
        db.add(
            StockAdjustment(
                id=uuid.uuid4().hex,
                user_id=user.id,
                item_id=item.id,
                branch_id=item.branch_id,
                adjust_date=adj_date,
                qty_before=before,
                qty_after=after,
                difference=diff,
                reason=(body.reason or "").strip(),
            )
        )
        db.add(
            StockMovement(
                id=uuid.uuid4().hex,
                user_id=user.id,
                item_id=item.id,
                movement_type="adjust",
                qty_in=diff if diff > 0 else 0.0,
                qty_out=abs(diff) if diff < 0 else 0.0,
                unit_cost=round(float(item.last_cost or 0.0), 4),
                reference_type="adjust",
                reference_id=item.id,
                movement_date=adj_date,
                notes=(body.reason or "").strip() or None,
            )
        )
        db.commit()
        return {"ok": True, "qty_before": before, "qty_after": after, "difference": diff}
    finally:
        db.close()


@router.get("/reports/profit")
def profit_report(request: Request, date_from: str = Query(""), date_to: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        df = _parse_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_date(date_to, "date_to") if (date_to or "").strip() else None
        q = db.query(Sale, SaleLine, Item).join(SaleLine, SaleLine.sale_id == Sale.id).join(Item, Item.id == SaleLine.item_id).filter(Sale.user_id == user.id, Item.user_id == user.id)
        if df:
            q = q.filter(Sale.sale_date >= df)
        if dt_to:
            q = q.filter(Sale.sale_date <= dt_to)
        rows = q.all()
        items = []
        sales_total = 0.0
        cost_total = 0.0
        profit_total = 0.0
        for sale, ln, item in rows:
            net = round(float(ln.sale_price or 0.0) * float(ln.qty or 0.0), 2)
            cost = round(float(ln.cost_price or 0.0), 2)
            pr = round(float(ln.profit or 0.0), 2)
            sales_total += net
            cost_total += cost
            profit_total += pr
            items.append(
                {
                    "date": sale.sale_date.strftime("%Y-%m-%d"),
                    "invoice_no": sale.invoice_no,
                    "item": item.name,
                    "qty": round(float(ln.qty or 0.0), 4),
                    "net_sales": net,
                    "cost_total": cost,
                    "profit": pr,
                }
            )
        returns_q = db.query(ReturnTxn, ReturnLine).join(ReturnLine, ReturnLine.return_id == ReturnTxn.id).filter(ReturnTxn.user_id == user.id, ReturnTxn.return_type == "sale_return")
        if df:
            returns_q = returns_q.filter(ReturnTxn.return_date >= df)
        if dt_to:
            returns_q = returns_q.filter(ReturnTxn.return_date <= dt_to)
        return_rows = returns_q.all()
        returned_sales = round(sum(float(ln.line_total or 0.0) for _, ln in return_rows), 2)
        returned_cost = round(sum(float(ln.unit_cost or 0.0) * float(ln.qty or 0.0) for _, ln in return_rows), 2)
        sales_total = round(max(0.0, sales_total - returned_sales), 2)
        cost_total = round(max(0.0, cost_total - returned_cost), 2)
        profit_total = round(sales_total - cost_total, 2)
        margin = round((profit_total / sales_total) * 100.0, 2) if sales_total > 0 else 0.0
        return {
            "items": items,
            "totals": {
                "net_sales": round(sales_total, 2),
                "cost_total": round(cost_total, 2),
                "profit": round(profit_total, 2),
                "returned_sales": returned_sales,
                "returned_cost": returned_cost,
                "profit_margin_pct": margin,
            },
        }
    finally:
        db.close()


@router.get("/reports/top-items")
def top_items_report(request: Request, date_from: str = Query(""), date_to: str = Query(""), limit: int = Query(10, ge=1, le=100)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        df = _parse_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_date(date_to, "date_to") if (date_to or "").strip() else None
        q = (
            db.query(Sale, SaleLine, Item)
            .join(SaleLine, SaleLine.sale_id == Sale.id)
            .join(Item, Item.id == SaleLine.item_id)
            .filter(Sale.user_id == user.id, Item.user_id == user.id)
        )
        if df:
            q = q.filter(Sale.sale_date >= df)
        if dt_to:
            q = q.filter(Sale.sale_date <= dt_to)
        rows = q.all()
        agg: dict[str, dict[str, Any]] = {}
        for _, ln, item in rows:
            cur = agg.setdefault(item.id, {"item_id": item.id, "item": item.name, "qty": 0.0, "sales": 0.0, "profit": 0.0})
            qty = round(float(ln.qty or 0.0), 4)
            sales = round(float(ln.sale_price or 0.0) * qty, 2)
            cur["qty"] = round(float(cur["qty"]) + qty, 4)
            cur["sales"] = round(float(cur["sales"]) + sales, 2)
            cur["profit"] = round(float(cur["profit"]) + float(ln.profit or 0.0), 2)
        data = list(agg.values())
        best = sorted(data, key=lambda x: (float(x["qty"]), float(x["sales"])), reverse=True)[:limit]
        worst = sorted(data, key=lambda x: (float(x["qty"]), float(x["sales"])))[:limit]
        return {"best_selling": best, "worst_selling": worst}
    finally:
        db.close()


@router.get("/reports/profit-by-item")
def profit_by_item_report(request: Request, date_from: str = Query(""), date_to: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        df = _parse_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_date(date_to, "date_to") if (date_to or "").strip() else None
        q = (
            db.query(Sale, SaleLine, Item)
            .join(SaleLine, SaleLine.sale_id == Sale.id)
            .join(Item, Item.id == SaleLine.item_id)
            .filter(Sale.user_id == user.id, Item.user_id == user.id)
        )
        if df:
            q = q.filter(Sale.sale_date >= df)
        if dt_to:
            q = q.filter(Sale.sale_date <= dt_to)
        rows = q.all()
        agg: dict[str, dict[str, Any]] = {}
        for _, ln, item in rows:
            cur = agg.setdefault(item.id, {"item_id": item.id, "item": item.name, "qty": 0.0, "net_sales": 0.0, "cost_total": 0.0, "profit": 0.0})
            qty = round(float(ln.qty or 0.0), 4)
            net_sales = round(float(ln.sale_price or 0.0) * qty, 2)
            cost_total = round(float(ln.cost_price or 0.0), 2)
            cur["qty"] = round(float(cur["qty"]) + qty, 4)
            cur["net_sales"] = round(float(cur["net_sales"]) + net_sales, 2)
            cur["cost_total"] = round(float(cur["cost_total"]) + cost_total, 2)
            cur["profit"] = round(float(cur["profit"]) + float(ln.profit or 0.0), 2)
        items = sorted(list(agg.values()), key=lambda x: float(x["profit"]), reverse=True)
        return {"items": items}
    finally:
        db.close()


@router.get("/reports/stock-status")
def stock_status_report(request: Request, slow_days: int = Query(45, ge=1, le=365)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        items = db.query(Item).filter(Item.user_id == user.id).order_by(Item.code.asc(), Item.created_at.asc()).all()
        now = dt.datetime.utcnow()
        low = []
        out = []
        slow = []
        for x in items:
            qty = round(float(x.quantity or 0.0), 4)
            min_qty = round(float(x.min_qty or 0.0), 4)
            row = {
                "id": x.id,
                "code": x.code,
                "name": x.name,
                "quantity": qty,
                "min_qty": min_qty,
                "last_cost": round(float(x.last_cost or 0.0), 2),
                "sale_price": round(float(x.default_sale_price or 0.0), 2),
            }
            if qty <= 0:
                out.append(row)
            if qty <= min_qty:
                low.append(row)
            last_sale = (
                db.query(StockMovement)
                .filter(StockMovement.user_id == user.id, StockMovement.item_id == x.id, StockMovement.movement_type == "sale")
                .order_by(StockMovement.movement_date.desc(), StockMovement.created_at.desc())
                .first()
            )
            if not last_sale or (now - last_sale.movement_date).days >= slow_days:
                row["last_sale_date"] = last_sale.movement_date.strftime("%Y-%m-%d") if last_sale and last_sale.movement_date else ""
                slow.append(row)
        return {"low_stock": low, "out_of_stock": out, "slow_moving": slow}
    finally:
        db.close()


@router.get("/branch-transfers")
def list_branch_transfers(request: Request, limit: int = Query(200, ge=1, le=1000)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = (
            db.query(BranchTransfer)
            .filter(BranchTransfer.user_id == user.id)
            .order_by(BranchTransfer.transfer_date.desc(), BranchTransfer.created_at.desc())
            .limit(limit)
            .all()
        )
        items = []
        for t in rows:
            fb = db.query(Branch).filter(Branch.id == t.from_branch_id).first()
            tb = db.query(Branch).filter(Branch.id == t.to_branch_id).first()
            items.append(
                {
                    "id": t.id,
                    "transfer_no": t.transfer_no,
                    "transfer_date": t.transfer_date.strftime("%Y-%m-%d"),
                    "from_branch": (fb.name if fb else ""),
                    "to_branch": (tb.name if tb else ""),
                    "notes": t.notes or "",
                }
            )
        return {"items": items}
    finally:
        db.close()


@router.post("/branch-transfers")
def create_branch_transfer(request: Request, body: BranchTransferCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        if not body.lines:
            raise HTTPException(400, "سند التحويل يحتاج بنوداً")
        t_date = _parse_date(body.transfer_date, "transfer_date")
        if body.from_branch_id == body.to_branch_id:
            raise HTTPException(400, "لا يمكن التحويل لنفس الفرع")
        from_b = db.query(Branch).filter(Branch.user_id == user.id, Branch.id == body.from_branch_id).first()
        to_b = db.query(Branch).filter(Branch.user_id == user.id, Branch.id == body.to_branch_id).first()
        if not from_b or not to_b:
            raise HTTPException(400, "بيانات الفرع غير صحيحة")
        rec = BranchTransfer(
            id=uuid.uuid4().hex,
            user_id=user.id,
            transfer_no=body.transfer_no.strip(),
            transfer_date=t_date,
            from_branch_id=from_b.id,
            to_branch_id=to_b.id,
            notes=(body.notes or "").strip() or None,
        )
        db.add(rec)
        db.flush()
        for ln in body.lines:
            qty = round(abs(float(ln.qty or 0.0)), 4)
            if qty <= 0:
                continue
            from_item = db.query(Item).filter(Item.user_id == user.id, Item.id == ln.from_item_id).first()
            to_item = db.query(Item).filter(Item.user_id == user.id, Item.id == ln.to_item_id).first()
            if not from_item or not to_item:
                raise HTTPException(400, "أحد الأصناف في التحويل غير صالح")
            if from_item.branch_id != from_b.id or to_item.branch_id != to_b.id:
                raise HTTPException(400, "الصنف غير مرتبط بالفرع المختار")
            available = round(float(from_item.quantity or 0.0), 4)
            if available + 0.0001 < qty:
                raise HTTPException(400, f"المخزون غير كافٍ للصنف: {from_item.name}")
            from_item.quantity = round(available - qty, 4)
            to_item.quantity = round(float(to_item.quantity or 0.0) + qty, 4)
            unit_cost = round(float(from_item.last_cost or 0.0), 4)
            db.add(BranchTransferLine(transfer_id=rec.id, from_item_id=from_item.id, to_item_id=to_item.id, qty=qty))
            db.add(
                StockMovement(
                    id=uuid.uuid4().hex,
                    user_id=user.id,
                    item_id=from_item.id,
                    movement_type="transfer_out",
                    qty_in=0.0,
                    qty_out=qty,
                    unit_cost=unit_cost,
                    reference_type="branch_transfer",
                    reference_id=rec.id,
                    movement_date=t_date,
                    notes=rec.notes or f"تحويل إلى {to_b.name}",
                )
            )
            db.add(
                StockMovement(
                    id=uuid.uuid4().hex,
                    user_id=user.id,
                    item_id=to_item.id,
                    movement_type="transfer_in",
                    qty_in=qty,
                    qty_out=0.0,
                    unit_cost=unit_cost,
                    reference_type="branch_transfer",
                    reference_id=rec.id,
                    movement_date=t_date,
                    notes=rec.notes or f"تحويل من {from_b.name}",
                )
            )
        db.commit()
        return {"id": rec.id, "transfer_no": rec.transfer_no}
    finally:
        db.close()


@router.get("/reports/item-movement")
def item_movement_report(
    request: Request,
    item_id: str = Query(...),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        item = db.query(Item).filter(Item.user_id == user.id, Item.id == item_id).first()
        if not item:
            raise HTTPException(404, "الصنف غير موجود")
        df = _parse_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_date(date_to, "date_to") if (date_to or "").strip() else None
        q = db.query(StockMovement).filter(StockMovement.user_id == user.id, StockMovement.item_id == item_id)
        if df:
            q = q.filter(StockMovement.movement_date >= df)
        if dt_to:
            q = q.filter(StockMovement.movement_date <= dt_to)
        rows = q.order_by(StockMovement.movement_date.asc(), StockMovement.created_at.asc()).all()
        items = []
        qty_in = 0.0
        qty_out = 0.0
        running_balance = round(float(item.quantity or 0.0) - (sum(float(r.qty_in or 0.0) - float(r.qty_out or 0.0) for r in rows)), 4)
        for x in rows:
            qi = round(float(x.qty_in or 0.0), 4)
            qo = round(float(x.qty_out or 0.0), 4)
            qty_in += qi
            qty_out += qo
            running_balance = round(running_balance + qi - qo, 4)
            items.append(
                {
                    "date": x.movement_date.strftime("%Y-%m-%d"),
                    "movement_type": x.movement_type,
                    "qty_in": qi,
                    "qty_out": qo,
                    "unit_cost": round(float(x.unit_cost or 0.0), 2),
                    "reference_type": x.reference_type or "",
                    "reference_id": x.reference_id or "",
                    "balance_after": running_balance,
                }
            )
        return {
            "item": {"id": item.id, "code": item.code, "name": item.name},
            "items": items,
            "summary": {
                "opening_qty": round(float(item.quantity or 0.0) - (qty_in - qty_out), 4),
                "qty_in": round(qty_in, 4),
                "qty_out": round(qty_out, 4),
                "balance_qty": round(float(item.quantity or 0.0), 4),
            },
        }
    finally:
        db.close()


@router.get("/dashboard-summary")
def dashboard_summary(request: Request, date_from: str = Query(""), date_to: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        df = _parse_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_date(date_to, "date_to") if (date_to or "").strip() else None

        sales_q = db.query(Sale).filter(Sale.user_id == user.id)
        purchases_q = db.query(Purchase).filter(Purchase.user_id == user.id)
        if df:
            sales_q = sales_q.filter(Sale.sale_date >= df)
            purchases_q = purchases_q.filter(Purchase.purchase_date >= df)
        if dt_to:
            sales_q = sales_q.filter(Sale.sale_date <= dt_to)
            purchases_q = purchases_q.filter(Purchase.purchase_date <= dt_to)

        sales_rows = sales_q.all()
        purchase_rows = purchases_q.all()
        items = db.query(Item).filter(Item.user_id == user.id).all()

        total_sales = round(sum(float(x.total_amount or 0.0) for x in sales_rows), 2)
        total_purchases = round(sum(float(x.total_amount or 0.0) for x in purchase_rows), 2)
        total_inventory_value = round(
            sum(round(float(x.quantity or 0.0), 4) * round(float(x.last_cost or 0.0), 4) for x in items),
            2,
        )
        total_items_qty = round(sum(float(x.quantity or 0.0) for x in items), 4)
        return {
            "totals": {
                "sales": total_sales,
                "purchases": total_purchases,
                "inventory_value": total_inventory_value,
                "inventory_qty": total_items_qty,
            }
        }
    finally:
        db.close()
