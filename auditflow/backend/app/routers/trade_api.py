from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..auth_core import require_csrf, require_user
from ..db import SessionLocal
from ..models import Item, Purchase, PurchaseLine, Sale, SaleLine, StockMovement

router = APIRouter(prefix="/trade", tags=["trade"])


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
    discount: float = Field(ge=0, default=0)
    notes: str = ""
    seller_name: str = ""
    branch_name: str = ""
    lines: list[SaleLineIn]


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
                    "is_set": bool(int(r.is_set or 0)),
                    "quantity": round(float(r.quantity or 0.0), 4),
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
        rec = Purchase(
            id=uuid.uuid4().hex,
            user_id=user.id,
            invoice_no=body.invoice_no.strip(),
            supplier_name=body.supplier_name.strip(),
            purchase_date=p_date,
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
        rec.total_amount = round(total, 2)
        db.commit()
        return {"id": rec.id, "total_amount": rec.total_amount}
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
        rec = Sale(
            id=uuid.uuid4().hex,
            user_id=user.id,
            invoice_no=body.invoice_no.strip(),
            customer_name=body.customer_name.strip(),
            sale_date=s_date,
            payment_type=(body.payment_type or "cash").strip().lower(),
            discount=round(abs(float(body.discount or 0.0)), 2),
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
                    "total_amount": round(float(x.total_amount or 0.0), 2),
                }
                for x in rows
            ]
        }
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
        margin = round((profit_total / sales_total) * 100.0, 2) if sales_total > 0 else 0.0
        return {
            "items": items,
            "totals": {
                "net_sales": round(sales_total, 2),
                "cost_total": round(cost_total, 2),
                "profit": round(profit_total, 2),
                "profit_margin_pct": margin,
            },
        }
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
        for x in rows:
            qi = round(float(x.qty_in or 0.0), 4)
            qo = round(float(x.qty_out or 0.0), 4)
            qty_in += qi
            qty_out += qo
            items.append(
                {
                    "date": x.movement_date.strftime("%Y-%m-%d"),
                    "movement_type": x.movement_type,
                    "qty_in": qi,
                    "qty_out": qo,
                    "unit_cost": round(float(x.unit_cost or 0.0), 2),
                    "reference_type": x.reference_type or "",
                    "reference_id": x.reference_id or "",
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
