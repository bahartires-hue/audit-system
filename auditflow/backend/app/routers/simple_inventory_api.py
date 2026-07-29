from __future__ import annotations

import datetime as dt
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import func

from ..auth_core import log_event, require_csrf, require_user, user_can_access_page_key
from ..db import SessionLocal
from ..models import SimpleProduct, SimplePurchase, SimpleSale

router = APIRouter(prefix="/api/simple-inventory", tags=["simple-inventory"])


def _require_simple_inventory_access(db, request: Request):
    user = require_user(db, request)
    if not user_can_access_page_key(user, "op-inventory"):
        raise HTTPException(403, "ليس لديك صلاحية لاستخدام نظام المخزون البسيط")
    return user


def _product_out(p: SimpleProduct) -> dict:
    return {
        "id": p.id,
        "name": p.name,
        "purchase_price": p.purchase_price,
        "sale_price": p.sale_price,
        "quantity": p.quantity,
        "notes": p.notes,
        "out_of_stock": int(p.quantity or 0) <= 0,
        "created_at": p.created_at.isoformat() + "Z" if p.created_at else None,
    }


@router.get("/products")
def list_products(request: Request, q: str = ""):
    db = SessionLocal()
    try:
        _require_simple_inventory_access(db, request)
        query = db.query(SimpleProduct)
        qn = (q or "").strip()
        if qn:
            query = query.filter(SimpleProduct.name.ilike(f"%{qn}%"))
        rows = query.order_by(SimpleProduct.created_at.desc()).all()
        return {"items": [_product_out(p) for p in rows]}
    finally:
        db.close()


@router.post("/products")
async def create_product(request: Request):
    payload = await request.json()
    name = str((payload or {}).get("name", "")).strip()
    if not name:
        raise HTTPException(400, "اسم الصنف مطلوب")
    try:
        purchase_price = float((payload or {}).get("purchase_price", 0) or 0)
        sale_price = float((payload or {}).get("sale_price", 0) or 0)
        quantity = int((payload or {}).get("quantity", 0) or 0)
    except (TypeError, ValueError):
        raise HTTPException(400, "قيم السعر والكمية يجب أن تكون أرقامًا صحيحة")
    if purchase_price < 0 or sale_price < 0 or quantity < 0:
        raise HTTPException(400, "لا يمكن أن تكون الأسعار أو الكمية بقيمة سالبة")
    notes = str((payload or {}).get("notes", "") or "").strip() or None

    db = SessionLocal()
    try:
        user = _require_simple_inventory_access(db, request)
        require_csrf(request)
        p = SimpleProduct(
            id=uuid.uuid4().hex,
            name=name,
            purchase_price=purchase_price,
            sale_price=sale_price,
            quantity=quantity,
            notes=notes,
        )
        db.add(p)
        db.commit()
        log_event(db, "simple_inventory.product.created", user.id, {"product_id": p.id, "name": name})
        return _product_out(p)
    finally:
        db.close()


@router.patch("/products/{product_id}")
async def update_product(product_id: str, request: Request):
    payload = await request.json()
    db = SessionLocal()
    try:
        user = _require_simple_inventory_access(db, request)
        require_csrf(request)
        p = db.query(SimpleProduct).filter(SimpleProduct.id == product_id).first()
        if not p:
            raise HTTPException(404, "الصنف غير موجود")
        if "name" in payload:
            new_name = str(payload.get("name") or "").strip()
            if not new_name:
                raise HTTPException(400, "اسم الصنف مطلوب")
            p.name = new_name
        if "sale_price" in payload:
            p.sale_price = max(0.0, float(payload.get("sale_price") or 0))
        if "notes" in payload:
            p.notes = (str(payload.get("notes") or "").strip() or None)
        db.commit()
        log_event(db, "simple_inventory.product.updated", user.id, {"product_id": product_id})
        return _product_out(p)
    finally:
        db.close()


@router.delete("/products/{product_id}")
def delete_product(product_id: str, request: Request):
    db = SessionLocal()
    try:
        user = _require_simple_inventory_access(db, request)
        require_csrf(request)
        p = db.query(SimpleProduct).filter(SimpleProduct.id == product_id).first()
        if not p:
            raise HTTPException(404, "الصنف غير موجود")
        has_movements = (
            db.query(SimplePurchase).filter(SimplePurchase.product_id == product_id).first()
            or db.query(SimpleSale).filter(SimpleSale.product_id == product_id).first()
        )
        if has_movements:
            raise HTTPException(400, "لا يمكن حذف صنف له حركات شراء أو بيع مسجّلة")
        db.delete(p)
        db.commit()
        log_event(db, "simple_inventory.product.deleted", user.id, {"product_id": product_id, "name": p.name})
        return {"ok": True}
    finally:
        db.close()


@router.get("/products/{product_id}/movements")
def product_movements(product_id: str, request: Request):
    db = SessionLocal()
    try:
        _require_simple_inventory_access(db, request)
        p = db.query(SimpleProduct).filter(SimpleProduct.id == product_id).first()
        if not p:
            raise HTTPException(404, "الصنف غير موجود")
        purchases = db.query(SimplePurchase).filter(SimplePurchase.product_id == product_id).all()
        sales = db.query(SimpleSale).filter(SimpleSale.product_id == product_id).all()
        items = []
        for x in purchases:
            items.append(
                {
                    "type": "purchase",
                    "quantity": x.quantity,
                    "price": x.purchase_price,
                    "total": x.total,
                    "created_at": x.created_at.isoformat() + "Z" if x.created_at else None,
                }
            )
        for x in sales:
            items.append(
                {
                    "type": "sale",
                    "quantity": x.quantity,
                    "price": x.sale_price,
                    "total": x.total,
                    "profit": x.profit,
                    "customer_name": x.customer_name,
                    "created_at": x.created_at.isoformat() + "Z" if x.created_at else None,
                }
            )
        items.sort(key=lambda r: r["created_at"] or "", reverse=True)
        return {"product": _product_out(p), "items": items}
    finally:
        db.close()


@router.post("/purchases")
async def create_purchase(request: Request):
    payload = await request.json()
    product_id = str((payload or {}).get("product_id", "")).strip()
    if not product_id:
        raise HTTPException(400, "الرجاء اختيار الصنف")
    try:
        quantity = int((payload or {}).get("quantity", 0) or 0)
        purchase_price = float((payload or {}).get("purchase_price", 0) or 0)
    except (TypeError, ValueError):
        raise HTTPException(400, "الكمية والسعر يجب أن تكون أرقامًا صحيحة")
    if quantity <= 0:
        raise HTTPException(400, "الكمية يجب أن تكون أكبر من صفر")
    if purchase_price < 0:
        raise HTTPException(400, "سعر الشراء لا يمكن أن يكون سالبًا")
    created_at = None
    raw_date = (payload or {}).get("date")
    if raw_date:
        try:
            created_at = dt.datetime.fromisoformat(str(raw_date).replace("Z", ""))
        except ValueError:
            raise HTTPException(400, "صيغة التاريخ غير صحيحة")

    db = SessionLocal()
    try:
        user = _require_simple_inventory_access(db, request)
        require_csrf(request)
        p = db.query(SimpleProduct).filter(SimpleProduct.id == product_id).first()
        if not p:
            raise HTTPException(404, "الصنف غير موجود")
        total = quantity * purchase_price
        purchase = SimplePurchase(
            id=uuid.uuid4().hex,
            product_id=product_id,
            quantity=quantity,
            purchase_price=purchase_price,
            total=total,
            created_at=created_at or dt.datetime.utcnow(),
        )
        db.add(purchase)
        p.quantity = int(p.quantity or 0) + quantity
        p.purchase_price = purchase_price
        db.commit()
        log_event(
            db,
            "simple_inventory.purchase.created",
            user.id,
            {"product_id": product_id, "quantity": quantity, "purchase_price": purchase_price},
        )
        return {"ok": True, "product": _product_out(p)}
    finally:
        db.close()


@router.post("/sales")
async def create_sale(request: Request):
    payload = await request.json()
    product_id = str((payload or {}).get("product_id", "")).strip()
    if not product_id:
        raise HTTPException(400, "الرجاء اختيار الصنف")
    try:
        quantity = int((payload or {}).get("quantity", 0) or 0)
        sale_price = float((payload or {}).get("sale_price", 0) or 0)
    except (TypeError, ValueError):
        raise HTTPException(400, "الكمية والسعر يجب أن تكون أرقامًا صحيحة")
    if quantity <= 0:
        raise HTTPException(400, "الكمية يجب أن تكون أكبر من صفر")
    if sale_price < 0:
        raise HTTPException(400, "سعر البيع لا يمكن أن يكون سالبًا")
    customer_name = str((payload or {}).get("customer_name", "") or "").strip() or "نقدي"
    created_at = None
    raw_date = (payload or {}).get("date")
    if raw_date:
        try:
            created_at = dt.datetime.fromisoformat(str(raw_date).replace("Z", ""))
        except ValueError:
            raise HTTPException(400, "صيغة التاريخ غير صحيحة")

    db = SessionLocal()
    try:
        user = _require_simple_inventory_access(db, request)
        require_csrf(request)
        p = db.query(SimpleProduct).filter(SimpleProduct.id == product_id).first()
        if not p:
            raise HTTPException(404, "الصنف غير موجود")
        if int(p.quantity or 0) < quantity:
            raise HTTPException(400, f"الكمية المتاحة ({p.quantity}) غير كافية لإتمام عملية البيع")
        total = quantity * sale_price
        profit = (sale_price - float(p.purchase_price or 0)) * quantity
        sale = SimpleSale(
            id=uuid.uuid4().hex,
            product_id=product_id,
            quantity=quantity,
            sale_price=sale_price,
            total=total,
            profit=profit,
            customer_name=customer_name,
            created_at=created_at or dt.datetime.utcnow(),
        )
        db.add(sale)
        p.quantity = int(p.quantity or 0) - quantity
        db.commit()
        log_event(
            db,
            "simple_inventory.sale.created",
            user.id,
            {"product_id": product_id, "quantity": quantity, "sale_price": sale_price, "profit": profit},
        )
        return {"ok": True, "product": _product_out(p), "profit": profit}
    finally:
        db.close()


@router.get("/dashboard-summary")
def dashboard_summary(request: Request):
    db = SessionLocal()
    try:
        _require_simple_inventory_access(db, request)
        now = dt.datetime.utcnow()
        today_start = dt.datetime(now.year, now.month, now.day)

        items_count = db.query(SimpleProduct).count()
        total_quantity = int(db.query(func.coalesce(func.sum(SimpleProduct.quantity), 0)).scalar() or 0)

        today_sales_total = float(
            db.query(func.coalesce(func.sum(SimpleSale.total), 0.0))
            .filter(SimpleSale.created_at >= today_start)
            .scalar()
            or 0.0
        )
        today_profit_total = float(
            db.query(func.coalesce(func.sum(SimpleSale.profit), 0.0))
            .filter(SimpleSale.created_at >= today_start)
            .scalar()
            or 0.0
        )

        all_time_sales_total = float(db.query(func.coalesce(func.sum(SimpleSale.total), 0.0)).scalar() or 0.0)
        all_time_purchases_total = float(db.query(func.coalesce(func.sum(SimplePurchase.total), 0.0)).scalar() or 0.0)
        all_time_profit_total = float(db.query(func.coalesce(func.sum(SimpleSale.profit), 0.0)).scalar() or 0.0)

        since_30d = today_start - dt.timedelta(days=29)
        rows = (
            db.query(func.date(SimpleSale.created_at), func.coalesce(func.sum(SimpleSale.total), 0.0))
            .filter(SimpleSale.created_at >= since_30d)
            .group_by(func.date(SimpleSale.created_at))
            .all()
        )
        by_day = {str(d): float(v or 0.0) for d, v in rows}
        labels = []
        series = []
        for i in range(30):
            day = since_30d + dt.timedelta(days=i)
            key = day.strftime("%Y-%m-%d")
            labels.append(key)
            series.append(by_day.get(key, 0.0))

        out_of_stock = db.query(SimpleProduct).filter(SimpleProduct.quantity <= 0).all()

        return {
            "items_count": items_count,
            "total_quantity": total_quantity,
            "today_sales_total": today_sales_total,
            "today_profit_total": today_profit_total,
            "all_time_sales_total": all_time_sales_total,
            "all_time_purchases_total": all_time_purchases_total,
            "all_time_profit_total": all_time_profit_total,
            "chart_30d": {"labels": labels, "sales": series},
            "out_of_stock": [{"id": p.id, "name": p.name} for p in out_of_stock],
        }
    finally:
        db.close()
