from __future__ import annotations

import datetime as dt
import io
import uuid
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import func

from ..auth_core import log_event, require_csrf, require_user, user_can_access_page_key
from ..db import SessionLocal
from ..models import CompanyProfile, SimpleProduct, SimplePurchase, SimpleSale

router = APIRouter(prefix="/api/simple-inventory", tags=["simple-inventory"])


def _attachment(content: bytes, filename: str, media_type: str) -> Response:
    ascii_fallback = "".join(c if ord(c) < 128 else "_" for c in filename) or "download"
    headers = {
        "Content-Disposition": f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{quote(filename, safe='')}"
    }
    return Response(content=content, media_type=media_type, headers=headers)


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
                    "id": x.id,
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
        return {"ok": True, "product": _product_out(p), "profit": profit, "sale_id": sale.id}
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


EXCEL_COLUMNS = ["اسم الصنف", "سعر الشراء", "سعر البيع", "الكمية", "ملاحظات"]


@router.get("/import-template")
def import_template(request: Request):
    db = SessionLocal()
    try:
        _require_simple_inventory_access(db, request)
    finally:
        db.close()
    import pandas as pd

    df = pd.DataFrame(
        [{"اسم الصنف": "مثال: صنف تجريبي", "سعر الشراء": 100, "سعر البيع": 150, "الكمية": 10, "ملاحظات": ""}],
        columns=EXCEL_COLUMNS,
    )
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="قالب الأصناف")
    return _attachment(
        out.getvalue(),
        "قالب_استيراد_الأصناف.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _parse_number(v):
    if v is None:
        return None
    try:
        s = str(v).strip()
        if not s or s.lower() == "nan":
            return None
        return float(s)
    except (TypeError, ValueError):
        return None


@router.post("/import-preview")
async def import_preview(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_simple_inventory_access(db, request)
        require_csrf(request)
    finally:
        db.close()

    name_l = (file.filename or "").lower()
    if not (name_l.endswith(".xlsx") or name_l.endswith(".xls")):
        raise HTTPException(400, "الملف يجب أن يكون بصيغة Excel (.xlsx أو .xls)")
    content = await file.read()
    import pandas as pd

    try:
        df = pd.read_excel(io.BytesIO(content), dtype=str)
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")

    df.columns = [str(c).strip() for c in df.columns]
    colmap = {}
    for c in df.columns:
        cn = c.strip()
        if cn in ("اسم الصنف", "الصنف", "الاسم"):
            colmap[c] = "name"
        elif cn in ("سعر الشراء", "الشراء"):
            colmap[c] = "purchase_price"
        elif cn in ("سعر البيع", "البيع"):
            colmap[c] = "sale_price"
        elif cn in ("الكمية", "كمية"):
            colmap[c] = "quantity"
        elif cn in ("ملاحظات", "ملاحظة"):
            colmap[c] = "notes"
    df = df.rename(columns=colmap)
    df = df.dropna(how="all")

    if "name" not in df.columns:
        raise HTTPException(400, "الملف لا يحتوي على عمود 'اسم الصنف'")

    db = SessionLocal()
    try:
        rows_out = []
        for i, row in df.iterrows():
            raw_name = str(row.get("name", "") or "").strip()
            if not raw_name or raw_name.lower() == "nan":
                continue
            purchase_price = _parse_number(row.get("purchase_price"))
            sale_price = _parse_number(row.get("sale_price"))
            quantity_raw = _parse_number(row.get("quantity"))
            notes = str(row.get("notes", "") or "").strip()
            if notes.lower() == "nan":
                notes = ""

            errors = []
            if purchase_price is None or purchase_price < 0:
                errors.append("سعر الشراء غير صحيح")
            if sale_price is None or sale_price < 0:
                errors.append("سعر البيع غير صحيح")
            if quantity_raw is None or quantity_raw < 0:
                errors.append("الكمية غير صحيحة")

            existing = db.query(SimpleProduct).filter(func.lower(SimpleProduct.name) == raw_name.lower()).first()

            rows_out.append(
                {
                    "row": int(i) + 2,
                    "name": raw_name,
                    "purchase_price": purchase_price if purchase_price is not None else 0,
                    "sale_price": sale_price if sale_price is not None else 0,
                    "quantity": int(quantity_raw) if quantity_raw is not None else 0,
                    "notes": notes or None,
                    "status": "invalid" if errors else ("duplicate" if existing else "new"),
                    "errors": errors,
                    "existing_id": existing.id if existing else None,
                    "existing_quantity": existing.quantity if existing else None,
                }
            )
        return {"rows": rows_out, "total": len(rows_out)}
    finally:
        db.close()


@router.post("/import-commit")
async def import_commit(request: Request):
    payload = await request.json()
    rows = (payload or {}).get("rows") or []
    if not isinstance(rows, list) or not rows:
        raise HTTPException(400, "لا توجد بيانات لاستيرادها")

    db = SessionLocal()
    try:
        user = _require_simple_inventory_access(db, request)
        require_csrf(request)
        imported = 0
        skipped = 0
        for r in rows:
            name = str((r or {}).get("name", "")).strip()
            if not name:
                skipped += 1
                continue
            try:
                purchase_price = float(r.get("purchase_price", 0) or 0)
                sale_price = float(r.get("sale_price", 0) or 0)
                quantity = int(r.get("quantity", 0) or 0)
            except (TypeError, ValueError):
                skipped += 1
                continue
            if purchase_price < 0 or sale_price < 0 or quantity < 0:
                skipped += 1
                continue
            notes = str(r.get("notes", "") or "").strip() or None
            action = str(r.get("action", "create") or "create").strip()
            existing_id = r.get("existing_id")

            if action == "ignore":
                skipped += 1
                continue

            if existing_id and action in ("update_quantity", "update_prices"):
                p = db.query(SimpleProduct).filter(SimpleProduct.id == existing_id).first()
                if not p:
                    skipped += 1
                    continue
                if action == "update_quantity":
                    if quantity > 0:
                        purchase = SimplePurchase(
                            id=uuid.uuid4().hex,
                            product_id=p.id,
                            quantity=quantity,
                            purchase_price=purchase_price,
                            total=quantity * purchase_price,
                            created_at=dt.datetime.utcnow(),
                        )
                        db.add(purchase)
                        p.quantity = int(p.quantity or 0) + quantity
                        p.purchase_price = purchase_price
                elif action == "update_prices":
                    p.purchase_price = purchase_price
                    p.sale_price = sale_price
                db.commit()
                imported += 1
            else:
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
                if quantity > 0:
                    purchase = SimplePurchase(
                        id=uuid.uuid4().hex,
                        product_id=p.id,
                        quantity=quantity,
                        purchase_price=purchase_price,
                        total=quantity * purchase_price,
                        created_at=dt.datetime.utcnow(),
                    )
                    db.add(purchase)
                    db.commit()
                imported += 1
        log_event(db, "simple_inventory.import.completed", user.id, {"imported": imported, "skipped": skipped})
        return {"ok": True, "imported": imported, "skipped": skipped}
    finally:
        db.close()


@router.get("/export/movements")
def export_movements(request: Request, scope: str = "all", product_id: Optional[str] = None):
    db = SessionLocal()
    try:
        _require_simple_inventory_access(db, request)
        import pandas as pd

        products_by_id = {p.id: p for p in db.query(SimpleProduct).all()}

        if product_id:
            purchases = db.query(SimplePurchase).filter(SimplePurchase.product_id == product_id).all()
            sales = db.query(SimpleSale).filter(SimpleSale.product_id == product_id).all()
        else:
            purchases = db.query(SimplePurchase).all() if scope in ("all", "purchases") else []
            sales = db.query(SimpleSale).all() if scope in ("all", "sales") else []

        rows = []
        for x in purchases:
            p = products_by_id.get(x.product_id)
            rows.append(
                {
                    "التاريخ": x.created_at.strftime("%Y-%m-%d") if x.created_at else "",
                    "نوع الحركة": "شراء",
                    "اسم الصنف": p.name if p else "",
                    "الكمية": x.quantity,
                    "سعر الوحدة": x.purchase_price,
                    "الإجمالي": x.total,
                }
            )
        for x in sales:
            p = products_by_id.get(x.product_id)
            rows.append(
                {
                    "التاريخ": x.created_at.strftime("%Y-%m-%d") if x.created_at else "",
                    "نوع الحركة": "بيع",
                    "اسم الصنف": p.name if p else "",
                    "الكمية": x.quantity,
                    "سعر الوحدة": x.sale_price,
                    "الإجمالي": x.total,
                }
            )
        rows.sort(key=lambda r: r["التاريخ"])

        df = pd.DataFrame(rows, columns=["التاريخ", "نوع الحركة", "اسم الصنف", "الكمية", "سعر الوحدة", "الإجمالي"])
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="حركة الأصناف")
        return _attachment(
            out.getvalue(),
            "حركة_الأصناف.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    finally:
        db.close()


@router.get("/export/inventory-report")
def export_inventory_report(request: Request):
    db = SessionLocal()
    try:
        _require_simple_inventory_access(db, request)
        import pandas as pd

        products = db.query(SimpleProduct).order_by(SimpleProduct.name).all()
        rows = [
            {
                "اسم الصنف": p.name,
                "سعر الشراء": p.purchase_price,
                "سعر البيع": p.sale_price,
                "الكمية": p.quantity,
                "قيمة المخزون": float(p.quantity or 0) * float(p.purchase_price or 0),
            }
            for p in products
        ]
        df = pd.DataFrame(rows, columns=["اسم الصنف", "سعر الشراء", "سعر البيع", "الكمية", "قيمة المخزون"])
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="تقرير المخزون")
        return _attachment(
            out.getvalue(),
            "تقرير_المخزون.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    finally:
        db.close()


def _build_zatca_qr_base64(seller_name, vat_number, timestamp_iso, total, vat_total):
    def _tlv(tag, value):
        value_bytes = (value or "").encode("utf-8")
        return bytes([tag, len(value_bytes)]) + value_bytes

    import base64 as _b64

    payload = (
        _tlv(1, seller_name)
        + _tlv(2, vat_number)
        + _tlv(3, timestamp_iso)
        + _tlv(4, f"{total:.2f}")
        + _tlv(5, f"{vat_total:.2f}")
    )
    return _b64.b64encode(payload).decode("ascii")


@router.get("/sales/{sale_id}/invoice", response_class=HTMLResponse)
def sale_invoice(sale_id: str, request: Request):
    import base64
    import html as _html

    db = SessionLocal()
    try:
        _require_simple_inventory_access(db, request)
        sale = db.query(SimpleSale).filter(SimpleSale.id == sale_id).first()
        if not sale:
            raise HTTPException(404, "الفاتورة غير موجودة")
        product = db.query(SimpleProduct).filter(SimpleProduct.id == sale.product_id).first()
        company = db.query(CompanyProfile).filter(CompanyProfile.id == "default").first()

        vat_percentage = float(company.vat_percentage) if company and company.vat_percentage is not None else 15.0
        subtotal = float(sale.total or 0)
        tax_amount = subtotal * (vat_percentage / 100.0)
        final_total = subtotal + tax_amount

        created_at = sale.created_at or dt.datetime.utcnow()
        invoice_no = f"INV-{created_at.strftime('%Y%m%d')}-{sale.id[:6].upper()}"
        invoice_date = created_at.strftime("%Y-%m-%d")
        invoice_time = created_at.strftime("%H:%M")

        company_name = (company.company_name if company else None) or "اسم الشركة"
        tax_number = (company.tax_number if company else None) or ""
        logo_url = (company.logo_url if company else None) or ""
        commercial_register = (company.commercial_register if company else None) or ""
        address_parts = [p for p in [
            (company.address if company else None),
            (company.city if company else None),
            (company.country if company else None),
        ] if p]
        address_line = "، ".join(address_parts)
        phone = (company.phone if company else None) or ""
        email = (company.email if company else None) or ""
        currency = (company.currency if company else None) or "SAR"

        qr_html = ""
        if tax_number:
            qr_b64_payload = _build_zatca_qr_base64(
                company_name, tax_number, created_at.isoformat(), final_total, tax_amount
            )
            import qrcode

            qr_img = qrcode.make(qr_b64_payload)
            qr_buf = io.BytesIO()
            qr_img.save(qr_buf, format="PNG")
            qr_data_uri = "data:image/png;base64," + base64.b64encode(qr_buf.getvalue()).decode("ascii")
            qr_html = f'<div class="inv-qr"><img src="{qr_data_uri}" alt="QR" /></div>'

        logo_html = f'<img src="{_html.escape(logo_url)}" class="inv-logo" alt="شعار الشركة" />' if logo_url else ""
        cr_html = f'<div>س.ت: {_html.escape(commercial_register)}</div>' if commercial_register else ""
        tax_html = f'<div>الرقم الضريبي: {_html.escape(tax_number)}</div>' if tax_number else ""
        address_html = f'<div>{_html.escape(address_line)}</div>' if address_line else ""
        phone_html = f'<div dir="ltr">{_html.escape(phone)}</div>' if phone else ""
        email_html = f'<div dir="ltr">{_html.escape(email)}</div>' if email else ""

        product_name = _html.escape(product.name) if product else "صنف محذوف"
        customer_name = _html.escape(sale.customer_name or "نقدي")

        html_out = f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8" />
<title>فاتورة {_html.escape(invoice_no)}</title>
<style>
  @page {{ size: A4; margin: 14mm; }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: "Segoe UI", Tahoma, Arial, sans-serif;
    background: #f4f5f7; color: #1a1a2e; margin: 0; padding: 24px;
  }}
  .inv-sheet {{
    max-width: 800px; margin: 0 auto; background: #fff; border-radius: 14px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.08); padding: 32px 36px; position: relative;
  }}
  .inv-toolbar {{
    max-width: 800px; margin: 0 auto 14px; display: flex; gap: 10px; justify-content: flex-end;
  }}
  .inv-toolbar button {{
    padding: 10px 18px; border-radius: 10px; border: 1px solid #d4af37; background: #0f1420;
    color: #d4af37; font-weight: 700; cursor: pointer; font-size: 0.85rem;
  }}
  .inv-toolbar button:hover {{ background: #d4af37; color: #0f1420; }}
  .inv-head {{ display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 2px solid #0f1420; padding-bottom: 16px; margin-bottom: 16px; gap: 16px; flex-wrap: wrap; }}
  .inv-logo {{ max-width: 110px; max-height: 90px; object-fit: contain; }}
  .inv-company-info {{ font-size: 0.82rem; color: #444; line-height: 1.6; }}
  .inv-company-info strong {{ font-size: 1.15rem; color: #0f1420; display: block; margin-bottom: 4px; }}
  .inv-meta {{ text-align: start; font-size: 0.85rem; color: #444; line-height: 1.7; }}
  .inv-meta b {{ color: #0f1420; }}
  .inv-title {{ font-size: 1.4rem; font-weight: 800; color: #0f1420; margin-bottom: 4px; }}
  .inv-customer {{ margin: 18px 0; font-size: 0.9rem; }}
  table.inv-table {{ width: 100%; border-collapse: collapse; margin: 18px 0; font-size: 0.88rem; }}
  table.inv-table th {{ background: #0f1420; color: #d4af37; padding: 10px 8px; text-align: center; }}
  table.inv-table td {{ padding: 10px 8px; text-align: center; border-bottom: 1px solid #eee; }}
  .inv-totals {{ margin-inline-start: auto; width: 280px; font-size: 0.9rem; }}
  .inv-totals div {{ display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid #eee; }}
  .inv-totals .inv-final {{ font-weight: 800; font-size: 1.05rem; color: #0f1420; border-bottom: none; border-top: 2px solid #0f1420; padding-top: 10px; margin-top: 4px; }}
  .inv-footer {{ text-align: center; margin-top: 28px; color: #666; font-size: 0.85rem; border-top: 1px dashed #ccc; padding-top: 14px; }}
  .inv-qr {{ text-align: center; margin-top: 14px; }}
  .inv-qr img {{ width: 110px; height: 110px; }}
  @media print {{
    body {{ background: #fff; padding: 0; }}
    .inv-toolbar {{ display: none; }}
    .inv-sheet {{ box-shadow: none; border-radius: 0; padding: 0; }}
  }}
</style>
</head>
<body>
  <div class="inv-toolbar">
    <button onclick="window.print()">🖨️ طباعة مباشرة</button>
    <button onclick="window.print()">💾 حفظ PDF</button>
    <button onclick="var b=this; b.textContent='قريبًا 🙂';">📧 إرسال عبر البريد الإلكتروني</button>
  </div>

  <div class="inv-sheet">
    <div class="inv-head">
      <div style="display:flex;gap:14px;align-items:flex-start;">
        {logo_html}
        <div class="inv-company-info">
          <strong>{_html.escape(company_name)}</strong>
          {cr_html}
          {tax_html}
          {address_html}
          {phone_html}
          {email_html}
        </div>
      </div>
      <div class="inv-meta">
        <div class="inv-title">فاتورة بيع</div>
        <div><b>رقم الفاتورة:</b> {_html.escape(invoice_no)}</div>
        <div><b>التاريخ:</b> {invoice_date}</div>
        <div><b>الوقت:</b> {invoice_time}</div>
      </div>
    </div>

    <div class="inv-customer"><b>العميل:</b> {customer_name}</div>

    <table class="inv-table">
      <thead><tr><th>الصنف</th><th>الكمية</th><th>سعر الوحدة</th><th>الإجمالي</th></tr></thead>
      <tbody>
        <tr>
          <td>{product_name}</td>
          <td>{sale.quantity}</td>
          <td>{sale.sale_price:,.2f} {_html.escape(currency)}</td>
          <td>{subtotal:,.2f} {_html.escape(currency)}</td>
        </tr>
      </tbody>
    </table>

    <div class="inv-totals">
      <div><span>الإجمالي قبل الضريبة</span><span>{subtotal:,.2f} {_html.escape(currency)}</span></div>
      <div><span>الضريبة ({vat_percentage:g}%)</span><span>{tax_amount:,.2f} {_html.escape(currency)}</span></div>
      <div class="inv-final"><span>الإجمالي النهائي</span><span>{final_total:,.2f} {_html.escape(currency)}</span></div>
    </div>

    {qr_html}

    <div class="inv-footer">شكراً لتعاملكم معنا.</div>
  </div>
</body>
</html>"""
        return HTMLResponse(content=html_out)
    finally:
        db.close()
