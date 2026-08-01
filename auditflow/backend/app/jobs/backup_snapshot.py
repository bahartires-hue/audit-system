from __future__ import annotations

import datetime as dt
import gzip
import json
import os
from pathlib import Path

from app.db import SessionLocal
from app.models import (
    AnalysisReport,
    Branch,
    Category,
    Customer,
    Item,
    Purchase,
    PurchaseLine,
    ReturnLine,
    ReturnTxn,
    Sale,
    SaleLine,
    StockAdjustment,
    StockMovement,
    Supplier,
    Unit,
    User,
)


def _backup_dir() -> Path:
    root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
    if root:
        p = Path(root) / "backups"
    else:
        p = Path(__file__).resolve().parents[3] / "backups"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _iso(v):
    return v.isoformat() + "Z" if v else None


def _finance_snapshot(db) -> dict:
    """Full snapshot of the finance/inventory tables (items, purchases, sales,
    customers, suppliers, stock movements, ...). Taken before any schema
    migration so record counts can be verified before/after and, if ever
    needed, the data can be restored/inspected from this JSON dump."""
    branches = db.query(Branch).all()
    categories = db.query(Category).all()
    units = db.query(Unit).all()
    suppliers = db.query(Supplier).all()
    customers = db.query(Customer).all()
    items = db.query(Item).all()
    purchases = db.query(Purchase).all()
    purchase_lines = db.query(PurchaseLine).all()
    sales = db.query(Sale).all()
    sale_lines = db.query(SaleLine).all()
    stock_movements = db.query(StockMovement).all()
    stock_adjustments = db.query(StockAdjustment).all()
    returns = db.query(ReturnTxn).all()
    return_lines = db.query(ReturnLine).all()

    return {
        "counts": {
            "branches": len(branches),
            "categories": len(categories),
            "units": len(units),
            "suppliers": len(suppliers),
            "customers": len(customers),
            "items": len(items),
            "purchases": len(purchases),
            "purchase_lines": len(purchase_lines),
            "sales": len(sales),
            "sale_lines": len(sale_lines),
            "stock_movements": len(stock_movements),
            "stock_adjustments": len(stock_adjustments),
            "returns": len(returns),
            "return_lines": len(return_lines),
        },
        "branches": [{"id": x.id, "code": x.code, "name": x.name} for x in branches],
        "categories": [{"id": x.id, "code": x.code, "name": x.name} for x in categories],
        "units": [{"id": x.id, "code": x.code, "name": x.name} for x in units],
        "suppliers": [{"id": x.id, "name": x.name, "phone": x.phone, "opening_balance": x.opening_balance} for x in suppliers],
        "customers": [{"id": x.id, "name": x.name, "phone": x.phone, "opening_balance": x.opening_balance} for x in customers],
        "items": [
            {
                "id": x.id, "code": x.code, "barcode": x.barcode, "name": x.name,
                "category": x.category, "category_id": x.category_id, "unit": x.unit,
                "quantity": x.quantity, "min_qty": x.min_qty,
                "default_sale_price": x.default_sale_price, "last_cost": x.last_cost,
                "is_taxable": x.is_taxable, "tax_rate": x.tax_rate, "is_active": x.is_active,
            }
            for x in items
        ],
        "purchases": [
            {
                "id": x.id, "invoice_no": x.invoice_no, "supplier_id": x.supplier_id,
                "supplier_name": x.supplier_name, "purchase_date": _iso(x.purchase_date),
                "tax_amount": x.tax_amount, "discount": x.discount,
                "paid_amount": x.paid_amount, "due_amount": x.due_amount, "total_amount": x.total_amount,
            }
            for x in purchases
        ],
        "purchase_lines": [
            {"id": x.id, "purchase_id": x.purchase_id, "item_id": x.item_id, "qty": x.qty, "unit_cost": x.unit_cost, "total_cost": x.total_cost}
            for x in purchase_lines
        ],
        "sales": [
            {
                "id": x.id, "invoice_no": x.invoice_no, "customer_id": x.customer_id,
                "customer_name": x.customer_name, "sale_date": _iso(x.sale_date),
                "tax_amount": x.tax_amount, "discount": x.discount,
                "paid_amount": x.paid_amount, "due_amount": x.due_amount, "total_amount": x.total_amount,
            }
            for x in sales
        ],
        "sale_lines": [
            {"id": x.id, "sale_id": x.sale_id, "item_id": x.item_id, "qty": x.qty, "sale_price": x.sale_price, "tax_amount": x.tax_amount, "cost_price": x.cost_price, "profit": x.profit}
            for x in sale_lines
        ],
        "stock_movements": [
            {
                "id": x.id, "item_id": x.item_id, "movement_type": x.movement_type,
                "qty_in": x.qty_in, "qty_out": x.qty_out, "unit_cost": x.unit_cost,
                "reference_type": x.reference_type, "reference_id": x.reference_id,
                "movement_date": _iso(x.movement_date),
            }
            for x in stock_movements
        ],
        "stock_adjustments": [
            {"id": x.id, "item_id": x.item_id, "qty_before": x.qty_before, "qty_after": x.qty_after, "difference": x.difference, "reason": x.reason, "adjust_date": _iso(x.adjust_date)}
            for x in stock_adjustments
        ],
        "returns": [
            {"id": x.id, "return_type": x.return_type, "reference_id": x.reference_id, "invoice_no": x.invoice_no, "total_amount": x.total_amount, "return_date": _iso(x.return_date)}
            for x in returns
        ],
        "return_lines": [
            {"id": x.id, "return_id": x.return_id, "item_id": x.item_id, "qty": x.qty, "unit_price": x.unit_price, "unit_cost": x.unit_cost, "line_total": x.line_total}
            for x in return_lines
        ],
    }


def _snapshot_payload() -> dict:
    db = SessionLocal()
    try:
        users = db.query(User).order_by(User.created_at.desc()).all()
        reports = db.query(AnalysisReport).order_by(AnalysisReport.created_at.desc()).limit(2000).all()
        payload = {
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
            "users": [
                {
                    "id": u.id,
                    "username": u.username,
                    "email": u.email,
                    "is_admin": int(u.is_admin or 0),
                    "is_active": int(u.is_active or 0),
                    "plan_name": u.plan_name,
                    "subscription_expires_at": u.subscription_expires_at.isoformat() + "Z" if u.subscription_expires_at else None,
                    "created_at": u.created_at.isoformat() + "Z" if u.created_at else None,
                }
                for u in users
            ],
            "reports": [
                {
                    "id": r.id,
                    "user_id": r.user_id,
                    "title": r.title,
                    "branch1_name": r.branch1_name,
                    "branch2_name": r.branch2_name,
                    "status": r.status,
                    "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
                    "total_ops": r.total_ops,
                    "matched_ops": r.matched_ops,
                    "mismatch_ops": r.mismatch_ops,
                    "errors_count": r.errors_count,
                    "warnings_count": r.warnings_count,
                    "archived": int(r.archived or 0),
                }
                for r in reports
            ],
        }
        payload["finance"] = _finance_snapshot(db)
        return payload
    finally:
        db.close()


def main() -> None:
    payload = _snapshot_payload()
    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    out = _backup_dir() / f"auditflow-snapshot-{ts}.json.gz"
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    with gzip.open(out, "wb") as gz:
        gz.write(raw)
    # Keep recent files only.
    keep = max(3, int((os.getenv("AUDITFLOW_BACKUP_KEEP") or "14").strip()))
    files = sorted(_backup_dir().glob("auditflow-snapshot-*.json.gz"))
    for f in files[:-keep]:
        try:
            f.unlink()
        except Exception:
            pass
    print(f"backup written: {out}")
    print(f"finance record counts: {payload['finance']['counts']}")


if __name__ == "__main__":
    main()
