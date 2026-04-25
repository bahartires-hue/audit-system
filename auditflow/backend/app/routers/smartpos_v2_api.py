from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..auth_core import verify_password
from ..db import SessionLocal
from ..models import AppSetting, User
from .cashierko_api import ExpenseIn, create_expense, list_customers, list_suppliers
from .trade_api import (
    ItemCreate,
    ItemUpdate,
    PurchaseCreate,
    PurchaseReturnCreate,
    SaleCreate,
    SaleReturnCreate,
    create_item,
    create_purchase,
    create_purchase_return,
    create_sale,
    create_sale_return,
    inventory_report,
    item_movement_report,
    list_items,
    list_purchases,
    list_sales,
    profit_report,
    tax_return_report,
    update_item,
)

router = APIRouter(prefix="/api/v2", tags=["smartpos-v2"])

JWT_SECRET = (os.getenv("SMARTPOS_JWT_SECRET") or os.getenv("AUDITFLOW_JWT_SECRET") or "smartpos-dev-secret").strip()
JWT_EXP_MIN = int((os.getenv("SMARTPOS_JWT_EXP_MIN") or "720").strip())


class LoginIn(BaseModel):
    username: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=1, max_length=200)


class SettingsPatchIn(BaseModel):
    store_name: str | None = None
    tax_number: str | None = None
    tax_rate: float | None = None
    currency: str | None = None
    prices_include_tax: bool | None = None
    language: str | None = None


def _b64e(data: bytes) -> str:
    return urlsafe_b64encode(data).decode().rstrip("=")


def _b64d(data: str) -> bytes:
    pad = "=" * ((4 - len(data) % 4) % 4)
    return urlsafe_b64decode((data + pad).encode())


def _sign(message: str) -> str:
    sig = hmac.new(JWT_SECRET.encode(), message.encode(), hashlib.sha256).digest()
    return _b64e(sig)


def _encode_token(user: User) -> str:
    now = dt.datetime.utcnow()
    payload: dict[str, Any] = {
        "sub": user.id,
        "username": user.username,
        "role": user.role_name or ("admin" if int(user.is_admin or 0) == 1 else "user"),
        "iat": int(now.timestamp()),
        "exp": int((now + dt.timedelta(minutes=max(30, JWT_EXP_MIN))).timestamp()),
    }
    header = {"alg": "HS256", "typ": "JWT"}
    h = _b64e(json.dumps(header, separators=(",", ":")).encode())
    p = _b64e(json.dumps(payload, separators=(",", ":")).encode())
    unsigned = f"{h}.{p}"
    return f"{unsigned}.{_sign(unsigned)}"


def _decode_token(token: str) -> dict[str, Any]:
    parts = (token or "").split(".")
    if len(parts) != 3:
        raise HTTPException(401, "توكن غير صالح")
    h, p, s = parts
    unsigned = f"{h}.{p}"
    if not hmac.compare_digest(_sign(unsigned), s):
        raise HTTPException(401, "توكن غير صالح")
    try:
        payload = json.loads(_b64d(p).decode())
    except Exception:
        raise HTTPException(401, "توكن غير صالح")
    exp = int(payload.get("exp") or 0)
    if exp <= int(dt.datetime.utcnow().timestamp()):
        raise HTTPException(401, "انتهت صلاحية التوكن")
    return payload


@router.post("/auth/login")
def v2_login(body: LoginIn = Body(...)):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == body.username.strip()).first()
        if not user:
            raise HTTPException(401, "بيانات الدخول غير صحيحة")
        if int(user.is_active or 0) != 1:
            raise HTTPException(403, "الحساب غير نشط")
        if not verify_password(body.password.strip(), user.password_hash):
            raise HTTPException(401, "بيانات الدخول غير صحيحة")
        token = _encode_token(user)
        return {
            "access_token": token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "username": user.username,
                "role": user.role_name or ("admin" if int(user.is_admin or 0) == 1 else "user"),
            },
        }
    finally:
        db.close()


@router.get("/auth/me")
def v2_me(request: Request):
    auth = (request.headers.get("authorization") or "").strip()
    if not auth.lower().startswith("bearer "):
        raise HTTPException(401, "Bearer token مطلوب")
    token = auth.split(" ", 1)[1].strip()
    payload = _decode_token(token)
    return {
        "id": payload.get("sub"),
        "username": payload.get("username"),
        "role": payload.get("role"),
    }


@router.get("/settings")
def v2_settings(request: Request):
    db = SessionLocal()
    try:
        # يعتمد نفس جلسة النظام الحالي لتوحيد الإعدادات.
        from .cashierko_api import get_cashierko_settings

        d = get_cashierko_settings(request)
        s = d.get("settings", {})
        return {
            "store_name": s.get("shop_name", "SmartPOS"),
            "tax_number": s.get("tax_number", ""),
            "tax_rate": float(s.get("tax_rate", 0) or 0),
            "currency": s.get("currency", "SAR"),
            "prices_include_tax": bool(s.get("prices_include_tax", True)),
            "language": s.get("language", "ar"),
        }
    finally:
        db.close()


@router.patch("/settings")
def v2_patch_settings(request: Request, body: SettingsPatchIn = Body(...)):
    db = SessionLocal()
    try:
        from ..auth_core import require_user

        user = require_user(db, request)
        key = f"cashierko_settings:{user.id}"
        row = db.query(AppSetting).filter(AppSetting.key == key).first()
        data = row.value_json if row and isinstance(row.value_json, dict) else {}
        if body.store_name is not None:
            data["shop_name"] = body.store_name
        if body.tax_number is not None:
            data["tax_number"] = body.tax_number
        if body.tax_rate is not None:
            data["tax_rate"] = float(body.tax_rate or 0)
        if body.currency is not None:
            data["currency"] = body.currency
        if body.prices_include_tax is not None:
            data["prices_include_tax"] = bool(body.prices_include_tax)
        if body.language is not None:
            data["language"] = body.language
        if not row:
            row = AppSetting(key=key, value_json=data)
            db.add(row)
        else:
            row.value_json = data
            row.updated_at = dt.datetime.utcnow()
        db.commit()
        return {"ok": True, "settings": data}
    finally:
        db.close()


@router.get("/items")
def v2_items(request: Request, q: str = Query(""), category: str = Query(""), is_active: str = Query("")):
    return list_items(request=request, q=q, category=category, is_active=is_active)


@router.post("/items")
def v2_create_item(request: Request, body: ItemCreate = Body(...)):
    return create_item(request=request, body=body)


@router.put("/items/{item_id}")
def v2_update_item(item_id: str, request: Request, body: ItemUpdate = Body(...)):
    return update_item(item_id=item_id, request=request, body=body)


@router.get("/customers")
def v2_customers(request: Request):
    return list_customers(request=request)


@router.get("/suppliers")
def v2_suppliers(request: Request):
    return list_suppliers(request=request)


@router.post("/purchases")
def v2_create_purchase(request: Request, body: PurchaseCreate = Body(...)):
    return create_purchase(request=request, body=body)


@router.post("/sales")
def v2_create_sale(request: Request, body: SaleCreate = Body(...)):
    return create_sale(request=request, body=body)


@router.post("/sales/{sale_id}/return")
def v2_sale_return(sale_id: str, request: Request, body: SaleReturnCreate = Body(...)):
    body.sale_id = sale_id
    return create_sale_return(request=request, body=body)


@router.post("/purchases/{purchase_id}/return")
def v2_purchase_return(purchase_id: str, request: Request, body: PurchaseReturnCreate = Body(...)):
    body.purchase_id = purchase_id
    return create_purchase_return(request=request, body=body)


@router.get("/inventory")
def v2_inventory(request: Request):
    return inventory_report(request=request)


@router.get("/inventory/movements")
def v2_inventory_movements(request: Request, item_id: str = Query(...), date_from: str = Query(""), date_to: str = Query("")):
    return item_movement_report(request=request, item_id=item_id, date_from=date_from, date_to=date_to)


@router.post("/expenses")
def v2_expense(request: Request, body: ExpenseIn = Body(...)):
    return create_expense(request=request, body=body)


@router.get("/reports/sales")
def v2_reports_sales(request: Request, limit: int = Query(200, ge=1, le=1000)):
    return list_sales(request=request, limit=limit)


@router.get("/reports/purchases")
def v2_reports_purchases(request: Request, limit: int = Query(200, ge=1, le=1000)):
    return list_purchases(request=request, limit=limit)


@router.get("/reports/inventory")
def v2_reports_inventory(request: Request):
    return inventory_report(request=request)


@router.get("/reports/profit")
def v2_reports_profit(request: Request, date_from: str = Query(""), date_to: str = Query("")):
    return profit_report(request=request, date_from=date_from, date_to=date_to)


@router.get("/reports/tax-return")
def v2_reports_tax_return(request: Request, date_from: str = Query(""), date_to: str = Query("")):
    return tax_return_report(request=request, date_from=date_from, date_to=date_to)
