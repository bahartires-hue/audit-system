from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ..auth_core import log_event, require_csrf, require_user
from ..db import SessionLocal
from ..models import (
    Account,
    AccountingInvoice,
    AccountingInvoiceLine,
    AccountingPeriod,
    Counterparty,
    JournalEntry,
    JournalLine,
    PaymentVoucher,
)

router = APIRouter(prefix="/accounting", tags=["accounting"])

ACCOUNT_OPERATION_MAP: dict[str, str] = {
    "40101": "sale",
    "40102": "sale",
    "40103": "sale",
    "40201": "sale_return",
    "50101": "purchase",
    "50102": "purchase_return",
    "50103": "purchase",
    "80101": "branch_transfer",
    "80102": "branch_transfer",
    "80201": "branch_transfer",
    "80202": "branch_transfer",
}

EXPECTED_SIDE_BY_OPERATION: dict[str, str] = {
    "sale": "debit",
    "sale_return": "credit",
    "purchase": "credit",
    "purchase_return": "debit",
    "branch_transfer": "either",
}


class AccountCreate(BaseModel):
    code: str = Field(min_length=1, max_length=40)
    name: str = Field(min_length=1, max_length=160)
    account_type: str = Field(min_length=1, max_length=40)
    parent_id: Optional[str] = None


class AccountUpdate(BaseModel):
    code: Optional[str] = Field(default=None, min_length=1, max_length=40)
    name: Optional[str] = Field(default=None, min_length=1, max_length=160)
    account_type: Optional[str] = Field(default=None, min_length=1, max_length=40)
    parent_id: Optional[str] = None
    is_active: Optional[bool] = None


class JournalLineIn(BaseModel):
    account_id: str
    description: str = ""
    debit: float = 0.0
    credit: float = 0.0


class JournalEntryCreate(BaseModel):
    entry_date: str
    reference: str = ""
    doc_type: str = ""
    description: str = ""
    lines: List[JournalLineIn]


class CounterpartyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    party_type: str = Field(min_length=1, max_length=20)  # customer | supplier
    code: str = ""
    phone: str = ""
    email: str = ""
    address: str = ""


class InvoiceLineIn(BaseModel):
    account_id: str
    description: str = ""
    qty: float = 1.0
    unit_price: float = 0.0
    amount: float = 0.0
    tax_rate: float = 0.0


class InvoiceCreate(BaseModel):
    invoice_type: str = Field(min_length=1, max_length=20)  # sale | purchase
    invoice_no: str = Field(min_length=1, max_length=60)
    invoice_date: str
    counterparty_id: str
    description: str = ""
    offset_account_id: str
    lines: List[InvoiceLineIn]


class InvoicePaymentCreate(BaseModel):
    payment_date: str
    cash_account_id: str
    amount: float = Field(gt=0)
    reference: str = ""
    description: str = ""


class VoucherCreate(BaseModel):
    voucher_type: str = Field(min_length=1, max_length=20)  # receipt | payment
    voucher_no: str = Field(min_length=1, max_length=60)
    voucher_date: str
    account_id: str
    cash_account_id: str
    amount: float = Field(gt=0)
    counterparty_id: str = ""
    reference: str = ""
    description: str = ""


class PeriodCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    start_date: str
    end_date: str


def _to_iso(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d")


def _parse_iso_date(value: str, field_name: str) -> dt.datetime:
    try:
        return dt.datetime.strptime((value or "").strip(), "%Y-%m-%d")
    except Exception:
        raise HTTPException(400, f"{field_name} يجب أن يكون بصيغة YYYY-MM-DD")


def _ensure_write_access(user: Any) -> None:
    role = (getattr(user, "role_name", "") or "user").strip().lower()
    if role in {"viewer", "readonly"}:
        raise HTTPException(403, "لا تملك صلاحية تنفيذ عمليات كتابة في المحاسبة")


def _validate_account_ownership(db: Any, user_id: str, account_ids: set[str]) -> None:
    rows = db.query(Account).filter(Account.user_id == user_id, Account.id.in_(list(account_ids))).all()
    if len(rows) != len(account_ids):
        raise HTTPException(400, "يوجد حساب غير صالح")


def _assert_date_in_open_period(db: Any, user_id: str, doc_date: dt.datetime) -> None:
    hit = (
        db.query(AccountingPeriod)
        .filter(
            AccountingPeriod.user_id == user_id,
            AccountingPeriod.is_closed == 1,
            AccountingPeriod.start_date <= doc_date,
            AccountingPeriod.end_date >= doc_date,
        )
        .first()
    )
    if hit:
        raise HTTPException(400, f"الفترة المحاسبية مغلقة: {hit.name}")


def _movement_side(debit: float, credit: float) -> str:
    d = round(abs(float(debit or 0.0)), 2)
    c = round(abs(float(credit or 0.0)), 2)
    if d > 0 and c <= 0:
        return "debit"
    if c > 0 and d <= 0:
        return "credit"
    return "none"


def _operation_from_account_code(code: str) -> str:
    c = (code or "").strip()
    if c in ACCOUNT_OPERATION_MAP:
        return ACCOUNT_OPERATION_MAP[c]
    if c.startswith("401"):
        return "sale"
    if c.startswith("402"):
        return "sale_return"
    if c.startswith("501"):
        return "purchase"
    if c.startswith("502"):
        return "purchase_return"
    if c.startswith("801") or c.startswith("802"):
        return "branch_transfer"
    return "other"


def _item_key(description: str, account_id: str) -> str:
    d = (description or "").strip().lower()
    return d if d else f"acc:{(account_id or '').strip()}"


def _normalize_invoice_lines(lines: List[InvoiceLineIn]) -> tuple[list[dict[str, Any]], float, float, float]:
    norm: list[dict[str, Any]] = []
    subtotal = 0.0
    total_tax = 0.0
    grand_total = 0.0
    for ln in lines:
        qty = abs(float(ln.qty or 0.0))
        unit_price = abs(float(ln.unit_price or 0.0))
        calc_amount = round(qty * unit_price, 2)
        given = abs(float(ln.amount or 0.0))
        net = round(given, 2) if given > 0 else calc_amount
        if net <= 0.0:
            continue
        tax_rate = round(abs(float(ln.tax_rate or 0.0)), 4)
        tax_amount = round(net * tax_rate / 100.0, 2)
        total_amount = round(net + tax_amount, 2)
        norm.append(
            {
                "account_id": ln.account_id,
                "description": (ln.description or "").strip(),
                "qty": round(qty if qty > 0 else 1.0, 4),
                "unit_price": round(unit_price if unit_price > 0 else net, 4),
                "tax_rate": tax_rate,
                "net_amount": net,
                "tax_amount": tax_amount,
                "amount": total_amount,
            }
        )
        subtotal += net
        total_tax += tax_amount
        grand_total += total_amount
    return norm, round(subtotal, 2), round(total_tax, 2), round(grand_total, 2)


@router.get("/accounts")
def list_accounts(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = (
            db.query(Account)
            .filter(Account.user_id == user.id)
            .order_by(Account.code.asc(), Account.created_at.asc())
            .all()
        )
        return {
            "items": [
                {
                    "id": a.id,
                    "code": a.code,
                    "name": a.name,
                    "account_type": a.account_type,
                    "parent_id": a.parent_id,
                    "is_active": bool(int(a.is_active or 0)),
                }
                for a in rows
            ]
        }
    finally:
        db.close()


@router.post("/accounts")
def create_account(request: Request, body: AccountCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        code = body.code.strip()
        name = body.name.strip()
        account_type = body.account_type.strip().lower()
        if not code or not name or not account_type:
            raise HTTPException(400, "بيانات الحساب غير مكتملة")

        exists = (
            db.query(Account)
            .filter(Account.user_id == user.id, Account.code == code)
            .first()
        )
        if exists:
            raise HTTPException(400, "كود الحساب مستخدم مسبقاً")

        if body.parent_id:
            parent = (
                db.query(Account)
                .filter(Account.user_id == user.id, Account.id == body.parent_id)
                .first()
            )
            if not parent:
                raise HTTPException(400, "الحساب الأب غير موجود")

        rec = Account(
            id=uuid.uuid4().hex,
            user_id=user.id,
            code=code,
            name=name,
            account_type=account_type,
            parent_id=body.parent_id,
            is_active=1,
        )
        db.add(rec)
        db.commit()
        log_event(db, "account.created", user.id, {"account_id": rec.id, "code": rec.code})
        return {
            "id": rec.id,
            "code": rec.code,
            "name": rec.name,
            "account_type": rec.account_type,
            "parent_id": rec.parent_id,
            "is_active": True,
        }
    finally:
        db.close()


@router.patch("/accounts/{account_id}")
def update_account(account_id: str, request: Request, body: AccountUpdate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        rec = db.query(Account).filter(Account.user_id == user.id, Account.id == account_id).first()
        if not rec:
            raise HTTPException(404, "الحساب غير موجود")

        new_code = (body.code or rec.code or "").strip()
        new_name = (body.name or rec.name or "").strip()
        new_type = (body.account_type or rec.account_type or "").strip().lower()
        if not new_code or not new_name or not new_type:
            raise HTTPException(400, "بيانات الحساب غير مكتملة")

        dup = (
            db.query(Account)
            .filter(Account.user_id == user.id, Account.code == new_code, Account.id != rec.id)
            .first()
        )
        if dup:
            raise HTTPException(400, "كود الحساب مستخدم مسبقاً")

        new_parent_id = body.parent_id
        if new_parent_id:
            if new_parent_id == rec.id:
                raise HTTPException(400, "لا يمكن جعل الحساب أباً لنفسه")
            parent = db.query(Account).filter(Account.user_id == user.id, Account.id == new_parent_id).first()
            if not parent:
                raise HTTPException(400, "الحساب الأب غير موجود")

        rec.code = new_code
        rec.name = new_name
        rec.account_type = new_type
        rec.parent_id = new_parent_id
        if body.is_active is not None:
            rec.is_active = 1 if bool(body.is_active) else 0
        db.commit()
        return {
            "id": rec.id,
            "code": rec.code,
            "name": rec.name,
            "account_type": rec.account_type,
            "parent_id": rec.parent_id,
            "is_active": bool(int(rec.is_active or 0)),
        }
    finally:
        db.close()


@router.delete("/accounts/{account_id}")
def delete_account(account_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        rec = db.query(Account).filter(Account.user_id == user.id, Account.id == account_id).first()
        if not rec:
            raise HTTPException(404, "الحساب غير موجود")

        child = db.query(Account).filter(Account.user_id == user.id, Account.parent_id == rec.id).first()
        if child:
            raise HTTPException(400, "لا يمكن حذف الحساب لأنه يحتوي حسابات فرعية")

        has_lines = db.query(JournalLine).filter(JournalLine.account_id == rec.id).first()
        if has_lines:
            raise HTTPException(400, "لا يمكن حذف الحساب لوجود حركات محاسبية مرتبطة به")

        used_in_invoice_line = db.query(AccountingInvoiceLine).filter(AccountingInvoiceLine.account_id == rec.id).first()
        if used_in_invoice_line:
            raise HTTPException(400, "لا يمكن حذف الحساب لارتباطه ببنود فواتير")

        used_as_offset = db.query(AccountingInvoice).filter(AccountingInvoice.offset_account_id == rec.id).first()
        if used_as_offset:
            raise HTTPException(400, "لا يمكن حذف الحساب لارتباطه بفواتير")

        used_in_voucher = db.query(PaymentVoucher).filter(
            (PaymentVoucher.account_id == rec.id) | (PaymentVoucher.cash_account_id == rec.id)
        ).first()
        if used_in_voucher:
            raise HTTPException(400, "لا يمكن حذف الحساب لارتباطه بسندات")

        db.delete(rec)
        db.commit()
        return {"deleted": True, "id": account_id}
    finally:
        db.close()


@router.get("/journal-entries")
def list_journal_entries(
    request: Request,
    limit: int = Query(100, ge=1, le=500),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = (
            db.query(JournalEntry)
            .filter(JournalEntry.user_id == user.id)
            .order_by(JournalEntry.entry_date.desc(), JournalEntry.created_at.desc())
            .limit(limit)
            .all()
        )
        out: list[dict[str, Any]] = []
        for e in rows:
            lines = (
                db.query(JournalLine)
                .filter(JournalLine.entry_id == e.id)
                .order_by(JournalLine.id.asc())
                .all()
            )
            total_debit = round(sum(float(x.debit or 0.0) for x in lines), 2)
            total_credit = round(sum(float(x.credit or 0.0) for x in lines), 2)
            out.append(
                {
                    "id": e.id,
                    "entry_date": _to_iso(e.entry_date),
                    "reference": e.reference or "",
                    "doc_type": e.doc_type or "",
                    "description": e.description or "",
                    "total_debit": total_debit,
                    "total_credit": total_credit,
                    "lines": [
                        {
                            "id": ln.id,
                            "account_id": ln.account_id,
                            "description": ln.description or "",
                            "debit": round(float(ln.debit or 0.0), 2),
                            "credit": round(float(ln.credit or 0.0), 2),
                        }
                        for ln in lines
                    ],
                }
            )
        return {"items": out}
    finally:
        db.close()


@router.post("/journal-entries")
def create_journal_entry(request: Request, body: JournalEntryCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        entry_date = _parse_iso_date(body.entry_date, "entry_date")
        _assert_date_in_open_period(db, user.id, entry_date)

        if not body.lines or len(body.lines) < 2:
            raise HTTPException(400, "القيد يحتاج سطرين على الأقل")

        account_ids = {ln.account_id for ln in body.lines}
        accounts = (
            db.query(Account)
            .filter(Account.user_id == user.id, Account.id.in_(list(account_ids)))
            .all()
        )
        if len(accounts) != len(account_ids):
            raise HTTPException(400, "يوجد حساب غير صالح داخل سطور القيد")

        total_debit = 0.0
        total_credit = 0.0
        norm_lines: list[tuple[str, str, float, float]] = []
        for ln in body.lines:
            debit = round(abs(float(ln.debit or 0.0)), 2)
            credit = round(abs(float(ln.credit or 0.0)), 2)
            if debit > 0 and credit > 0:
                raise HTTPException(400, "السطر الواحد لا يمكن أن يحتوي مدين ودائن معاً")
            if debit <= 0 and credit <= 0:
                continue
            total_debit += debit
            total_credit += credit
            norm_lines.append((ln.account_id, (ln.description or "").strip(), debit, credit))

        if len(norm_lines) < 2:
            raise HTTPException(400, "القيد غير صالح: لا توجد سطور مالية كافية")
        total_debit = round(total_debit, 2)
        total_credit = round(total_credit, 2)
        if abs(total_debit - total_credit) > 0.009:
            raise HTTPException(
                400,
                f"القيد غير متوازن: إجمالي المدين {total_debit} لا يساوي إجمالي الدائن {total_credit}",
            )

        entry = JournalEntry(
            id=uuid.uuid4().hex,
            user_id=user.id,
            entry_date=entry_date,
            reference=(body.reference or "").strip(),
            doc_type=(body.doc_type or "").strip(),
            description=(body.description or "").strip(),
        )
        db.add(entry)
        db.flush()
        for account_id, desc, debit, credit in norm_lines:
            db.add(
                JournalLine(
                    entry_id=entry.id,
                    account_id=account_id,
                    description=desc,
                    debit=debit,
                    credit=credit,
                )
            )
        db.commit()
        log_event(
            db,
            "journal_entry.created",
            user.id,
            {"entry_id": entry.id, "total_debit": total_debit, "total_credit": total_credit},
        )
        return {
            "id": entry.id,
            "entry_date": _to_iso(entry.entry_date),
            "reference": entry.reference or "",
            "doc_type": entry.doc_type or "",
            "description": entry.description or "",
            "total_debit": total_debit,
            "total_credit": total_credit,
        }
    finally:
        db.close()


@router.get("/counterparties")
def list_counterparties(
    request: Request,
    party_type: str = Query("", description="customer or supplier"),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(Counterparty).filter(Counterparty.user_id == user.id)
        pt = (party_type or "").strip().lower()
        if pt in ("customer", "supplier"):
            q = q.filter(Counterparty.party_type == pt)
        rows = q.order_by(Counterparty.name.asc(), Counterparty.created_at.asc()).all()
        return {
            "items": [
                {
                    "id": r.id,
                    "code": r.code or "",
                    "name": r.name,
                    "party_type": r.party_type,
                    "phone": r.phone or "",
                    "email": r.email or "",
                    "address": r.address or "",
                    "is_active": bool(int(r.is_active or 0)),
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@router.post("/counterparties")
def create_counterparty(request: Request, body: CounterpartyCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        party_type = (body.party_type or "").strip().lower()
        if party_type not in ("customer", "supplier"):
            raise HTTPException(400, "party_type يجب أن يكون customer أو supplier")
        rec = Counterparty(
            id=uuid.uuid4().hex,
            user_id=user.id,
            code=(body.code or "").strip() or None,
            name=body.name.strip(),
            party_type=party_type,
            phone=(body.phone or "").strip() or None,
            email=(body.email or "").strip() or None,
            address=(body.address or "").strip() or None,
            is_active=1,
        )
        db.add(rec)
        db.commit()
        log_event(db, "counterparty.created", user.id, {"counterparty_id": rec.id, "party_type": party_type})
        return {
            "id": rec.id,
            "code": rec.code or "",
            "name": rec.name,
            "party_type": rec.party_type,
            "phone": rec.phone or "",
            "email": rec.email or "",
            "address": rec.address or "",
            "is_active": True,
        }
    finally:
        db.close()


@router.get("/invoices")
def list_invoices(
    request: Request,
    invoice_type: str = Query("", description="sale or purchase"),
    limit: int = Query(100, ge=1, le=500),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(AccountingInvoice).filter(AccountingInvoice.user_id == user.id)
        it = (invoice_type or "").strip().lower()
        if it in ("sale", "purchase"):
            q = q.filter(AccountingInvoice.invoice_type == it)
        rows = q.order_by(AccountingInvoice.invoice_date.desc(), AccountingInvoice.created_at.desc()).limit(limit).all()
        cp_ids = {x.counterparty_id for x in rows}
        cps = (
            db.query(Counterparty).filter(Counterparty.user_id == user.id, Counterparty.id.in_(list(cp_ids))).all()
            if cp_ids
            else []
        )
        cp_name = {c.id: c.name for c in cps}
        return {
            "items": [
                {
                    "id": r.id,
                    "invoice_type": r.invoice_type,
                    "invoice_no": r.invoice_no,
                    "invoice_date": _to_iso(r.invoice_date),
                    "counterparty_id": r.counterparty_id,
                    "counterparty_name": cp_name.get(r.counterparty_id, ""),
                    "description": r.description or "",
                    "subtotal_amount": round(float(r.subtotal_amount or 0.0), 2),
                    "tax_amount": round(float(r.tax_amount or 0.0), 2),
                    "total_amount": round(float(r.total_amount or 0.0), 2),
                    "paid_amount": round(float(r.paid_amount or 0.0), 2),
                    "due_amount": round(float(r.total_amount or 0.0) - float(r.paid_amount or 0.0), 2),
                    "status": r.status,
                    "offset_account_id": r.offset_account_id,
                    "journal_entry_id": r.journal_entry_id or "",
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@router.post("/invoices")
def create_invoice(request: Request, body: InvoiceCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        invoice_type = (body.invoice_type or "").strip().lower()
        if invoice_type not in ("sale", "purchase"):
            raise HTTPException(400, "invoice_type يجب أن يكون sale أو purchase")
        invoice_date = _parse_iso_date(body.invoice_date, "invoice_date")
        _assert_date_in_open_period(db, user.id, invoice_date)

        cp = (
            db.query(Counterparty)
            .filter(Counterparty.user_id == user.id, Counterparty.id == body.counterparty_id)
            .first()
        )
        if not cp:
            raise HTTPException(400, "العميل/المورد غير موجود")
        if invoice_type == "sale" and cp.party_type != "customer":
            raise HTTPException(400, "فاتورة البيع تتطلب طرفاً من نوع customer")
        if invoice_type == "purchase" and cp.party_type != "supplier":
            raise HTTPException(400, "فاتورة الشراء تتطلب طرفاً من نوع supplier")

        norm_lines, subtotal, tax_total, total = _normalize_invoice_lines(body.lines or [])
        if len(norm_lines) == 0 or total <= 0:
            raise HTTPException(400, "الفاتورة تحتاج بنوداً صالحة بمبالغ موجبة")
        account_ids = {x["account_id"] for x in norm_lines}
        account_ids.add(body.offset_account_id)
        _validate_account_ownership(db, user.id, account_ids)

        dup = (
            db.query(AccountingInvoice)
            .filter(
                AccountingInvoice.user_id == user.id,
                AccountingInvoice.invoice_type == invoice_type,
                AccountingInvoice.invoice_no == body.invoice_no.strip(),
            )
            .first()
        )
        if dup:
            raise HTTPException(400, "رقم الفاتورة مستخدم مسبقاً لنفس النوع")

        inv = AccountingInvoice(
            id=uuid.uuid4().hex,
            user_id=user.id,
            invoice_type=invoice_type,
            invoice_no=body.invoice_no.strip(),
            invoice_date=invoice_date,
            counterparty_id=body.counterparty_id,
            description=(body.description or "").strip(),
            subtotal_amount=subtotal,
            tax_amount=tax_total,
            total_amount=total,
            paid_amount=0.0,
            status="draft",
            offset_account_id=body.offset_account_id,
        )
        db.add(inv)
        db.flush()
        for ln in norm_lines:
            db.add(
                AccountingInvoiceLine(
                    invoice_id=inv.id,
                    account_id=ln["account_id"],
                    description=ln["description"],
                    qty=ln["qty"],
                    unit_price=ln["unit_price"],
                    tax_rate=ln["tax_rate"],
                    net_amount=ln["net_amount"],
                    tax_amount=ln["tax_amount"],
                    amount=ln["amount"],
                )
            )
        db.commit()
        log_event(
            db,
            "invoice.created",
            user.id,
            {"invoice_id": inv.id, "invoice_no": inv.invoice_no, "invoice_type": inv.invoice_type},
        )
        return {
            "id": inv.id,
            "invoice_type": inv.invoice_type,
            "invoice_no": inv.invoice_no,
            "invoice_date": _to_iso(inv.invoice_date),
            "counterparty_id": inv.counterparty_id,
            "description": inv.description or "",
            "subtotal_amount": round(float(inv.subtotal_amount or 0.0), 2),
            "tax_amount": round(float(inv.tax_amount or 0.0), 2),
            "total_amount": round(float(inv.total_amount or 0.0), 2),
            "paid_amount": 0.0,
            "due_amount": round(float(inv.total_amount or 0.0), 2),
            "status": inv.status,
            "offset_account_id": inv.offset_account_id,
        }
    finally:
        db.close()


@router.post("/invoices/{invoice_id}/post")
def post_invoice(invoice_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        inv = (
            db.query(AccountingInvoice)
            .filter(AccountingInvoice.user_id == user.id, AccountingInvoice.id == invoice_id)
            .first()
        )
        if not inv:
            raise HTTPException(404, "الفاتورة غير موجودة")
        if inv.status == "posted":
            return {"id": inv.id, "status": inv.status, "journal_entry_id": inv.journal_entry_id or ""}
        _assert_date_in_open_period(db, user.id, inv.invoice_date)

        lines = (
            db.query(AccountingInvoiceLine)
            .filter(AccountingInvoiceLine.invoice_id == inv.id)
            .order_by(AccountingInvoiceLine.id.asc())
            .all()
        )
        if not lines:
            raise HTTPException(400, "لا يمكن ترحيل فاتورة بدون بنود")

        total = round(sum(float(x.amount or 0.0) for x in lines), 2)
        if total <= 0:
            raise HTTPException(400, "إجمالي الفاتورة غير صالح")

        je = JournalEntry(
            id=uuid.uuid4().hex,
            user_id=user.id,
            entry_date=inv.invoice_date,
            reference=inv.invoice_no,
            doc_type="فاتورة مبيعات" if inv.invoice_type == "sale" else "فاتورة مشتريات",
            description=inv.description or "",
        )
        db.add(je)
        db.flush()

        if inv.invoice_type == "sale":
            db.add(
                JournalLine(
                    entry_id=je.id,
                    account_id=inv.offset_account_id,
                    description=f"ذمم - {inv.invoice_no}",
                    debit=total,
                    credit=0.0,
                )
            )
            for ln in lines:
                db.add(
                    JournalLine(
                        entry_id=je.id,
                        account_id=ln.account_id,
                        description=ln.description or f"مبيعات - {inv.invoice_no}",
                        debit=0.0,
                        credit=round(float(ln.amount or 0.0), 2),
                    )
                )
        else:
            for ln in lines:
                db.add(
                    JournalLine(
                        entry_id=je.id,
                        account_id=ln.account_id,
                        description=ln.description or f"مشتريات - {inv.invoice_no}",
                        debit=round(float(ln.amount or 0.0), 2),
                        credit=0.0,
                    )
                )
            db.add(
                JournalLine(
                    entry_id=je.id,
                    account_id=inv.offset_account_id,
                    description=f"ذمم دائنة - {inv.invoice_no}",
                    debit=0.0,
                    credit=total,
                )
            )

        inv.status = "posted"
        inv.journal_entry_id = je.id
        db.commit()
        log_event(
            db,
            "invoice.posted",
            user.id,
            {"invoice_id": inv.id, "journal_entry_id": je.id, "invoice_type": inv.invoice_type},
        )
        return {
            "id": inv.id,
            "status": inv.status,
            "journal_entry_id": inv.journal_entry_id,
            "total_amount": round(float(inv.total_amount or 0.0), 2),
        }
    finally:
        db.close()


@router.post("/invoices/{invoice_id}/payments")
def pay_invoice(invoice_id: str, request: Request, body: InvoicePaymentCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        pay_date = _parse_iso_date(body.payment_date, "payment_date")
        _assert_date_in_open_period(db, user.id, pay_date)
        inv = db.query(AccountingInvoice).filter(AccountingInvoice.user_id == user.id, AccountingInvoice.id == invoice_id).first()
        if not inv:
            raise HTTPException(404, "الفاتورة غير موجودة")
        if inv.status == "draft":
            raise HTTPException(400, "يجب ترحيل الفاتورة قبل تسجيل السداد")
        due = round(float(inv.total_amount or 0.0) - float(inv.paid_amount or 0.0), 2)
        amount = round(abs(float(body.amount or 0.0)), 2)
        if amount <= 0:
            raise HTTPException(400, "مبلغ السداد غير صالح")
        if amount - due > 0.009:
            raise HTTPException(400, f"المبلغ أكبر من المتبقي ({due})")
        _validate_account_ownership(db, user.id, {inv.offset_account_id, body.cash_account_id})

        je = JournalEntry(
            id=uuid.uuid4().hex,
            user_id=user.id,
            entry_date=pay_date,
            reference=(body.reference or "").strip() or inv.invoice_no,
            doc_type="سداد فاتورة",
            description=(body.description or "").strip() or f"سداد {inv.invoice_no}",
        )
        db.add(je)
        db.flush()
        if inv.invoice_type == "sale":
            db.add(JournalLine(entry_id=je.id, account_id=body.cash_account_id, description="تحصيل نقدي", debit=amount, credit=0.0))
            db.add(JournalLine(entry_id=je.id, account_id=inv.offset_account_id, description="تخفيض ذمم عميل", debit=0.0, credit=amount))
        else:
            db.add(JournalLine(entry_id=je.id, account_id=inv.offset_account_id, description="تخفيض ذمم مورد", debit=amount, credit=0.0))
            db.add(JournalLine(entry_id=je.id, account_id=body.cash_account_id, description="سداد نقدي", debit=0.0, credit=amount))

        inv.paid_amount = round(float(inv.paid_amount or 0.0) + amount, 2)
        remaining = round(float(inv.total_amount or 0.0) - float(inv.paid_amount or 0.0), 2)
        inv.status = "paid" if remaining <= 0.009 else "partially_paid"
        db.commit()
        return {
            "invoice_id": inv.id,
            "status": inv.status,
            "paid_amount": round(float(inv.paid_amount or 0.0), 2),
            "due_amount": round(max(remaining, 0.0), 2),
            "journal_entry_id": je.id,
        }
    finally:
        db.close()


@router.post("/vouchers")
def create_voucher(request: Request, body: VoucherCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        vtype = (body.voucher_type or "").strip().lower()
        if vtype not in ("receipt", "payment"):
            raise HTTPException(400, "voucher_type يجب أن يكون receipt أو payment")
        voucher_date = _parse_iso_date(body.voucher_date, "voucher_date")
        _assert_date_in_open_period(db, user.id, voucher_date)
        amount = round(abs(float(body.amount or 0.0)), 2)
        if amount <= 0:
            raise HTTPException(400, "المبلغ غير صالح")
        ids = {body.account_id, body.cash_account_id}
        _validate_account_ownership(db, user.id, ids)

        if body.counterparty_id:
            cp = db.query(Counterparty).filter(Counterparty.user_id == user.id, Counterparty.id == body.counterparty_id).first()
            if not cp:
                raise HTTPException(400, "الطرف المقابل غير موجود")

        dup = (
            db.query(PaymentVoucher)
            .filter(PaymentVoucher.user_id == user.id, PaymentVoucher.voucher_type == vtype, PaymentVoucher.voucher_no == body.voucher_no.strip())
            .first()
        )
        if dup:
            raise HTTPException(400, "رقم السند مستخدم مسبقاً")

        je = JournalEntry(
            id=uuid.uuid4().hex,
            user_id=user.id,
            entry_date=voucher_date,
            reference=(body.reference or "").strip() or body.voucher_no.strip(),
            doc_type="سند قبض" if vtype == "receipt" else "سند صرف",
            description=(body.description or "").strip() or ("قبض" if vtype == "receipt" else "صرف"),
        )
        db.add(je)
        db.flush()
        if vtype == "receipt":
            db.add(JournalLine(entry_id=je.id, account_id=body.cash_account_id, description="قبض نقدي", debit=amount, credit=0.0))
            db.add(JournalLine(entry_id=je.id, account_id=body.account_id, description="الطرف المقابل", debit=0.0, credit=amount))
        else:
            db.add(JournalLine(entry_id=je.id, account_id=body.account_id, description="الطرف المقابل", debit=amount, credit=0.0))
            db.add(JournalLine(entry_id=je.id, account_id=body.cash_account_id, description="صرف نقدي", debit=0.0, credit=amount))

        rec = PaymentVoucher(
            id=uuid.uuid4().hex,
            user_id=user.id,
            voucher_type=vtype,
            voucher_no=body.voucher_no.strip(),
            voucher_date=voucher_date,
            counterparty_id=(body.counterparty_id or "").strip() or None,
            account_id=body.account_id,
            cash_account_id=body.cash_account_id,
            amount=amount,
            description=(body.description or "").strip() or None,
            journal_entry_id=je.id,
        )
        db.add(rec)
        db.commit()
        return {
            "id": rec.id,
            "voucher_type": rec.voucher_type,
            "voucher_no": rec.voucher_no,
            "voucher_date": _to_iso(rec.voucher_date),
            "amount": round(float(rec.amount or 0.0), 2),
            "journal_entry_id": rec.journal_entry_id or "",
        }
    finally:
        db.close()


@router.get("/vouchers")
def list_vouchers(request: Request, voucher_type: str = Query("", description="receipt or payment"), limit: int = Query(100, ge=1, le=500)):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        q = db.query(PaymentVoucher).filter(PaymentVoucher.user_id == user.id)
        vt = (voucher_type or "").strip().lower()
        if vt in ("receipt", "payment"):
            q = q.filter(PaymentVoucher.voucher_type == vt)
        rows = q.order_by(PaymentVoucher.voucher_date.desc(), PaymentVoucher.created_at.desc()).limit(limit).all()
        return {
            "items": [
                {
                    "id": r.id,
                    "voucher_type": r.voucher_type,
                    "voucher_no": r.voucher_no,
                    "voucher_date": _to_iso(r.voucher_date),
                    "amount": round(float(r.amount or 0.0), 2),
                    "account_id": r.account_id,
                    "cash_account_id": r.cash_account_id,
                    "journal_entry_id": r.journal_entry_id or "",
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@router.get("/ledger")
def get_ledger(
    request: Request,
    account_id: str = Query(...),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        acc = db.query(Account).filter(Account.user_id == user.id, Account.id == account_id).first()
        if not acc:
            raise HTTPException(404, "الحساب غير موجود")
        df = _parse_iso_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_iso_date(date_to, "date_to") if (date_to or "").strip() else None

        q = (
            db.query(JournalLine, JournalEntry)
            .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
            .filter(JournalEntry.user_id == user.id, JournalLine.account_id == account_id)
        )
        if df:
            q = q.filter(JournalEntry.entry_date >= df)
        if dt_to:
            q = q.filter(JournalEntry.entry_date <= dt_to)
        rows = q.order_by(JournalEntry.entry_date.asc(), JournalEntry.created_at.asc(), JournalLine.id.asc()).all()

        balance = 0.0
        items: list[dict[str, Any]] = []
        for ln, je in rows:
            debit = round(float(ln.debit or 0.0), 2)
            credit = round(float(ln.credit or 0.0), 2)
            balance = round(balance + debit - credit, 2)
            items.append(
                {
                    "entry_id": je.id,
                    "entry_date": _to_iso(je.entry_date),
                    "reference": je.reference or "",
                    "doc_type": je.doc_type or "",
                    "description": ln.description or je.description or "",
                    "debit": debit,
                    "credit": credit,
                    "running_balance": balance,
                }
            )
        return {"account": {"id": acc.id, "code": acc.code, "name": acc.name}, "items": items, "closing_balance": balance}
    finally:
        db.close()


@router.get("/trial-balance")
def get_trial_balance(request: Request, date_from: str = Query(""), date_to: str = Query("")):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        df = _parse_iso_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_iso_date(date_to, "date_to") if (date_to or "").strip() else None
        q = db.query(JournalLine, JournalEntry, Account).join(JournalEntry, JournalEntry.id == JournalLine.entry_id).join(Account, Account.id == JournalLine.account_id).filter(JournalEntry.user_id == user.id, Account.user_id == user.id)
        if df:
            q = q.filter(JournalEntry.entry_date >= df)
        if dt_to:
            q = q.filter(JournalEntry.entry_date <= dt_to)
        rows = q.all()
        by_acc: dict[str, dict[str, Any]] = {}
        for ln, _, acc in rows:
            rec = by_acc.setdefault(
                acc.id,
                {"account_id": acc.id, "code": acc.code, "name": acc.name, "total_debit": 0.0, "total_credit": 0.0},
            )
            rec["total_debit"] = round(rec["total_debit"] + float(ln.debit or 0.0), 2)
            rec["total_credit"] = round(rec["total_credit"] + float(ln.credit or 0.0), 2)
        items = []
        sum_dr = 0.0
        sum_cr = 0.0
        for rec in sorted(by_acc.values(), key=lambda x: ((x["code"] or ""), (x["name"] or ""))):
            bal = round(rec["total_debit"] - rec["total_credit"], 2)
            items.append(
                {
                    **rec,
                    "balance_debit": bal if bal > 0 else 0.0,
                    "balance_credit": abs(bal) if bal < 0 else 0.0,
                }
            )
            sum_dr += rec["total_debit"]
            sum_cr += rec["total_credit"]
        return {
            "items": items,
            "totals": {
                "total_debit": round(sum_dr, 2),
                "total_credit": round(sum_cr, 2),
                "is_balanced": abs(round(sum_dr - sum_cr, 2)) <= 0.009,
            },
        }
    finally:
        db.close()


@router.get("/periods")
def list_periods(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        rows = db.query(AccountingPeriod).filter(AccountingPeriod.user_id == user.id).order_by(AccountingPeriod.start_date.desc()).all()
        return {
            "items": [
                {
                    "id": p.id,
                    "name": p.name,
                    "start_date": _to_iso(p.start_date),
                    "end_date": _to_iso(p.end_date),
                    "is_closed": bool(int(p.is_closed or 0)),
                }
                for p in rows
            ]
        }
    finally:
        db.close()


@router.post("/periods")
def create_period(request: Request, body: PeriodCreate = Body(...)):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        start_date = _parse_iso_date(body.start_date, "start_date")
        end_date = _parse_iso_date(body.end_date, "end_date")
        if end_date < start_date:
            raise HTTPException(400, "end_date يجب أن يكون أكبر من أو يساوي start_date")
        rec = AccountingPeriod(
            id=uuid.uuid4().hex,
            user_id=user.id,
            name=body.name.strip(),
            start_date=start_date,
            end_date=end_date,
            is_closed=0,
        )
        db.add(rec)
        db.commit()
        return {"id": rec.id, "name": rec.name, "start_date": _to_iso(rec.start_date), "end_date": _to_iso(rec.end_date), "is_closed": False}
    finally:
        db.close()


@router.post("/periods/{period_id}/toggle-close")
def toggle_period_close(period_id: str, request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        rec = db.query(AccountingPeriod).filter(AccountingPeriod.user_id == user.id, AccountingPeriod.id == period_id).first()
        if not rec:
            raise HTTPException(404, "الفترة غير موجودة")
        rec.is_closed = 0 if int(rec.is_closed or 0) == 1 else 1
        db.commit()
        return {"id": rec.id, "is_closed": bool(int(rec.is_closed or 0))}
    finally:
        db.close()


@router.post("/bootstrap-chart")
def bootstrap_chart(request: Request):
    db = SessionLocal()
    try:
        require_csrf(request)
        user = require_user(db, request)
        _ensure_write_access(user)
        defaults = [
            # 1) الأصول
            ("10101", "صندوق الفرع الرئيسي", "asset", None),
            ("10102", "صندوق فرع 1", "asset", None),
            ("10103", "صندوق فرع 2", "asset", None),
            ("10201", "بنك الراجحي", "asset", None),
            ("10202", "بنك الأهلي", "asset", None),
            ("10301", "عملاء الفرع الرئيسي", "asset", None),
            ("10302", "عملاء فرع 1", "asset", None),
            ("10303", "عملاء فرع 2", "asset", None),
            ("10401", "مخزون كفرات الفرع الرئيسي", "asset", None),
            ("10402", "مخزون كفرات فرع 1", "asset", None),
            ("10403", "مخزون كفرات فرع 2", "asset", None),
            # 2) الالتزامات
            ("20101", "موردين كفرات", "liability", None),
            ("20102", "موردين خدمات", "liability", None),
            ("20201", "ضريبة القيمة المضافة", "liability", None),
            ("20301", "رواتب مستحقة", "liability", None),
            # 3) حقوق الملكية
            ("30101", "رأس المال", "equity", None),
            ("30201", "جاري المالك", "equity", None),
            ("30301", "أرباح محتجزة", "equity", None),
            # 4) الإيرادات
            ("40101", "مبيعات كفرات نقدي", "revenue", None),
            ("40102", "مبيعات كفرات آجل", "revenue", None),
            ("40103", "مبيعات خدمات (ترصيص / ميزان)", "revenue", None),
            ("40201", "مردود مبيعات كفرات", "revenue", None),
            # 5) المشتريات
            ("50101", "مشتريات كفرات", "expense", None),
            ("50102", "مردود مشتريات", "expense", None),
            ("50103", "نقل مشتريات", "expense", None),
            # 6) تكلفة المبيعات
            ("60101", "تكلفة بضاعة مباعة", "expense", None),
            # 7) المصروفات
            ("70101", "رواتب", "expense", None),
            ("70102", "إيجار", "expense", None),
            ("70103", "كهرباء", "expense", None),
            ("70104", "صيانة أجهزة", "expense", None),
            ("70105", "مصاريف تشغيل", "expense", None),
            # 8) حسابات الفروع
            ("80101", "تحويل إلى فرع 1", "equity", None),
            ("80102", "تحويل إلى فرع 2", "equity", None),
            ("80201", "تحويل من فرع 1", "equity", None),
            ("80202", "تحويل من فرع 2", "equity", None),
        ]
        existing = {
            x.code: x
            for x in db.query(Account).filter(Account.user_id == user.id).all()
        }
        created = 0
        for code, name, acc_type, parent_id in defaults:
            if code in existing:
                continue
            db.add(
                Account(
                    id=uuid.uuid4().hex,
                    user_id=user.id,
                    code=code,
                    name=name,
                    account_type=acc_type,
                    parent_id=parent_id,
                    is_active=1,
                )
            )
            created += 1
        db.commit()
        return {"created": created}
    finally:
        db.close()


@router.get("/roles/me")
def accounting_role_info(request: Request):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        role = (getattr(user, "role_name", "") or "user").strip().lower()
        can_write = role not in {"viewer", "readonly"}
        return {"role": role, "can_write": can_write, "can_read": True}
    finally:
        db.close()


@router.get("/analysis/operations")
def analyze_operations(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    limit: int = Query(1000, ge=1, le=5000),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        df = _parse_iso_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_iso_date(date_to, "date_to") if (date_to or "").strip() else None
        q = (
            db.query(JournalLine, JournalEntry, Account)
            .join(JournalEntry, JournalEntry.id == JournalLine.entry_id)
            .join(Account, Account.id == JournalLine.account_id)
            .filter(JournalEntry.user_id == user.id, Account.user_id == user.id)
            .order_by(JournalEntry.entry_date.desc(), JournalEntry.created_at.desc())
            .limit(limit)
        )
        if df:
            q = q.filter(JournalEntry.entry_date >= df)
        if dt_to:
            q = q.filter(JournalEntry.entry_date <= dt_to)
        rows = q.all()
        items: list[dict[str, Any]] = []
        summary: dict[str, dict[str, int]] = {}
        for ln, je, acc in rows:
            side = _movement_side(float(ln.debit or 0.0), float(ln.credit or 0.0))
            op = _operation_from_account_code(acc.code or "")
            expected = EXPECTED_SIDE_BY_OPERATION.get(op, "either")
            is_ok = expected == "either" or side == expected
            rec = summary.setdefault(op, {"total": 0, "ok": 0, "violations": 0})
            rec["total"] += 1
            if is_ok:
                rec["ok"] += 1
            else:
                rec["violations"] += 1
            items.append(
                {
                    "entry_id": je.id,
                    "entry_date": _to_iso(je.entry_date),
                    "reference": je.reference or "",
                    "account_code": acc.code or "",
                    "account_name": acc.name or "",
                    "debit": round(float(ln.debit or 0.0), 2),
                    "credit": round(float(ln.credit or 0.0), 2),
                    "side": side,
                    "operation": op,
                    "expected_side": expected,
                    "is_consistent": is_ok,
                }
            )
        return {"items": items, "summary": summary, "rules": EXPECTED_SIDE_BY_OPERATION, "map": ACCOUNT_OPERATION_MAP}
    finally:
        db.close()


@router.get("/reports/sales-movement")
def sales_movement_report(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    item_query: str = Query(""),
    limit: int = Query(5000, ge=1, le=20000),
):
    db = SessionLocal()
    try:
        user = require_user(db, request)
        df = _parse_iso_date(date_from, "date_from") if (date_from or "").strip() else None
        dt_to = _parse_iso_date(date_to, "date_to") if (date_to or "").strip() else None
        q_txt = (item_query or "").strip().lower()

        sale_q = (
            db.query(AccountingInvoice, AccountingInvoiceLine, Account, Counterparty)
            .join(AccountingInvoiceLine, AccountingInvoiceLine.invoice_id == AccountingInvoice.id)
            .join(Account, Account.id == AccountingInvoiceLine.account_id)
            .join(Counterparty, Counterparty.id == AccountingInvoice.counterparty_id)
            .filter(
                AccountingInvoice.user_id == user.id,
                AccountingInvoice.invoice_type == "sale",
                AccountingInvoice.status.in_(["posted", "partially_paid", "paid"]),
            )
            .order_by(AccountingInvoice.invoice_date.asc(), AccountingInvoice.created_at.asc())
            .limit(limit)
        )
        if df:
            sale_q = sale_q.filter(AccountingInvoice.invoice_date >= df)
        if dt_to:
            sale_q = sale_q.filter(AccountingInvoice.invoice_date <= dt_to)
        sale_rows = sale_q.all()

        # Build weighted average cost per item key from posted purchase lines.
        pur_q = (
            db.query(AccountingInvoice, AccountingInvoiceLine)
            .join(AccountingInvoiceLine, AccountingInvoiceLine.invoice_id == AccountingInvoice.id)
            .filter(
                AccountingInvoice.user_id == user.id,
                AccountingInvoice.invoice_type == "purchase",
                AccountingInvoice.status.in_(["posted", "partially_paid", "paid"]),
            )
        )
        if dt_to:
            pur_q = pur_q.filter(AccountingInvoice.invoice_date <= dt_to)
        purchase_rows = pur_q.all()

        cost_acc: dict[str, dict[str, float]] = {}
        for inv, ln in purchase_rows:
            key = _item_key(ln.description or "", ln.account_id or "")
            qty = abs(float(ln.qty or 0.0))
            if qty <= 0:
                continue
            unit_cost = abs(float(ln.net_amount or 0.0)) / qty if float(ln.net_amount or 0.0) > 0 else abs(float(ln.unit_price or 0.0))
            if unit_cost <= 0:
                continue
            rec = cost_acc.setdefault(key, {"qty": 0.0, "amount": 0.0})
            rec["qty"] += qty
            rec["amount"] += round(unit_cost * qty, 4)

        avg_cost: dict[str, float] = {}
        for k, v in cost_acc.items():
            q = float(v["qty"] or 0.0)
            avg_cost[k] = round(float(v["amount"] or 0.0) / q, 4) if q > 0 else 0.0

        items: list[dict[str, Any]] = []
        t_qty = 0.0
        t_sales = 0.0
        t_tax = 0.0
        t_cost = 0.0
        t_profit = 0.0
        for inv, ln, acc, cp in sale_rows:
            item_name = (ln.description or "").strip() or (acc.name or "")
            if q_txt and q_txt not in item_name.lower() and q_txt not in (acc.code or "").lower():
                continue
            qty = abs(float(ln.qty or 0.0))
            if qty <= 0:
                continue
            sale_unit = round(abs(float(ln.unit_price or 0.0)), 4)
            net_sales = round(abs(float(ln.net_amount or 0.0)), 2)
            tax_amount = round(abs(float(ln.tax_amount or 0.0)), 2)
            gross_sales = round(abs(float(ln.amount or 0.0)), 2)
            key = _item_key(ln.description or "", ln.account_id or "")
            cost_unit = round(float(avg_cost.get(key, 0.0)), 4)
            cost_total = round(cost_unit * qty, 2)
            profit = round(net_sales - cost_total, 2)
            margin_pct = round((profit / net_sales) * 100.0, 2) if net_sales > 0 else 0.0
            t_qty += qty
            t_sales += net_sales
            t_tax += tax_amount
            t_cost += cost_total
            t_profit += profit
            items.append(
                {
                    "invoice_id": inv.id,
                    "invoice_no": inv.invoice_no,
                    "invoice_date": _to_iso(inv.invoice_date),
                    "customer_name": cp.name,
                    "item_name": item_name,
                    "account_code": acc.code or "",
                    "qty": round(qty, 4),
                    "sale_unit_price": sale_unit,
                    "cost_unit_price": cost_unit,
                    "net_sales": net_sales,
                    "tax_amount": tax_amount,
                    "gross_sales": gross_sales,
                    "cost_total": cost_total,
                    "profit": profit,
                    "profit_margin_pct": margin_pct,
                }
            )

        return {
            "items": items,
            "totals": {
                "qty": round(t_qty, 4),
                "net_sales": round(t_sales, 2),
                "tax_amount": round(t_tax, 2),
                "cost_total": round(t_cost, 2),
                "profit": round(t_profit, 2),
                "profit_margin_pct": round((t_profit / t_sales) * 100.0, 2) if t_sales > 0 else 0.0,
            },
            "cost_method": "weighted_avg_from_posted_purchases",
        }
    finally:
        db.close()
