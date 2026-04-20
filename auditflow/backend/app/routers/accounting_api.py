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
    Counterparty,
    JournalEntry,
    JournalLine,
)

router = APIRouter(prefix="/accounting", tags=["accounting"])


class AccountCreate(BaseModel):
    code: str = Field(min_length=1, max_length=40)
    name: str = Field(min_length=1, max_length=160)
    account_type: str = Field(min_length=1, max_length=40)
    parent_id: Optional[str] = None


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


class InvoiceCreate(BaseModel):
    invoice_type: str = Field(min_length=1, max_length=20)  # sale | purchase
    invoice_no: str = Field(min_length=1, max_length=60)
    invoice_date: str
    counterparty_id: str
    description: str = ""
    offset_account_id: str
    lines: List[InvoiceLineIn]


def _to_iso(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d")


def _validate_account_ownership(db: Any, user_id: str, account_ids: set[str]) -> None:
    rows = db.query(Account).filter(Account.user_id == user_id, Account.id.in_(list(account_ids))).all()
    if len(rows) != len(account_ids):
        raise HTTPException(400, "يوجد حساب غير صالح")


def _normalize_invoice_lines(lines: List[InvoiceLineIn]) -> tuple[list[dict[str, Any]], float]:
    norm: list[dict[str, Any]] = []
    total = 0.0
    for ln in lines:
        qty = abs(float(ln.qty or 0.0))
        unit_price = abs(float(ln.unit_price or 0.0))
        calc_amount = round(qty * unit_price, 2)
        given = abs(float(ln.amount or 0.0))
        amount = round(given, 2) if given > 0 else calc_amount
        if amount <= 0.0:
            continue
        norm.append(
            {
                "account_id": ln.account_id,
                "description": (ln.description or "").strip(),
                "qty": round(qty if qty > 0 else 1.0, 4),
                "unit_price": round(unit_price if unit_price > 0 else amount, 4),
                "amount": amount,
            }
        )
        total += amount
    return norm, round(total, 2)


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
        try:
            entry_date = dt.datetime.strptime(body.entry_date.strip(), "%Y-%m-%d")
        except Exception:
            raise HTTPException(400, "entry_date يجب أن يكون بصيغة YYYY-MM-DD")

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
                    "total_amount": round(float(r.total_amount or 0.0), 2),
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
        invoice_type = (body.invoice_type or "").strip().lower()
        if invoice_type not in ("sale", "purchase"):
            raise HTTPException(400, "invoice_type يجب أن يكون sale أو purchase")
        try:
            invoice_date = dt.datetime.strptime(body.invoice_date.strip(), "%Y-%m-%d")
        except Exception:
            raise HTTPException(400, "invoice_date يجب أن يكون بصيغة YYYY-MM-DD")

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

        norm_lines, total = _normalize_invoice_lines(body.lines or [])
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
            total_amount=total,
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
            "total_amount": round(float(inv.total_amount or 0.0), 2),
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
        inv = (
            db.query(AccountingInvoice)
            .filter(AccountingInvoice.user_id == user.id, AccountingInvoice.id == invoice_id)
            .first()
        )
        if not inv:
            raise HTTPException(404, "الفاتورة غير موجودة")
        if inv.status == "posted":
            return {"id": inv.id, "status": inv.status, "journal_entry_id": inv.journal_entry_id or ""}

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
