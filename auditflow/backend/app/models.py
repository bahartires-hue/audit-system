from __future__ import annotations

import datetime as dt

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, JSON, String, Text

from .db import Base


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True)
    username = Column(String, unique=True, nullable=False, index=True)
    email = Column(String, unique=True, nullable=True, index=True)
    is_admin = Column(Integer, nullable=False, default=0)
    role_name = Column(String, nullable=False, default="user")
    is_active = Column(Integer, nullable=False, default=1)
    plan_name = Column(String, nullable=False, default="free")
    subscription_expires_at = Column(DateTime, nullable=True)
    password_hash = Column(String, nullable=False)
    failed_attempts = Column(Integer, nullable=False, default=0)
    locked_until = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    preferences_json = Column(JSON, nullable=False, default=lambda: {})


class UserSession(Base):
    __tablename__ = "user_sessions"

    token = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=True, index=True)
    action = Column(String, nullable=False)
    meta_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class InviteCode(Base):
    __tablename__ = "invite_codes"

    code = Column(String, primary_key=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=True, index=True)
    max_uses = Column(Integer, nullable=False, default=1)
    used_count = Column(Integer, nullable=False, default=0)
    expires_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    disabled = Column(Integer, nullable=False, default=0)


class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"

    token = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    expires_at = Column(DateTime, nullable=False)
    used = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class AppSetting(Base):
    __tablename__ = "app_settings"

    key = Column(String, primary_key=True)
    value_json = Column(JSON, nullable=False, default=dict)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class AnalysisReport(Base):
    __tablename__ = "analysis_reports"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=True, index=True)
    title = Column(String, nullable=True)

    branch1_name = Column(String, nullable=False)
    branch2_name = Column(String, nullable=False)

    file1_original = Column(String, nullable=True)
    file2_original = Column(String, nullable=True)
    file1_path = Column(String, nullable=True)
    file2_path = Column(String, nullable=True)

    status = Column(String, default="completed", nullable=False)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)

    total_ops = Column(Integer, nullable=False, default=0)
    matched_ops = Column(Integer, nullable=False, default=0)
    mismatch_ops = Column(Integer, nullable=False, default=0)
    errors_count = Column(Integer, nullable=False, default=0)
    warnings_count = Column(Integer, nullable=False, default=0)

    stats_json = Column(JSON, nullable=False, default=dict)
    analysis_json = Column(JSON, nullable=False, default=dict)

    tags_json = Column(JSON, nullable=False, default=lambda: [])
    notes = Column(Text, nullable=True)
    archived = Column(Integer, nullable=False, default=0)


class Account(Base):
    __tablename__ = "accounts"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    code = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    account_type = Column(String, nullable=False, index=True)
    parent_id = Column(String, ForeignKey("accounts.id"), nullable=True, index=True)
    is_active = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class JournalEntry(Base):
    __tablename__ = "journal_entries"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    entry_date = Column(DateTime, nullable=False, index=True)
    reference = Column(String, nullable=True, index=True)
    doc_type = Column(String, nullable=True, index=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class JournalLine(Base):
    __tablename__ = "journal_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entry_id = Column(String, ForeignKey("journal_entries.id"), nullable=False, index=True)
    account_id = Column(String, ForeignKey("accounts.id"), nullable=False, index=True)
    description = Column(Text, nullable=True)
    debit = Column(Float, nullable=False, default=0.0)
    credit = Column(Float, nullable=False, default=0.0)


class Counterparty(Base):
    __tablename__ = "counterparties"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    code = Column(String, nullable=True, index=True)
    name = Column(String, nullable=False, index=True)
    party_type = Column(String, nullable=False, index=True)  # customer | supplier
    phone = Column(String, nullable=True)
    email = Column(String, nullable=True)
    address = Column(Text, nullable=True)
    is_active = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class AccountingInvoice(Base):
    __tablename__ = "accounting_invoices"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    invoice_type = Column(String, nullable=False, index=True)  # sale | purchase
    invoice_no = Column(String, nullable=False, index=True)
    invoice_date = Column(DateTime, nullable=False, index=True)
    counterparty_id = Column(String, ForeignKey("counterparties.id"), nullable=False, index=True)
    description = Column(Text, nullable=True)
    total_amount = Column(Float, nullable=False, default=0.0)
    status = Column(String, nullable=False, default="draft", index=True)  # draft | posted
    offset_account_id = Column(String, ForeignKey("accounts.id"), nullable=False, index=True)
    journal_entry_id = Column(String, ForeignKey("journal_entries.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class AccountingInvoiceLine(Base):
    __tablename__ = "accounting_invoice_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id = Column(String, ForeignKey("accounting_invoices.id"), nullable=False, index=True)
    account_id = Column(String, ForeignKey("accounts.id"), nullable=False, index=True)
    description = Column(Text, nullable=True)
    qty = Column(Float, nullable=False, default=1.0)
    unit_price = Column(Float, nullable=False, default=0.0)
    amount = Column(Float, nullable=False, default=0.0)


def init_db() -> None:
    from .db import engine
    from .migrate import run_migrations

    Base.metadata.create_all(bind=engine)
    run_migrations()
