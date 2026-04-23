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


class Item(Base):
    __tablename__ = "items"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    code = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    category = Column(String, nullable=False, default="rim", index=True)  # rim | set | accessory
    brand = Column(String, nullable=True, index=True)
    size = Column(String, nullable=True, index=True)
    pcd = Column(String, nullable=True)
    color = Column(String, nullable=True)
    item_condition = Column(String, nullable=True, index=True)
    location = Column(String, nullable=True, index=True)
    is_set = Column(Integer, nullable=False, default=0, index=True)
    quantity = Column(Float, nullable=False, default=0.0)
    default_sale_price = Column(Float, nullable=False, default=0.0)
    last_cost = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)
    image_url = Column(String, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Purchase(Base):
    __tablename__ = "purchases"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    invoice_no = Column(String, nullable=False, index=True)
    supplier_name = Column(String, nullable=False, index=True)
    purchase_date = Column(DateTime, nullable=False, index=True)
    total_amount = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class PurchaseLine(Base):
    __tablename__ = "purchase_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    purchase_id = Column(String, ForeignKey("purchases.id"), nullable=False, index=True)
    item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    qty = Column(Float, nullable=False, default=1.0)
    unit_cost = Column(Float, nullable=False, default=0.0)
    extra_cost = Column(Float, nullable=False, default=0.0)
    total_cost = Column(Float, nullable=False, default=0.0)


class Sale(Base):
    __tablename__ = "sales"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    invoice_no = Column(String, nullable=False, index=True)
    customer_name = Column(String, nullable=False, index=True)
    sale_date = Column(DateTime, nullable=False, index=True)
    payment_type = Column(String, nullable=False, default="cash", index=True)  # cash | transfer | credit
    discount = Column(Float, nullable=False, default=0.0)
    total_amount = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)
    seller_name = Column(String, nullable=True, index=True)
    branch_name = Column(String, nullable=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class SaleLine(Base):
    __tablename__ = "sale_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sale_id = Column(String, ForeignKey("sales.id"), nullable=False, index=True)
    item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    qty = Column(Float, nullable=False, default=1.0)
    sale_price = Column(Float, nullable=False, default=0.0)
    tax_amount = Column(Float, nullable=False, default=0.0)
    cost_price = Column(Float, nullable=False, default=0.0)
    profit = Column(Float, nullable=False, default=0.0)


class StockMovement(Base):
    __tablename__ = "stock_movements"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    movement_type = Column(String, nullable=False, index=True)  # purchase | sale | sale_return | purchase_return | adjust
    qty_in = Column(Float, nullable=False, default=0.0)
    qty_out = Column(Float, nullable=False, default=0.0)
    unit_cost = Column(Float, nullable=False, default=0.0)
    reference_type = Column(String, nullable=True, index=True)  # purchase | sale | adjust
    reference_id = Column(String, nullable=True, index=True)
    movement_date = Column(DateTime, nullable=False, index=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


def init_db() -> None:
    from .db import engine
    from .migrate import run_migrations

    Base.metadata.create_all(bind=engine)
    run_migrations()
