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
    allowed_pages = Column(JSON, nullable=True)
    last_backup_at = Column(DateTime, nullable=True)


class Role(Base):
    __tablename__ = "roles"

    id = Column(String, primary_key=True)
    name = Column(String, unique=True, nullable=False, index=True)
    display_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Permission(Base):
    __tablename__ = "permissions"

    id = Column(String, primary_key=True)
    code = Column(String, unique=True, nullable=False, index=True)
    display_name = Column(String, nullable=False)
    module = Column(String, nullable=False, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class RolePermission(Base):
    __tablename__ = "role_permissions"

    id = Column(String, primary_key=True)
    role_id = Column(String, ForeignKey("roles.id"), nullable=False, index=True)
    permission_id = Column(String, ForeignKey("permissions.id"), nullable=False, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


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
    employee_id = Column(String, ForeignKey("employees.id"), nullable=True, index=True)
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


class Document(Base):
    __tablename__ = "documents"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    doc_type = Column(String, nullable=False, default="file")  # invoice | contract | file | attachment
    title = Column(String, nullable=False)
    original_filename = Column(String, nullable=False)
    stored_filename = Column(String, nullable=False)
    size_bytes = Column(Integer, nullable=False, default=0)
    notes = Column(Text, nullable=True)
    extracted_text = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


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
    report_type = Column(String, nullable=False, default="branches")

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


class Branch(Base):
    __tablename__ = "branches"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    code = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    city = Column(String, nullable=True)
    address = Column(Text, nullable=True)
    is_main = Column(Integer, nullable=False, default=0)
    is_active = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Category(Base):
    __tablename__ = "categories"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    code = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Unit(Base):
    __tablename__ = "units"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    code = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    phone = Column(String, nullable=True)
    city = Column(String, nullable=True)
    address = Column(Text, nullable=True)
    opening_balance = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Customer(Base):
    __tablename__ = "customers"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    phone = Column(String, nullable=True)
    city = Column(String, nullable=True)
    address = Column(Text, nullable=True)
    opening_balance = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Item(Base):
    __tablename__ = "items"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    code = Column(String, nullable=False, index=True)
    barcode = Column(String, nullable=True, index=True)
    name = Column(String, nullable=False, index=True)
    category = Column(String, nullable=False, default="rim", index=True)  # rim | set | accessory
    brand = Column(String, nullable=True, index=True)
    size = Column(String, nullable=True, index=True)
    bolt_pattern = Column(String, nullable=True, index=True)
    pcd = Column(String, nullable=True)
    color = Column(String, nullable=True)
    item_condition = Column(String, nullable=True, index=True)
    location = Column(String, nullable=True, index=True)
    unit = Column(String, nullable=False, default="قطعة", index=True)
    category_id = Column(String, ForeignKey("categories.id"), nullable=True, index=True)
    branch_id = Column(String, ForeignKey("branches.id"), nullable=True, index=True)
    is_set = Column(Integer, nullable=False, default=0, index=True)
    is_unique = Column(Integer, nullable=False, default=0, index=True)
    needs_service = Column(Integer, nullable=False, default=0, index=True)
    quantity = Column(Float, nullable=False, default=0.0)
    min_qty = Column(Float, nullable=False, default=0.0)
    default_sale_price = Column(Float, nullable=False, default=0.0)
    is_taxable = Column(Integer, nullable=False, default=1, index=True)
    tax_rate = Column(Float, nullable=False, default=0.0)
    is_active = Column(Integer, nullable=False, default=1, index=True)
    last_cost = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)
    image_url = Column(String, nullable=True)
    unit_id = Column(String, ForeignKey("units.id"), nullable=True, index=True)
    is_price_tax_inclusive = Column(Integer, nullable=False, default=0, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Purchase(Base):
    __tablename__ = "purchases"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    branch_id = Column(String, ForeignKey("branches.id"), nullable=True, index=True)
    supplier_id = Column(String, ForeignKey("suppliers.id"), nullable=True, index=True)
    invoice_no = Column(String, nullable=False, index=True)
    supplier_name = Column(String, nullable=False, index=True)
    purchase_date = Column(DateTime, nullable=False, index=True)
    payment_type = Column(String, nullable=False, default="cash", index=True)
    tax_amount = Column(Float, nullable=False, default=0.0)
    discount = Column(Float, nullable=False, default=0.0)
    paid_amount = Column(Float, nullable=False, default=0.0)
    due_amount = Column(Float, nullable=False, default=0.0)
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
    branch_id = Column(String, ForeignKey("branches.id"), nullable=True, index=True)
    customer_id = Column(String, ForeignKey("customers.id"), nullable=True, index=True)
    invoice_no = Column(String, nullable=False, index=True)
    customer_name = Column(String, nullable=False, index=True)
    customer_tax_no = Column(String, nullable=True, index=True)
    customer_phone = Column(String, nullable=True, index=True)
    customer_address = Column(Text, nullable=True)
    sale_date = Column(DateTime, nullable=False, index=True)
    payment_type = Column(String, nullable=False, default="cash", index=True)  # cash | transfer | credit
    tax_amount = Column(Float, nullable=False, default=0.0)
    paid_amount = Column(Float, nullable=False, default=0.0)
    due_amount = Column(Float, nullable=False, default=0.0)
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


class SuspendedSale(Base):
    __tablename__ = "suspended_sales"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    invoice_no = Column(String, nullable=False, index=True)
    customer_name = Column(String, nullable=False, index=True)
    sale_date = Column(DateTime, nullable=False, index=True)
    payment_type = Column(String, nullable=False, default="cash", index=True)
    discount = Column(Float, nullable=False, default=0.0)
    paid_amount = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)
    seller_name = Column(String, nullable=True, index=True)
    branch_name = Column(String, nullable=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class SuspendedSaleLine(Base):
    __tablename__ = "suspended_sale_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    suspended_sale_id = Column(String, ForeignKey("suspended_sales.id"), nullable=False, index=True)
    item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    qty = Column(Float, nullable=False, default=1.0)
    sale_price = Column(Float, nullable=False, default=0.0)
    tax_amount = Column(Float, nullable=False, default=0.0)


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


class ItemImage(Base):
    __tablename__ = "item_images"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    image_url = Column(String, nullable=False)
    is_primary = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class ReturnTxn(Base):
    __tablename__ = "returns"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    return_type = Column(String, nullable=False, index=True)  # sale_return | purchase_return
    reference_type = Column(String, nullable=False, index=True)  # sale | purchase
    reference_id = Column(String, nullable=False, index=True)
    invoice_no = Column(String, nullable=False, index=True)
    return_date = Column(DateTime, nullable=False, index=True)
    customer_name = Column(String, nullable=True, index=True)
    supplier_name = Column(String, nullable=True, index=True)
    reason = Column(Text, nullable=True)
    total_amount = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class ReturnLine(Base):
    __tablename__ = "return_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(String, ForeignKey("returns.id"), nullable=False, index=True)
    item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    qty = Column(Float, nullable=False, default=0.0)
    unit_price = Column(Float, nullable=False, default=0.0)
    unit_cost = Column(Float, nullable=False, default=0.0)
    line_total = Column(Float, nullable=False, default=0.0)


class Expense(Base):
    __tablename__ = "expenses"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    branch_id = Column(String, ForeignKey("branches.id"), nullable=True, index=True)
    expense_date = Column(DateTime, nullable=False, index=True)
    expense_type = Column(String, nullable=False, index=True)
    amount = Column(Float, nullable=False, default=0.0)
    payment_type = Column(String, nullable=False, default="cash")
    description = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class StockAdjustment(Base):
    __tablename__ = "stock_adjustments"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    branch_id = Column(String, ForeignKey("branches.id"), nullable=True, index=True)
    adjust_date = Column(DateTime, nullable=False, index=True)
    qty_before = Column(Float, nullable=False, default=0.0)
    qty_after = Column(Float, nullable=False, default=0.0)
    difference = Column(Float, nullable=False, default=0.0)
    reason = Column(Text, nullable=False)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class BranchTransfer(Base):
    __tablename__ = "branch_transfers"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    transfer_no = Column(String, nullable=False, index=True)
    transfer_date = Column(DateTime, nullable=False, index=True)
    from_branch_id = Column(String, ForeignKey("branches.id"), nullable=False, index=True)
    to_branch_id = Column(String, ForeignKey("branches.id"), nullable=False, index=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class BranchTransferLine(Base):
    __tablename__ = "branch_transfer_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transfer_id = Column(String, ForeignKey("branch_transfers.id"), nullable=False, index=True)
    from_item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    to_item_id = Column(String, ForeignKey("items.id"), nullable=False, index=True)
    qty = Column(Float, nullable=False, default=0.0)


class ImporterSnapshot(Base):
    """حفظ داخل النظام لنتائج سحب المستورد (استرجاع لاحقاً دون الاعتماد على تحميل ملفات)."""

    __tablename__ = "importer_snapshots"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    kind = Column(String, nullable=False, index=True)
    label = Column(String, nullable=True)
    request_json = Column(JSON, nullable=False, default=dict)
    result_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)



# ============================================================ المخزون البسيط (نظام بسيط ومستقل)
class SimpleProduct(Base):
    """صنف في نظام المخزون البسيط. نظام بسيط ومستقل تمامًا عن نظام Item/Purchase/Sale الأصلي وعن محرك المطابقة."""

    __tablename__ = "simple_products"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False, index=True)
    purchase_price = Column(Float, nullable=False, default=0.0)
    sale_price = Column(Float, nullable=False, default=0.0)
    quantity = Column(Integer, nullable=False, default=0)
    notes = Column(String, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class SimplePurchase(Base):
    __tablename__ = "simple_purchases"

    id = Column(String, primary_key=True)
    product_id = Column(String, ForeignKey("simple_products.id"), nullable=False, index=True)
    quantity = Column(Integer, nullable=False)
    purchase_price = Column(Float, nullable=False)
    total = Column(Float, nullable=False)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)


class SimpleSale(Base):
    __tablename__ = "simple_sales"

    id = Column(String, primary_key=True)
    product_id = Column(String, ForeignKey("simple_products.id"), nullable=False, index=True)
    quantity = Column(Integer, nullable=False)
    sale_price = Column(Float, nullable=False)
    total = Column(Float, nullable=False)
    profit = Column(Float, nullable=False, default=0.0)
    customer_name = Column(String, nullable=False, default="نقدي")
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False, index=True)


# ============================================================ بيانات الشركة الأساسية (للفواتير والتقارير)
class CompanyProfile(Base):
    """سجل واحد ثابت (id='default') يحمل بيانات الشركة الأساسية المستخدمة في الفواتير والتقارير."""

    __tablename__ = "company_profile"

    id = Column(String, primary_key=True, default="default")
    company_name = Column(String, nullable=True)
    trade_name = Column(String, nullable=True)
    logo_url = Column(String, nullable=True)
    commercial_register = Column(String, nullable=True)
    tax_number = Column(String, nullable=True)
    address = Column(String, nullable=True)
    city = Column(String, nullable=True)
    country = Column(String, nullable=True)
    postal_code = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    email = Column(String, nullable=True)
    website = Column(String, nullable=True)
    currency = Column(String, nullable=False, default="SAR")
    vat_percentage = Column(Float, nullable=False, default=15.0)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False)


class Employee(Base):
    __tablename__ = "employees"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_number = Column(String, nullable=True, index=True)
    name = Column(String, nullable=False, index=True)
    nationality = Column(String, nullable=True)
    national_id = Column(String, nullable=True)
    residency_expiry = Column(DateTime, nullable=True, index=True)
    passport_number = Column(String, nullable=True)
    passport_expiry = Column(DateTime, nullable=True, index=True)
    position = Column(String, nullable=True)
    department = Column(String, nullable=True, index=True)
    branch_name = Column(String, nullable=True, index=True)
    basic_salary = Column(Float, nullable=False, default=0.0)
    commission_percentage = Column(Float, nullable=True)
    hire_date = Column(DateTime, nullable=True)
    birth_date = Column(DateTime, nullable=True)
    phone = Column(String, nullable=True)
    email = Column(String, nullable=True)
    status = Column(String, nullable=False, default="active", index=True)  # active|on_leave|suspended|resigned|terminated
    photo_url = Column(String, nullable=True)
    is_active = Column(Integer, nullable=False, default=1, index=True)
    notes = Column(Text, nullable=True)
    gender = Column(String, nullable=True)  # male|female
    address = Column(Text, nullable=True)
    manager_id = Column(String, ForeignKey("employees.id"), nullable=True, index=True)
    bank_name = Column(String, nullable=True)
    iban = Column(String, nullable=True)
    bank_salary = Column(Float, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class EmployeeDocument(Base):
    __tablename__ = "employee_documents"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    doc_type = Column(String, nullable=False, index=True)
    doc_number = Column(String, nullable=True)
    issue_date = Column(DateTime, nullable=True)
    expiry_date = Column(DateTime, nullable=True, index=True)
    issuing_authority = Column(String, nullable=True)
    notes = Column(Text, nullable=True)
    file_url = Column(String, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False)


class EmployeeDocumentRenewal(Base):
    __tablename__ = "employee_document_renewals"

    id = Column(String, primary_key=True)
    document_id = Column(String, ForeignKey("employee_documents.id"), nullable=False, index=True)
    old_issue_date = Column(DateTime, nullable=True)
    old_expiry_date = Column(DateTime, nullable=True)
    renewed_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    renewed_by_name = Column(String, nullable=True)
    notes = Column(Text, nullable=True)


class Payroll(Base):
    __tablename__ = "payrolls"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    month = Column(Integer, nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    base_salary = Column(Float, nullable=False, default=0.0)
    total_allowances = Column(Float, nullable=False, default=0.0)
    total_deductions = Column(Float, nullable=False, default=0.0)
    gross_before_tax = Column(Float, nullable=False, default=0.0)
    tax_amount = Column(Float, nullable=False, default=0.0)
    net_salary = Column(Float, nullable=False, default=0.0)
    status = Column(String, nullable=False, default="unpaid", index=True)  # unpaid | paid | cancelled
    paid_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)
    # بنود يدوية إضافية أدخلها المستخدم عند الإنشاء — تُحفظ هنا (JSON) كي تُعاد نفسها بدقة
    # عند "إعادة الحساب" لاحقًا، بدل محاولة تخمينها من نصوص البنود المحفوظة.
    extra_allowances_json = Column(Text, nullable=True)
    extra_deductions_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class PayrollAllowance(Base):
    __tablename__ = "payroll_allowances"

    id = Column(Integer, primary_key=True, autoincrement=True)
    payroll_id = Column(String, ForeignKey("payrolls.id"), nullable=False, index=True)
    label = Column(String, nullable=False)
    amount = Column(Float, nullable=False, default=0.0)


class PayrollDeduction(Base):
    __tablename__ = "payroll_deductions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    payroll_id = Column(String, ForeignKey("payrolls.id"), nullable=False, index=True)
    label = Column(String, nullable=False)
    amount = Column(Float, nullable=False, default=0.0)



class EmployeeContract(Base):
    __tablename__ = "employee_contracts"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    contract_type = Column(String, nullable=True)
    start_date = Column(DateTime, nullable=True, index=True)
    end_date = Column(DateTime, nullable=True, index=True)
    salary = Column(Float, nullable=False, default=0.0)
    housing_allowance = Column(Float, nullable=False, default=0.0)
    transport_allowance = Column(Float, nullable=False, default=0.0)
    other_allowances = Column(Float, nullable=False, default=0.0)
    probation_days = Column(Integer, nullable=False, default=0)
    annual_leave_days = Column(Integer, nullable=False, default=21)
    file_url = Column(String, nullable=True)
    status = Column(String, nullable=False, default="active", index=True)  # active|ended
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow, nullable=False)


class Advance(Base):
    __tablename__ = "hr_advances"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    date = Column(DateTime, nullable=True, index=True)
    amount = Column(Float, nullable=False, default=0.0)
    reason = Column(String, nullable=True)
    installments_count = Column(Integer, nullable=False, default=1)
    installment_amount = Column(Float, nullable=False, default=0.0)
    remaining_amount = Column(Float, nullable=False, default=0.0)
    status = Column(String, nullable=False, default="active", index=True)  # active|completed|cancelled
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class DeductionRecord(Base):
    __tablename__ = "hr_deductions"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    deduction_type = Column(String, nullable=True)
    amount = Column(Float, nullable=False, default=0.0)
    reason = Column(String, nullable=True)
    date = Column(DateTime, nullable=True, index=True)
    applied = Column(Integer, nullable=False, default=0, index=True)
    payroll_id = Column(String, ForeignKey("payrolls.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class AllowanceRecord(Base):
    __tablename__ = "hr_allowances"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    allowance_type = Column(String, nullable=True)
    amount = Column(Float, nullable=False, default=0.0)
    recurring = Column(Integer, nullable=False, default=0, index=True)
    date = Column(DateTime, nullable=True, index=True)
    applied = Column(Integer, nullable=False, default=0, index=True)
    payroll_id = Column(String, ForeignKey("payrolls.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class CommissionRecord(Base):
    __tablename__ = "hr_commissions"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    sales_amount = Column(Float, nullable=False, default=0.0)
    percentage = Column(Float, nullable=False, default=0.0)
    commission_value = Column(Float, nullable=False, default=0.0)
    date = Column(DateTime, nullable=True, index=True)
    applied = Column(Integer, nullable=False, default=0, index=True)
    payroll_id = Column(String, ForeignKey("payrolls.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class LeaveRequest(Base):
    __tablename__ = "hr_leaves"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    leave_type = Column(String, nullable=True)
    start_date = Column(DateTime, nullable=True, index=True)
    end_date = Column(DateTime, nullable=True, index=True)
    days_count = Column(Integer, nullable=False, default=0)
    paid = Column(Integer, nullable=False, default=1)
    status = Column(String, nullable=False, default="pending", index=True)  # pending|accepted|rejected
    applied = Column(Integer, nullable=False, default=0, index=True)
    payroll_id = Column(String, ForeignKey("payrolls.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class Attendance(Base):
    __tablename__ = "hr_attendance"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    check_in = Column(String, nullable=True)
    check_out = Column(String, nullable=True)
    late_minutes = Column(Integer, nullable=False, default=0)
    overtime_minutes = Column(Integer, nullable=False, default=0)
    is_absent = Column(Integer, nullable=False, default=0, index=True)
    is_early_leave = Column(Integer, nullable=False, default=0, index=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class EndOfService(Base):
    __tablename__ = "hr_end_of_service"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    end_date = Column(DateTime, nullable=True)
    reason = Column(String, nullable=True)  # resigned|terminated|contract_end|retirement
    remaining_salaries = Column(Float, nullable=False, default=0.0)
    remaining_advances = Column(Float, nullable=False, default=0.0)
    deductions_total = Column(Float, nullable=False, default=0.0)
    leave_balance_days = Column(Float, nullable=False, default=0.0)
    leave_balance_value = Column(Float, nullable=False, default=0.0)
    gratuity_amount = Column(Float, nullable=False, default=0.0)
    total_dues = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class WithdrawalRecord(Base):
    __tablename__ = "hr_withdrawals"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    date = Column(DateTime, nullable=True, index=True)
    amount = Column(Float, nullable=False, default=0.0)
    reason = Column(String, nullable=True)
    status = Column(String, nullable=False, default="pending", index=True)  # pending|settled|cancelled
    applied = Column(Integer, nullable=False, default=0, index=True)
    payroll_id = Column(String, ForeignKey("payrolls.id"), nullable=True, index=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class TravelRecord(Base):
    __tablename__ = "hr_travel"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    travel_type = Column(String, nullable=False, default="domestic")  # domestic|international
    destination = Column(String, nullable=True)
    departure_date = Column(DateTime, nullable=True, index=True)
    return_date = Column(DateTime, nullable=True, index=True)  # planned return
    actual_return_date = Column(DateTime, nullable=True)
    purpose = Column(String, nullable=True)
    status = Column(String, nullable=False, default="scheduled", index=True)  # scheduled|ongoing|completed|cancelled
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class CustodyRecord(Base):
    __tablename__ = "hr_custody"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False, index=True)
    item_name = Column(String, nullable=False)
    item_type = Column(String, nullable=True)
    serial_number = Column(String, nullable=True)
    value = Column(Float, nullable=True)
    assigned_date = Column(DateTime, nullable=True, index=True)
    expected_return_date = Column(DateTime, nullable=True)
    actual_return_date = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="assigned", index=True)  # assigned|returned|lost|damaged
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)


class HRConfigItem(Base):
    __tablename__ = "hr_config_items"

    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    category = Column(String, nullable=False, index=True)  # department|position|leave_type|allowance_type|deduction_type|branch
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=dt.datetime.utcnow, nullable=False)



def init_db() -> None:
    from .db import engine
    from .migrate import run_migrations

    Base.metadata.create_all(bind=engine)
    run_migrations()
