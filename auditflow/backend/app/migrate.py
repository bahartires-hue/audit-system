from __future__ import annotations

from sqlalchemy import text

from .db import engine


def _migrate_postgresql() -> None:
    """أعمدة أضيفت لاحقاً: create_all لا يحدّث جداول موجودة على Render/Postgres."""
    stmts = [
        # users
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS role_name VARCHAR DEFAULT 'user'",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active INTEGER DEFAULT 1",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS plan_name VARCHAR DEFAULT 'free'",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS subscription_expires_at TIMESTAMP",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_attempts INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until TIMESTAMP",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS preferences_json JSON DEFAULT '{}'::json",
        # items
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS bolt_pattern VARCHAR",
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS category_id VARCHAR",
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS branch_id VARCHAR",
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS is_unique INTEGER DEFAULT 0",
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS needs_service INTEGER DEFAULT 0",
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS min_qty DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS unit VARCHAR DEFAULT 'قطعة'",
        # purchases
        "ALTER TABLE purchases ADD COLUMN IF NOT EXISTS branch_id VARCHAR",
        "ALTER TABLE purchases ADD COLUMN IF NOT EXISTS supplier_id VARCHAR",
        "ALTER TABLE purchases ADD COLUMN IF NOT EXISTS payment_type VARCHAR DEFAULT 'cash'",
        "ALTER TABLE purchases ADD COLUMN IF NOT EXISTS tax_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE purchases ADD COLUMN IF NOT EXISTS discount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE purchases ADD COLUMN IF NOT EXISTS paid_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE purchases ADD COLUMN IF NOT EXISTS due_amount DOUBLE PRECISION DEFAULT 0",
        # sales
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS branch_id VARCHAR",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS customer_id VARCHAR",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS tax_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS paid_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS due_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS customer_tax_no VARCHAR",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS customer_phone VARCHAR",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS customer_address TEXT",
        # analysis_reports
        "ALTER TABLE analysis_reports ADD COLUMN IF NOT EXISTS user_id VARCHAR",
        "ALTER TABLE analysis_reports ADD COLUMN IF NOT EXISTS tags_json JSON DEFAULT '[]'::json",
        "ALTER TABLE analysis_reports ADD COLUMN IF NOT EXISTS notes TEXT",
        "ALTER TABLE analysis_reports ADD COLUMN IF NOT EXISTS archived INTEGER DEFAULT 0",
        "ALTER TABLE IF EXISTS accounting_invoices ADD COLUMN IF NOT EXISTS subtotal_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE IF EXISTS accounting_invoices ADD COLUMN IF NOT EXISTS tax_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE IF EXISTS accounting_invoices ADD COLUMN IF NOT EXISTS paid_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE IF EXISTS accounting_invoice_lines ADD COLUMN IF NOT EXISTS tax_rate DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE IF EXISTS accounting_invoice_lines ADD COLUMN IF NOT EXISTS net_amount DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE IF EXISTS accounting_invoice_lines ADD COLUMN IF NOT EXISTS tax_amount DOUBLE PRECISION DEFAULT 0",
    ]
    with engine.begin() as conn:
        for sql in stmts:
            conn.execute(text(sql))
        conn.execute(
            text(
                "UPDATE analysis_reports SET archived = 0 WHERE archived IS NULL"
            )
        )
        conn.execute(
            text("UPDATE analysis_reports SET tags_json = '[]'::json WHERE tags_json IS NULL")
        )
        conn.execute(
            text(
                "UPDATE users SET preferences_json = '{}'::json WHERE preferences_json IS NULL"
            )
        )
        conn.execute(text("UPDATE accounting_invoices SET subtotal_amount = COALESCE(total_amount, 0) WHERE subtotal_amount IS NULL"))
        conn.execute(text("UPDATE accounting_invoices SET tax_amount = 0 WHERE tax_amount IS NULL"))
        conn.execute(text("UPDATE accounting_invoices SET paid_amount = 0 WHERE paid_amount IS NULL"))
        conn.execute(text("UPDATE accounting_invoice_lines SET tax_rate = 0 WHERE tax_rate IS NULL"))
        conn.execute(text("UPDATE accounting_invoice_lines SET net_amount = COALESCE(amount, 0) WHERE net_amount IS NULL"))
        conn.execute(text("UPDATE accounting_invoice_lines SET tax_amount = 0 WHERE tax_amount IS NULL"))
        conn.execute(text("UPDATE users SET is_active = 1 WHERE is_active IS NULL"))
        conn.execute(text("UPDATE users SET plan_name = 'free' WHERE plan_name IS NULL"))
        conn.execute(
            text(
                "UPDATE users SET role_name = 'admin' WHERE is_admin = 1 AND (role_name IS NULL OR role_name = '')"
            )
        )
        conn.execute(
            text("UPDATE users SET role_name = 'user' WHERE role_name IS NULL OR role_name = ''")
        )


def run_migrations() -> None:
    """أعمدة ناقصة على قواعد قديمة (SQLite محلياً، Postgres على Render)."""
    if engine.dialect.name == "postgresql":
        _migrate_postgresql()
        return
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        r = conn.execute(text("PRAGMA table_info(analysis_reports)"))
        cols = [row[1] for row in r.fetchall()]
        if cols and "user_id" not in cols:
            conn.execute(text("ALTER TABLE analysis_reports ADD COLUMN user_id VARCHAR"))

        r2 = conn.execute(text("PRAGMA table_info(users)"))
        ucols = [row[1] for row in r2.fetchall()]
        if ucols:
            if "failed_attempts" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN failed_attempts INTEGER DEFAULT 0"))
            if "locked_until" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN locked_until DATETIME"))
            if "email" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN email VARCHAR"))
            if "is_admin" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0"))
            if "role_name" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN role_name VARCHAR DEFAULT 'user'"))
            if "is_active" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1"))
            if "plan_name" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN plan_name VARCHAR DEFAULT 'free'"))
            if "subscription_expires_at" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN subscription_expires_at DATETIME"))
            if "preferences_json" not in ucols:
                conn.execute(text("ALTER TABLE users ADD COLUMN preferences_json JSON"))

        r_items = conn.execute(text("PRAGMA table_info(items)"))
        item_cols = [row[1] for row in r_items.fetchall()]
        if item_cols:
            if "bolt_pattern" not in item_cols:
                conn.execute(text("ALTER TABLE items ADD COLUMN bolt_pattern VARCHAR"))
            if "category_id" not in item_cols:
                conn.execute(text("ALTER TABLE items ADD COLUMN category_id VARCHAR"))
            if "branch_id" not in item_cols:
                conn.execute(text("ALTER TABLE items ADD COLUMN branch_id VARCHAR"))
            if "is_unique" not in item_cols:
                conn.execute(text("ALTER TABLE items ADD COLUMN is_unique INTEGER DEFAULT 0"))
            if "needs_service" not in item_cols:
                conn.execute(text("ALTER TABLE items ADD COLUMN needs_service INTEGER DEFAULT 0"))
            if "min_qty" not in item_cols:
                conn.execute(text("ALTER TABLE items ADD COLUMN min_qty REAL DEFAULT 0"))
            if "unit" not in item_cols:
                conn.execute(text("ALTER TABLE items ADD COLUMN unit VARCHAR DEFAULT 'قطعة'"))

        r_purchase = conn.execute(text("PRAGMA table_info(purchases)"))
        purchase_cols = [row[1] for row in r_purchase.fetchall()]
        if purchase_cols:
            if "branch_id" not in purchase_cols:
                conn.execute(text("ALTER TABLE purchases ADD COLUMN branch_id VARCHAR"))
            if "supplier_id" not in purchase_cols:
                conn.execute(text("ALTER TABLE purchases ADD COLUMN supplier_id VARCHAR"))
            if "payment_type" not in purchase_cols:
                conn.execute(text("ALTER TABLE purchases ADD COLUMN payment_type VARCHAR DEFAULT 'cash'"))
            if "tax_amount" not in purchase_cols:
                conn.execute(text("ALTER TABLE purchases ADD COLUMN tax_amount REAL DEFAULT 0"))
            if "discount" not in purchase_cols:
                conn.execute(text("ALTER TABLE purchases ADD COLUMN discount REAL DEFAULT 0"))
            if "paid_amount" not in purchase_cols:
                conn.execute(text("ALTER TABLE purchases ADD COLUMN paid_amount REAL DEFAULT 0"))
            if "due_amount" not in purchase_cols:
                conn.execute(text("ALTER TABLE purchases ADD COLUMN due_amount REAL DEFAULT 0"))

        r_sales = conn.execute(text("PRAGMA table_info(sales)"))
        sale_cols = [row[1] for row in r_sales.fetchall()]
        if sale_cols:
            if "branch_id" not in sale_cols:
                conn.execute(text("ALTER TABLE sales ADD COLUMN branch_id VARCHAR"))
            if "customer_id" not in sale_cols:
                conn.execute(text("ALTER TABLE sales ADD COLUMN customer_id VARCHAR"))
            if "tax_amount" not in sale_cols:
                conn.execute(text("ALTER TABLE sales ADD COLUMN tax_amount REAL DEFAULT 0"))
            if "paid_amount" not in sale_cols:
                conn.execute(text("ALTER TABLE sales ADD COLUMN paid_amount REAL DEFAULT 0"))
            if "due_amount" not in sale_cols:
                conn.execute(text("ALTER TABLE sales ADD COLUMN due_amount REAL DEFAULT 0"))
            if "customer_tax_no" not in sale_cols:
                conn.execute(text("ALTER TABLE sales ADD COLUMN customer_tax_no VARCHAR"))
            if "customer_phone" not in sale_cols:
                conn.execute(text("ALTER TABLE sales ADD COLUMN customer_phone VARCHAR"))
            if "customer_address" not in sale_cols:
                conn.execute(text("ALTER TABLE sales ADD COLUMN customer_address TEXT"))

        r3 = conn.execute(text("PRAGMA table_info(analysis_reports)"))
        rcols = [row[1] for row in r3.fetchall()]
        if rcols:
            if "tags_json" not in rcols:
                conn.execute(text("ALTER TABLE analysis_reports ADD COLUMN tags_json JSON"))
            if "notes" not in rcols:
                conn.execute(text("ALTER TABLE analysis_reports ADD COLUMN notes TEXT"))
            if "archived" not in rcols:
                conn.execute(text("ALTER TABLE analysis_reports ADD COLUMN archived INTEGER DEFAULT 0"))

        r4 = conn.execute(text("PRAGMA table_info(accounting_invoices)"))
        inv_cols = [row[1] for row in r4.fetchall()]
        if inv_cols:
            if "subtotal_amount" not in inv_cols:
                conn.execute(text("ALTER TABLE accounting_invoices ADD COLUMN subtotal_amount REAL DEFAULT 0"))
            if "tax_amount" not in inv_cols:
                conn.execute(text("ALTER TABLE accounting_invoices ADD COLUMN tax_amount REAL DEFAULT 0"))
            if "paid_amount" not in inv_cols:
                conn.execute(text("ALTER TABLE accounting_invoices ADD COLUMN paid_amount REAL DEFAULT 0"))

        r5 = conn.execute(text("PRAGMA table_info(accounting_invoice_lines)"))
        inv_line_cols = [row[1] for row in r5.fetchall()]
        if inv_line_cols:
            if "tax_rate" not in inv_line_cols:
                conn.execute(text("ALTER TABLE accounting_invoice_lines ADD COLUMN tax_rate REAL DEFAULT 0"))
            if "net_amount" not in inv_line_cols:
                conn.execute(text("ALTER TABLE accounting_invoice_lines ADD COLUMN net_amount REAL DEFAULT 0"))
            if "tax_amount" not in inv_line_cols:
                conn.execute(text("ALTER TABLE accounting_invoice_lines ADD COLUMN tax_amount REAL DEFAULT 0"))

        conn.execute(text("UPDATE analysis_reports SET archived = 0 WHERE archived IS NULL"))
        conn.execute(text("UPDATE analysis_reports SET tags_json = '[]' WHERE tags_json IS NULL"))
        conn.execute(text("UPDATE accounting_invoices SET subtotal_amount = COALESCE(total_amount, 0) WHERE subtotal_amount IS NULL"))
        conn.execute(text("UPDATE accounting_invoices SET tax_amount = 0 WHERE tax_amount IS NULL"))
        conn.execute(text("UPDATE accounting_invoices SET paid_amount = 0 WHERE paid_amount IS NULL"))
        conn.execute(text("UPDATE accounting_invoice_lines SET tax_rate = 0 WHERE tax_rate IS NULL"))
        conn.execute(text("UPDATE accounting_invoice_lines SET net_amount = COALESCE(amount, 0) WHERE net_amount IS NULL"))
        conn.execute(text("UPDATE accounting_invoice_lines SET tax_amount = 0 WHERE tax_amount IS NULL"))
        conn.execute(text("UPDATE users SET preferences_json = '{}' WHERE preferences_json IS NULL"))
        conn.execute(text("UPDATE users SET is_active = 1 WHERE is_active IS NULL"))
        conn.execute(text("UPDATE users SET plan_name = 'free' WHERE plan_name IS NULL"))
        conn.execute(text("UPDATE users SET role_name = 'admin' WHERE is_admin = 1 AND (role_name IS NULL OR role_name = '')"))
        conn.execute(text("UPDATE users SET role_name = 'user' WHERE role_name IS NULL OR role_name = ''"))
