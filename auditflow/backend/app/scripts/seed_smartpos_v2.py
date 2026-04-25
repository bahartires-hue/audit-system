from __future__ import annotations

import datetime as dt
import uuid

from ...auth_core import hash_password
from ...db import SessionLocal
from ...models import AppSetting, Category, Customer, Supplier, Unit, User


def run() -> None:
    db = SessionLocal()
    try:
        admin = db.query(User).filter(User.username == "admin").first()
        if not admin:
            admin = User(
                id=uuid.uuid4().hex,
                username="admin",
                email="admin@smartpos.local",
                password_hash=hash_password("admin123"),
                role_name="admin",
                is_admin=1,
                is_active=1,
                plan_name="free",
                created_at=dt.datetime.utcnow(),
            )
            db.add(admin)
            db.flush()

        uid = admin.id

        categories = [
            ("general", "عام"),
            ("food", "مواد غذائية"),
            ("electronics", "إلكترونيات"),
            ("spare", "قطع غيار"),
        ]
        for code, name in categories:
            if db.query(Category).filter(Category.user_id == uid, Category.code == code).first():
                continue
            db.add(
                Category(
                    id=uuid.uuid4().hex,
                    user_id=uid,
                    code=code,
                    name=name,
                    created_at=dt.datetime.utcnow(),
                )
            )

        units = [("piece", "قطعة"), ("box", "علبة"), ("carton", "كرتون"), ("kg", "كيلو"), ("liter", "لتر")]
        for code, name in units:
            if db.query(Unit).filter(Unit.user_id == uid, Unit.code == code).first():
                continue
            db.add(
                Unit(
                    id=uuid.uuid4().hex,
                    user_id=uid,
                    code=code,
                    name=name,
                    created_at=dt.datetime.utcnow(),
                )
            )

        if not db.query(Customer).filter(Customer.user_id == uid, Customer.name == "عميل نقدي").first():
            db.add(
                Customer(
                    id=uuid.uuid4().hex,
                    user_id=uid,
                    name="عميل نقدي",
                    phone="",
                    city="",
                    address="",
                    opening_balance=0.0,
                    created_at=dt.datetime.utcnow(),
                )
            )

        if not db.query(Supplier).filter(Supplier.user_id == uid, Supplier.name == "مورد افتراضي").first():
            db.add(
                Supplier(
                    id=uuid.uuid4().hex,
                    user_id=uid,
                    name="مورد افتراضي",
                    phone="",
                    city="",
                    address="",
                    opening_balance=0.0,
                    created_at=dt.datetime.utcnow(),
                )
            )

        key = f"cashierko_settings:{uid}"
        row = db.query(AppSetting).filter(AppSetting.key == key).first()
        defaults = {
            "shop_name": "SmartPOS",
            "tax_number": "",
            "tax_rate": 15.0,
            "currency": "SAR",
            "prices_include_tax": True,
            "language": "ar",
            "print_size": "A4",
            "invoice_prefix": "INV",
        }
        if not row:
            db.add(AppSetting(key=key, value_json=defaults, updated_at=dt.datetime.utcnow()))
        else:
            cur = row.value_json if isinstance(row.value_json, dict) else {}
            row.value_json = {**defaults, **cur}
            row.updated_at = dt.datetime.utcnow()

        db.commit()
        print("SmartPOS v2 seed completed.")
        print("Admin username: admin")
        print("Admin password: admin123")
    finally:
        db.close()


if __name__ == "__main__":
    run()
