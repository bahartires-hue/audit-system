# SmartPOS V2 Quick Run

## 1) Backend

From `auditflow`:

```bash
python -m pip install -r requirements.txt
```

Then from `auditflow/backend`:

```bash
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## 2) Seed data + default admin

From `auditflow/backend`:

```bash
python -m app.scripts.seed_smartpos_v2
```

Default admin credentials:

- username: `admin`
- password: `admin123`

## 3) API v2 base

`http://localhost:8000/api/v2`

Key endpoints:

- `POST /auth/login`
- `GET /auth/me`
- `GET /items`
- `POST /items`
- `POST /purchases`
- `POST /sales`
- `GET /inventory`
- `GET /reports/profit`
- `GET /reports/tax-return`

## 4) Notes

- Current v2 routes reuse tested business logic from existing SmartPOS backend.
- React frontend generation is pending a full Node.js + npm toolchain availability on this machine.
