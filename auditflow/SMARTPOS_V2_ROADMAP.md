# SmartPOS V2 Build Roadmap

This project now exposes a stable `v2` API surface under `/api/v2` aligned with the SmartPOS specification.

## Implemented Foundation

- JWT login endpoint: `POST /api/v2/auth/login`
- JWT profile endpoint: `GET /api/v2/auth/me`
- Unified settings endpoints:
  - `GET /api/v2/settings`
  - `PATCH /api/v2/settings`
- Core business endpoints mapped to existing SmartPOS logic:
  - Items: `GET/POST/PUT /api/v2/items`
  - Customers: `GET /api/v2/customers`
  - Suppliers: `GET /api/v2/suppliers`
  - Purchases: `POST /api/v2/purchases`
  - Sales: `POST /api/v2/sales`
  - Returns:
    - `POST /api/v2/sales/{sale_id}/return`
    - `POST /api/v2/purchases/{purchase_id}/return`
  - Inventory:
    - `GET /api/v2/inventory`
    - `GET /api/v2/inventory/movements`
  - Expenses: `POST /api/v2/expenses`
  - Reports:
    - `GET /api/v2/reports/sales`
    - `GET /api/v2/reports/purchases`
    - `GET /api/v2/reports/inventory`
    - `GET /api/v2/reports/profit`
    - `GET /api/v2/reports/tax-return`

## Next Build Steps

1. Create React + Tailwind RTL frontend shell (`frontend-react`) and connect it to `/api/v2`.
2. Implement JWT bearer middleware enforcement on all `v2` routes (currently login/me are JWT-first, while business routes still use existing session checks).
3. Build POS screen in React with:
   - barcode/name search
   - cart
   - mixed payments
   - suspend/retrieve
4. Add dedicated `v2` seed script:
   - default admin
   - categories, units
   - sample items/customers/suppliers
5. Add regression tests for SmartPOS flows:
   - purchase in
   - sale out
   - return reverse
   - tax return consistency
