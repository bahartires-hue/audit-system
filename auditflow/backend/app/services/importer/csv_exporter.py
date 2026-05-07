from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def export_products_files(products: List[Dict[str, Any]], csv_path: Path, xlsx_path: Path) -> Dict[str, str]:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(products)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    return {"csv_path": str(csv_path), "xlsx_path": str(xlsx_path)}

