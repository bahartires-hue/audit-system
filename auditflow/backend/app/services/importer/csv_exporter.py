from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def export_products_csv(products: List[Dict[str, Any]], csv_path: Path) -> str:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(products)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return str(csv_path)

