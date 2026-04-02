from __future__ import annotations

import csv
import io
from typing import Any, Dict, List


def mismatches_to_csv_bytes(entries: List[Dict[str, Any]]) -> bytes:
    """
    Return CSV bytes encoded so that Excel opens Arabic correctly.
    """
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["الفرع", "المبلغ", "نوع العملية", "التاريخ", "المستند", "السبب"])
    for e in entries:
        writer.writerow(
            [
                e.get("branch", ""),
                e.get("amount", ""),
                e.get("type", ""),
                e.get("date", "") or "",
                e.get("doc", "") or "",
                e.get("reason", "") or "",
            ]
        )

    # utf-8-sig helps Excel detect Arabic correctly
    return output.getvalue().encode("utf-8-sig")

