from __future__ import annotations

import datetime as dt
import io
import json
import zipfile
from typing import List
from urllib.parse import quote

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response

from ..auth_core import require_csrf, require_user, user_can_access_page_key
from ..db import SessionLocal
from ..services.ai_text_tools import ai_text_enabled, generate_text
from ..services.ocr_vision import extract_text_from_image, ocr_enabled

router = APIRouter(prefix="/api/tools", tags=["toolbox"])


def _require_tool_access(db, request: Request):
    user = require_user(db, request)
    if not user_can_access_page_key(user, "op-finance"):
        raise HTTPException(403, "ليس لديك صلاحية لاستخدام أدوات المساعد الذكي")
    return user


def _attachment(content: bytes, filename: str, media_type: str) -> Response:
    ascii_fallback = "".join(c if ord(c) < 128 else "_" for c in filename) or "download"
    headers = {
        "Content-Disposition": f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{quote(filename, safe='')}"
    }
    return Response(content=content, media_type=media_type, headers=headers)


def _parse_ranges(spec: str, page_count: int) -> List[List[int]]:
    """"1-2,4" -> [[0,1],[3]] (0-indexed page lists, one list per comma-separated segment)."""
    spec = (spec or "").strip()
    if not spec:
        return [list(range(page_count))]
    segments = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        pages = []
        if "-" in part:
            a, b = part.split("-", 1)
            a = int(a.strip())
            b = int(b.strip())
            if a < 1 or b > page_count or a > b:
                raise HTTPException(400, f"نطاق صفحات غير صالح: {part}")
            pages = list(range(a - 1, b))
        else:
            n = int(part.strip())
            if n < 1 or n > page_count:
                raise HTTPException(400, f"رقم صفحة غير صالح: {part}")
            pages = [n - 1]
        segments.append(pages)
    if not segments:
        raise HTTPException(400, "الرجاء إدخال نطاق صفحات صالح")
    return segments


# ---------------------------------------------------------------- PDF merge
@router.post("/pdf-merge")
async def pdf_merge(request: Request, files: List[UploadFile] = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    if len(files) < 2:
        raise HTTPException(400, "الرجاء اختيار ملفين PDF على الأقل للدمج")
    from pypdf import PdfReader, PdfWriter

    writer = PdfWriter()
    for f in files:
        name = (f.filename or "").lower()
        if not name.endswith(".pdf"):
            raise HTTPException(400, "جميع الملفات يجب أن تكون PDF")
        content = await f.read()
        try:
            reader = PdfReader(io.BytesIO(content))
            for page in reader.pages:
                writer.add_page(page)
        except Exception as e:
            raise HTTPException(400, f"تعذر قراءة الملف {f.filename}: {str(e)}")
    out = io.BytesIO()
    writer.write(out)
    return _attachment(out.getvalue(), "merged.pdf", "application/pdf")


# ---------------------------------------------------------------- PDF split
@router.post("/pdf-split")
async def pdf_split(request: Request, file: UploadFile = File(...), ranges: str = Form("")):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not name.endswith(".pdf"):
        raise HTTPException(400, "الملف يجب أن يكون PDF")
    content = await file.read()
    from pypdf import PdfReader, PdfWriter

    try:
        reader = PdfReader(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")
    page_count = len(reader.pages)
    segments = _parse_ranges(ranges, page_count)

    if len(segments) == 1:
        writer = PdfWriter()
        for idx in segments[0]:
            writer.add_page(reader.pages[idx])
        out = io.BytesIO()
        writer.write(out)
        return _attachment(out.getvalue(), "split.pdf", "application/pdf")

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, seg in enumerate(segments, start=1):
            writer = PdfWriter()
            for idx in seg:
                writer.add_page(reader.pages[idx])
            part_buf = io.BytesIO()
            writer.write(part_buf)
            zf.writestr(f"part_{i}.pdf", part_buf.getvalue())
    return _attachment(zip_buf.getvalue(), "split_parts.zip", "application/zip")


# ---------------------------------------------------------- PDF protect/unprotect
@router.post("/pdf-protect")
async def pdf_protect(
    request: Request,
    file: UploadFile = File(...),
    action: str = Form("protect"),
    password: str = Form(""),
):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not name.endswith(".pdf"):
        raise HTTPException(400, "الملف يجب أن يكون PDF")
    pwd = (password or "").strip()
    if not pwd:
        raise HTTPException(400, "الرجاء إدخال كلمة مرور")
    content = await file.read()
    from pypdf import PdfReader, PdfWriter

    act = (action or "protect").strip().lower()
    if act == "protect":
        try:
            reader = PdfReader(io.BytesIO(content))
        except Exception as e:
            raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)
        writer.encrypt(pwd)
        out = io.BytesIO()
        writer.write(out)
        return _attachment(out.getvalue(), "protected.pdf", "application/pdf")
    elif act == "unprotect":
        try:
            reader = PdfReader(io.BytesIO(content))
            if reader.is_encrypted:
                result = reader.decrypt(pwd)
                if not result:
                    raise HTTPException(400, "كلمة المرور غير صحيحة")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, f"كلمة المرور غير صحيحة أو الملف تالف: {str(e)}")
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)
        out = io.BytesIO()
        writer.write(out)
        return _attachment(out.getvalue(), "unprotected.pdf", "application/pdf")
    else:
        raise HTTPException(400, "إجراء غير مدعوم")


# --------------------------------------------------------------- Compression
@router.post("/compress")
async def compress_file(request: Request, file: UploadFile = File(...), quality: int = Form(70)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = file.filename or "file"
    suffix = name.lower().rsplit(".", 1)[-1] if "." in name else ""
    content = await file.read()
    q = max(10, min(int(quality or 70), 95))

    if suffix in ("jpg", "jpeg", "png", "webp"):
        from PIL import Image

        try:
            img = Image.open(io.BytesIO(content))
            img.load()
        except Exception as e:
            raise HTTPException(400, f"تعذر فتح الصورة: {str(e)}")
        out = io.BytesIO()
        if img.mode in ("RGBA", "P") and suffix in ("jpg", "jpeg"):
            img = img.convert("RGB")
        fmt = "JPEG" if suffix in ("jpg", "jpeg") else img.format or "PNG"
        save_kwargs = {"optimize": True}
        if fmt == "JPEG":
            save_kwargs["quality"] = q
        elif fmt == "PNG":
            save_kwargs["compress_level"] = 9
        img.save(out, format=fmt, **save_kwargs)
        out_name = f"compressed_{name}"
        media = "image/jpeg" if fmt == "JPEG" else f"image/{fmt.lower()}"
        return _attachment(out.getvalue(), out_name, media)

    if suffix == "pdf":
        from pypdf import PdfReader, PdfWriter

        try:
            reader = PdfReader(io.BytesIO(content))
        except Exception as e:
            raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)
        for page in writer.pages:
            try:
                page.compress_content_streams()
            except Exception:
                pass
        out = io.BytesIO()
        writer.write(out)
        return _attachment(out.getvalue(), f"compressed_{name}", "application/pdf")

    raise HTTPException(400, "نوع الملف غير مدعوم للضغط (صور أو PDF فقط)")


# --------------------------------------------------------------------- QR code
@router.post("/qrcode")
async def make_qrcode(request: Request, text: str = Form(...), size: int = Form(10)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    txt = (text or "").strip()
    if not txt:
        raise HTTPException(400, "الرجاء إدخال نص أو رابط لتوليد رمز QR")
    import qrcode

    box_size = max(4, min(int(size or 10), 20))
    qr = qrcode.QRCode(box_size=box_size, border=2)
    qr.add_data(txt)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    out = io.BytesIO()
    img.save(out, format="PNG")
    return _attachment(out.getvalue(), "qrcode.png", "image/png")


# ------------------------------------------------------------------- Barcode
@router.post("/barcode")
async def make_barcode(request: Request, text: str = Form(...), barcode_type: str = Form("code128")):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    txt = (text or "").strip()
    if not txt:
        raise HTTPException(400, "الرجاء إدخال نص أو رقم لتوليد الباركود")
    import barcode
    from barcode.writer import ImageWriter

    btype = (barcode_type or "code128").strip().lower()
    try:
        bc_cls = barcode.get_barcode_class(btype)
    except Exception:
        raise HTTPException(400, "نوع الباركود غير مدعوم")
    try:
        bc = bc_cls(txt, writer=ImageWriter())
        out = io.BytesIO()
        bc.write(out)
    except Exception as e:
        raise HTTPException(400, f"تعذر توليد الباركود: {str(e)}")
    return _attachment(out.getvalue(), "barcode.png", "image/png")


# ------------------------------------------------------------------ Date convert
@router.post("/date-convert")
async def date_convert(request: Request, date: str = Form(...), direction: str = Form("g2h")):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    d = (date or "").strip()
    if not d:
        raise HTTPException(400, "الرجاء إدخال تاريخ")
    dirn = (direction or "g2h").strip().lower()
    try:
        y, m, day = [int(x) for x in d.replace("/", "-").split("-")]
    except Exception:
        raise HTTPException(400, "صيغة التاريخ غير صحيحة، استخدم YYYY-MM-DD")

    from hijridate import Gregorian, Hijri

    try:
        if dirn == "g2h":
            h = Gregorian(y, m, day).to_hijri()
            result = f"{h.year}-{h.month:02d}-{h.day:02d}"
            label = "التاريخ الهجري"
        else:
            g = Hijri(y, m, day).to_gregorian()
            result = f"{g.year}-{g.month:02d}-{g.day:02d}"
            label = "التاريخ الميلادي"
    except Exception as e:
        raise HTTPException(400, f"تعذر تحويل التاريخ: {str(e)}")
    return {"result": result, "label": label}


# ------------------------------------------------------------------ Excel clean
@router.post("/excel-clean")
async def excel_clean(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".csv")):
        raise HTTPException(400, "الملف يجب أن يكون Excel أو CSV")
    content = await file.read()
    import pandas as pd

    try:
        if name.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content), dtype=str)
        else:
            df = pd.read_excel(io.BytesIO(content), dtype=str)
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")

    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
    df = df.drop_duplicates()
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed", na=False)]

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="بيانات نظيفة")
    return _attachment(
        out.getvalue(),
        "cleaned.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# --------------------------------------------------------- Excel -> PDF report
@router.post("/excel-report")
async def excel_report(request: Request, file: UploadFile = File(...), title: str = Form("تقرير البيانات")):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".csv")):
        raise HTTPException(400, "الملف يجب أن يكون Excel أو CSV")
    content = await file.read()
    import pandas as pd

    try:
        if name.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")

    if df.empty:
        raise HTTPException(400, "الملف لا يحتوي على بيانات")

    df.columns = [str(c).strip() for c in df.columns]
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    label_cols = [c for c in df.columns if c not in numeric_cols]

    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics.charts.barcharts import VerticalBarChart

    styles = getSampleStyleSheet()
    out = io.BytesIO()
    doc = SimpleDocTemplate(out, pagesize=A4, topMargin=1.5 * cm, bottomMargin=1.5 * cm)
    elements = [Paragraph(str(title or "تقرير البيانات"), styles["Title"]), Spacer(1, 12)]
    elements.append(Paragraph(f"عدد الصفوف: {len(df)} | عدد الأعمدة: {len(df.columns)}", styles["Normal"]))
    elements.append(Spacer(1, 16))

    if numeric_cols:
        value_col = numeric_cols[0]
        label_col = label_cols[0] if label_cols else df.columns[0]
        chart_df = df[[label_col, value_col]].dropna().head(15)
        if not chart_df.empty:
            drawing = Drawing(420, 220)
            chart = VerticalBarChart()
            chart.x = 40
            chart.y = 20
            chart.height = 170
            chart.width = 350
            data = [[float(v) for v in chart_df[value_col].tolist()]]
            chart.data = data
            chart.categoryAxis.categoryNames = [str(x)[:12] for x in chart_df[label_col].tolist()]
            chart.categoryAxis.labels.angle = 30
            chart.categoryAxis.labels.dy = -10
            chart.bars[0].fillColor = colors.HexColor("#c9a24d")
            drawing.add(chart)
            elements.append(Paragraph(f"رسم بياني: {value_col} حسب {label_col}", styles["Heading2"]))
            elements.append(drawing)
            elements.append(Spacer(1, 16))

    elements.append(Paragraph("عينة من البيانات", styles["Heading2"]))
    sample = df.head(25)
    table_data = [list(sample.columns)] + [[str(v) for v in row] for row in sample.values.tolist()]
    tbl = Table(table_data, repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#c9a24d")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f2e7")]),
            ]
        )
    )
    elements.append(tbl)
    doc.build(elements)
    return _attachment(out.getvalue(), "excel_report.pdf", "application/pdf")


# --------------------------------------------------------------------------- OCR
@router.post("/ocr")
async def ocr_extract(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    if not ocr_enabled():
        raise HTTPException(400, "خدمة استخراج النص غير مفعّلة حالياً")
    name = (file.filename or "").lower()
    content = await file.read()
    if not content:
        raise HTTPException(400, "ملف فارغ")

    if name.endswith(".pdf"):
        from pypdf import PdfReader

        try:
            reader = PdfReader(io.BytesIO(content))
        except Exception as e:
            raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")
        texts = []
        image_count = 0
        for page in reader.pages[:10]:
            try:
                for img in page.images:
                    if image_count >= 8:
                        break
                    image_count += 1
                    try:
                        txt = extract_text_from_image(img.data, "image/png")
                        if txt:
                            texts.append(txt)
                    except RuntimeError as e:
                        raise HTTPException(400, str(e))
            except HTTPException:
                raise
            except Exception:
                continue
            if image_count >= 8:
                break
        if not texts:
            raise HTTPException(400, "لم يتم العثور على صور نصية داخل ملف PDF لاستخراج النص منها")
        return {"text": "\\n\\n".join(texts)}

    mime_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png", "webp": "image/webp", "gif": "image/gif"}
    suffix = name.rsplit(".", 1)[-1] if "." in name else ""
    if suffix not in mime_map:
        raise HTTPException(400, "نوع الملف غير مدعوم لاستخراج النص (صور أو PDF فقط)")
    try:
        text = extract_text_from_image(content, mime_map[suffix])
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return {"text": text}


# =========================================================== أدوات سريعة
_CURRENCY_RATES_USD = {
    "USD": 1.0, "SAR": 3.75, "AED": 3.6725, "EGP": 49.0, "KWD": 0.307,
    "EUR": 0.92, "GBP": 0.78, "QAR": 3.64, "BHD": 0.376, "OMR": 0.385,
}


@router.post("/sku-generator")
async def sku_generator(
    request: Request,
    prefix: str = Form("SKU"),
    category: str = Form(""),
    start_number: int = Form(1),
    count: int = Form(10),
    digits: int = Form(5),
):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    n = max(1, min(int(count or 10), 500))
    dg = max(3, min(int(digits or 5), 10))
    start = max(0, int(start_number or 1))
    pre = (prefix or "SKU").strip().upper()[:12] or "SKU"
    cat = (category or "").strip().upper()[:8]
    codes = []
    for i in range(n):
        num = str(start + i).zfill(dg)
        code = f"{pre}-{cat}-{num}" if cat else f"{pre}-{num}"
        codes.append(code)
    return {"items": codes, "count": len(codes)}


@router.post("/item-number-generator")
async def item_number_generator(
    request: Request,
    prefix: str = Form("ITM"),
    start_number: int = Form(1),
    count: int = Form(10),
    digits: int = Form(6),
    use_date: bool = Form(False),
):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    n = max(1, min(int(count or 10), 500))
    dg = max(3, min(int(digits or 6), 10))
    start = max(0, int(start_number or 1))
    pre = (prefix or "ITM").strip().upper()[:12] or "ITM"
    date_part = ""
    if use_date:
        date_part = dt.datetime.utcnow().strftime("%Y%m%d") + "-"
    codes = [f"{pre}-{date_part}{str(start + i).zfill(dg)}" for i in range(n)]
    return {"items": codes, "count": len(codes)}


@router.post("/password-generator")
async def password_generator(
    request: Request,
    length: int = Form(16),
    count: int = Form(5),
    include_symbols: bool = Form(True),
    include_numbers: bool = Form(True),
    include_uppercase: bool = Form(True),
):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    import secrets as _secrets
    import string as _string

    ln = max(6, min(int(length or 16), 128))
    cnt = max(1, min(int(count or 5), 50))
    alphabet = _string.ascii_lowercase
    if include_uppercase:
        alphabet += _string.ascii_uppercase
    if include_numbers:
        alphabet += _string.digits
    if include_symbols:
        alphabet += "!@#$%^&*()-_=+"
    if not alphabet:
        raise HTTPException(400, "يجب اختيار نوع واحد على الأقل من الأحرف")
    passwords = ["".join(_secrets.choice(alphabet) for _ in range(ln)) for _ in range(cnt)]
    return {"items": passwords, "count": len(passwords)}


@router.post("/percentage-calculator")
async def percentage_calculator(
    request: Request,
    mode: str = Form("percent_of"),
    value1: float = Form(...),
    value2: float = Form(...),
):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    m = (mode or "percent_of").strip().lower()
    if m == "percent_of":
        result = (value1 / 100.0) * value2
        label = f"{value1}% من {value2} = {result:.2f}"
    elif m == "what_percent":
        if value2 == 0:
            raise HTTPException(400, "لا يمكن القسمة على صفر")
        result = (value1 / value2) * 100.0
        label = f"{value1} تمثل {result:.2f}% من {value2}"
    elif m == "change":
        if value1 == 0:
            raise HTTPException(400, "لا يمكن القسمة على صفر")
        result = ((value2 - value1) / value1) * 100.0
        direction = "زيادة" if result >= 0 else "نقصان"
        label = f"نسبة التغيير من {value1} إلى {value2}: {abs(result):.2f}% ({direction})"
    else:
        raise HTTPException(400, "نوع الحساب غير مدعوم")
    return {"result": round(result, 4), "label": label}


@router.post("/tax-calculator")
async def tax_calculator(
    request: Request,
    amount: float = Form(...),
    tax_rate: float = Form(15.0),
    mode: str = Form("add_tax"),
):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    rate = max(0.0, min(float(tax_rate or 15.0), 100.0))
    m = (mode or "add_tax").strip().lower()
    if m == "add_tax":
        tax_amount = amount * rate / 100.0
        total = amount + tax_amount
        label = f"المبلغ قبل الضريبة: {amount:.2f} | الضريبة ({rate}%): {tax_amount:.2f} | الإجمالي: {total:.2f}"
    elif m == "extract_tax":
        base = amount / (1 + rate / 100.0)
        tax_amount = amount - base
        total = amount
        label = f"المبلغ شامل الضريبة: {amount:.2f} | الأساسي: {base:.2f} | الضريبة ({rate}%): {tax_amount:.2f}"
    else:
        raise HTTPException(400, "نوع الحساب غير مدعوم")
    return {
        "base_amount": round(amount if m == "add_tax" else base, 2),
        "tax_amount": round(tax_amount, 2),
        "total": round(total, 2),
        "label": label,
    }


@router.post("/currency-converter")
async def currency_converter(
    request: Request,
    amount: float = Form(...),
    from_currency: str = Form("USD"),
    to_currency: str = Form("SAR"),
):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    fc = (from_currency or "USD").strip().upper()
    tc = (to_currency or "SAR").strip().upper()
    if fc not in _CURRENCY_RATES_USD or tc not in _CURRENCY_RATES_USD:
        raise HTTPException(400, f"العملة غير مدعومة. العملات المتاحة: {', '.join(_CURRENCY_RATES_USD.keys())}")
    usd_amount = amount / _CURRENCY_RATES_USD[fc]
    result = usd_amount * _CURRENCY_RATES_USD[tc]
    return {
        "result": round(result, 4),
        "label": f"{amount} {fc} = {result:.2f} {tc}",
        "disclaimer": "أسعار تقريبية وثابتة لأغراض الحساب السريع، وقد تختلف عن السعر اللحظي في السوق.",
    }


# ==================================================================== ملفات
@router.post("/pdf-extract-images")
async def pdf_extract_images(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not name.endswith(".pdf"):
        raise HTTPException(400, "الملف يجب أن يكون PDF")
    content = await file.read()
    from pypdf import PdfReader

    try:
        reader = PdfReader(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")

    zip_buf = io.BytesIO()
    count = 0
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for p_idx, page in enumerate(reader.pages, start=1):
            try:
                for img in page.images:
                    count += 1
                    ext = ""
                    try:
                        ext = "." + (img.name.split(".")[-1] if "." in img.name else "png")
                    except Exception:
                        ext = ".png"
                    zf.writestr(f"page{p_idx}_image{count}{ext}", img.data)
            except Exception:
                continue
    if count == 0:
        raise HTTPException(400, "لم يتم العثور على صور داخل ملف PDF")
    return _attachment(zip_buf.getvalue(), "extracted_images.zip", "application/zip")


@router.post("/word-pdf-convert")
async def word_pdf_convert(request: Request, file: UploadFile = File(...), direction: str = Form("word2pdf")):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    content = await file.read()
    dirn = (direction or "word2pdf").strip().lower()

    if dirn == "word2pdf":
        if not name.endswith(".docx"):
            raise HTTPException(400, "الملف يجب أن يكون Word بصيغة .docx")
        from docx import Document as DocxDoc
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import cm
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet

        try:
            doc = DocxDoc(io.BytesIO(content))
        except Exception as e:
            raise HTTPException(400, f"تعذر قراءة ملف Word: {str(e)}")
        styles = getSampleStyleSheet()
        out = io.BytesIO()
        pdf_doc = SimpleDocTemplate(out, pagesize=A4, topMargin=2 * cm, bottomMargin=2 * cm)
        elements = []
        for para in doc.paragraphs:
            txt = (para.text or "").strip()
            if not txt:
                elements.append(Spacer(1, 8))
                continue
            safe = txt.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            elements.append(Paragraph(safe, styles["Normal"]))
            elements.append(Spacer(1, 4))
        if not elements:
            raise HTTPException(400, "ملف Word لا يحتوي على نص قابل للتحويل")
        pdf_doc.build(elements)
        return _attachment(out.getvalue(), "converted.pdf", "application/pdf")

    elif dirn == "pdf2word":
        if not name.endswith(".pdf"):
            raise HTTPException(400, "الملف يجب أن يكون PDF")
        import pdfplumber
        from docx import Document as DocxDoc

        try:
            texts = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages[:200]:
                    txt = page.extract_text() or ""
                    if txt:
                        texts.append(txt)
        except Exception as e:
            raise HTTPException(400, f"تعذر قراءة ملف PDF: {str(e)}")
        if not texts:
            raise HTTPException(400, "لم يتم العثور على نص قابل للاستخراج في ملف PDF")
        docx_doc = DocxDoc()
        for page_text in texts:
            for line in page_text.split("\n"):
                docx_doc.add_paragraph(line)
            docx_doc.add_page_break()
        out = io.BytesIO()
        docx_doc.save(out)
        return _attachment(
            out.getvalue(), "converted.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )
    else:
        raise HTTPException(400, "اتجاه التحويل غير مدعوم")


# ==================================================================== Excel
def _read_tabular(name: str, content: bytes):
    import pandas as pd

    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(content))
    return pd.read_excel(io.BytesIO(content))


@router.post("/excel-dedup")
async def excel_dedup(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".csv")):
        raise HTTPException(400, "الملف يجب أن يكون Excel أو CSV")
    content = await file.read()
    import pandas as pd

    try:
        df = _read_tabular(name, content)
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")
    before = len(df)
    df2 = df.drop_duplicates()
    removed = before - len(df2)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df2.to_excel(writer, index=False, sheet_name="بدون تكرار")
    resp = _attachment(
        out.getvalue(), "deduped.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    resp.headers["X-Removed-Rows"] = str(removed)
    return resp


@router.post("/excel-merge")
async def excel_merge(request: Request, files: List[UploadFile] = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    if len(files) < 2:
        raise HTTPException(400, "الرجاء اختيار ملفين على الأقل للدمج")
    import pandas as pd

    frames = []
    for f in files:
        name = (f.filename or "").lower()
        if not (name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".csv")):
            raise HTTPException(400, f"الملف {f.filename} يجب أن يكون Excel أو CSV")
        content = await f.read()
        try:
            df = _read_tabular(name, content)
        except Exception as e:
            raise HTTPException(400, f"تعذر قراءة الملف {f.filename}: {str(e)}")
        df["__المصدر__"] = f.filename or "ملف"
        frames.append(df)
    merged = pd.concat(frames, ignore_index=True, sort=False)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        merged.to_excel(writer, index=False, sheet_name="مدمج")
    return _attachment(
        out.getvalue(), "merged.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@router.post("/excel-compare")
async def excel_compare(request: Request, file1: UploadFile = File(...), file2: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    import pandas as pd

    name1 = (file1.filename or "").lower()
    name2 = (file2.filename or "").lower()
    for nm, f in ((name1, file1), (name2, file2)):
        if not (nm.endswith(".xlsx") or nm.endswith(".xls") or nm.endswith(".csv")):
            raise HTTPException(400, f"الملف {f.filename} يجب أن يكون Excel أو CSV")
    c1 = await file1.read()
    c2 = await file2.read()
    try:
        df1 = _read_tabular(name1, c1)
        df2 = _read_tabular(name2, c2)
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة أحد الملفين: {str(e)}")

    common_cols = [c for c in df1.columns if c in df2.columns]
    if not common_cols:
        raise HTTPException(400, "لا توجد أعمدة مشتركة بين الملفين للمقارنة")
    d1 = df1[common_cols].astype(str)
    d2 = df2[common_cols].astype(str)
    merged = d1.merge(d2, how="outer", indicator=True)
    only_1 = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    only_2 = merged[merged["_merge"] == "right_only"].drop(columns=["_merge"])
    common = merged[merged["_merge"] == "both"].drop(columns=["_merge"])

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        only_1.to_excel(writer, index=False, sheet_name="فقط في الملف الأول")
        only_2.to_excel(writer, index=False, sheet_name="فقط في الملف الثاني")
        common.to_excel(writer, index=False, sheet_name="مشترك بين الملفين")
    resp = _attachment(
        out.getvalue(), "compare_result.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    resp.headers["X-Only-File1"] = str(len(only_1))
    resp.headers["X-Only-File2"] = str(len(only_2))
    resp.headers["X-Common"] = str(len(common))
    return resp


@router.post("/excel-detect-errors")
async def excel_detect_errors(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".csv")):
        raise HTTPException(400, "الملف يجب أن يكون Excel أو CSV")
    content = await file.read()
    import pandas as pd

    try:
        df = _read_tabular(name, content)
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")

    issues = []
    total_rows = len(df)
    dup_count = int(df.duplicated().sum())
    if dup_count:
        issues.append(f"يوجد {dup_count} صف مكرر بالكامل")

    empty_cells_by_col = {}
    for col in df.columns:
        n_empty = int(df[col].isna().sum())
        if n_empty:
            empty_cells_by_col[str(col)] = n_empty
    for col, n_empty in list(empty_cells_by_col.items())[:15]:
        pct = (n_empty / total_rows * 100) if total_rows else 0
        issues.append(f"العمود «{col}»: {n_empty} خلية فارغة ({pct:.0f}%)")

    negative_cols = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            n_neg = int((df[col] < 0).sum())
            if n_neg:
                negative_cols.append(f"العمود «{col}»: {n_neg} قيمة سالبة")
    issues.extend(negative_cols[:10])

    return {
        "total_rows": total_rows,
        "total_columns": len(df.columns),
        "duplicate_rows": dup_count,
        "issues": issues,
        "ok": len(issues) == 0,
    }


# ================================================================ أدوات AI
@router.post("/translate")
async def translate_text(request: Request, text: str = Form(...), target_language: str = Form("English")):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    txt = (text or "").strip()
    if not txt:
        raise HTTPException(400, "الرجاء إدخال نص للترجمة")
    if not ai_text_enabled():
        raise HTTPException(400, "خدمة الترجمة الذكية غير مفعّلة حالياً")
    lang = (target_language or "English").strip()
    system_prompt = (
        f"You are a professional translator. Translate the user's text into {lang}. "
        "Return ONLY the translated text, with no explanations, no quotes, and no markdown."
    )
    try:
        result = generate_text(system_prompt, txt)
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return {"result": result}


@router.post("/rephrase")
async def rephrase_text(request: Request, text: str = Form(...), style: str = Form("formal")):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    txt = (text or "").strip()
    if not txt:
        raise HTTPException(400, "الرجاء إدخال نص لإعادة الصياغة")
    if not ai_text_enabled():
        raise HTTPException(400, "خدمة إعادة الصياغة غير مفعّلة حالياً")
    style_map = {
        "formal": "أعد صياغة النص التالي بأسلوب رسمي واحترافي، بنفس اللغة الأصلية للنص.",
        "casual": "أعد صياغة النص التالي بأسلوب بسيط وودود، بنفس اللغة الأصلية للنص.",
        "concise": "أعد صياغة النص التالي بإيجاز شديد مع الحفاظ على المعنى، بنفس اللغة الأصلية للنص.",
        "expand": "أعد صياغة النص التالي وأضف تفاصيل توضيحية أكثر، بنفس اللغة الأصلية للنص.",
    }
    st = (style or "formal").strip().lower()
    instruction = style_map.get(st, style_map["formal"])
    system_prompt = f"{instruction} أعد النص المعاد صياغته فقط بدون أي شرح أو علامات اقتباس."
    try:
        result = generate_text(system_prompt, txt)
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return {"result": result}


@router.post("/email-writer")
async def email_writer(
    request: Request,
    topic: str = Form(...),
    tone: str = Form("formal"),
    recipient_context: str = Form(""),
):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    tp = (topic or "").strip()
    if not tp:
        raise HTTPException(400, "الرجاء إدخال موضوع الرسالة")
    if not ai_text_enabled():
        raise HTTPException(400, "خدمة إنشاء الرسائل غير مفعّلة حالياً")
    tn = (tone or "formal").strip().lower()
    tone_ar = {"formal": "رسمي", "friendly": "ودّي", "urgent": "عاجل ومباشر"}.get(tn, "رسمي")
    ctx = (recipient_context or "").strip()
    system_prompt = (
        f"اكتب رسالة بريد إلكتروني احترافية باللغة العربية بأسلوب {tone_ar}، "
        "تتضمن عنوانًا مناسبًا وتحية وخاتمة، حول الموضوع المذكور من المستخدم"
        + (f"، مع مراعاة السياق التالي عن المستلم: {ctx}" if ctx else "")
        + ". أعد نص الرسالة كاملاً فقط بدون أي شرح إضافي."
    )
    try:
        result = generate_text(system_prompt, tp)
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return {"result": result}


@router.post("/excel-ai-analyze")
async def excel_ai_analyze(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".csv")):
        raise HTTPException(400, "الملف يجب أن يكون Excel أو CSV")
    if not ai_text_enabled():
        raise HTTPException(400, "خدمة التحليل الذكي غير مفعّلة حالياً")
    content = await file.read()
    import pandas as pd

    try:
        df = _read_tabular(name, content)
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة الملف: {str(e)}")
    if df.empty:
        raise HTTPException(400, "الملف لا يحتوي على بيانات")

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    summary = {
        "rows": len(df),
        "columns": list(map(str, df.columns))[:30],
        "numeric_summary": {
            str(c): {
                "sum": float(df[c].sum()),
                "mean": float(df[c].mean()),
                "min": float(df[c].min()),
                "max": float(df[c].max()),
            }
            for c in numeric_cols[:10]
        },
        "sample_rows": json.loads(df.head(10).to_json(orient="records", force_ascii=False)),
    }
    system_prompt = (
        "أنت محلل بيانات مالي. لديك ملخص إحصائي وعينة بيانات من ملف Excel. "
        "اكتب تحليلاً عمليًا وموجزًا بالعربية (فقرة أو نقاط قصيرة) يبرز أهم الاتجاهات والانحرافات والملاحظات "
        "القابلة للتنفيذ، بأسلوب مباشر (مثال: مبيعات الفرع الرابع منخفضة 28%). لا تخترع أرقامًا غير موجودة في البيانات."
    )
    try:
        result = generate_text(system_prompt, json.dumps(summary, ensure_ascii=False)[:12000])
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return {"result": result}


@router.post("/pdf-summarize")
async def pdf_summarize(request: Request, file: UploadFile = File(...)):
    db = SessionLocal()
    try:
        _require_tool_access(db, request)
        require_csrf(request)
    finally:
        db.close()
    name = (file.filename or "").lower()
    if not name.endswith(".pdf"):
        raise HTTPException(400, "الملف يجب أن يكون PDF")
    if not ai_text_enabled():
        raise HTTPException(400, "خدمة التلخيص الذكي غير مفعّلة حالياً")
    content = await file.read()
    import pdfplumber

    try:
        parts = []
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages[:150]:
                txt = page.extract_text() or ""
                if txt:
                    parts.append(txt)
        full_text = "\n".join(parts)
    except Exception as e:
        raise HTTPException(400, f"تعذر قراءة ملف PDF: {str(e)}")
    if not full_text.strip():
        raise HTTPException(400, "لم يتم العثور على نص قابل للاستخراج في ملف PDF (قد يكون ممسوحًا ضوئيًا)")

    system_prompt = (
        "لخّص النص التالي المستخرج من ملف PDF في صفحة واحدة كحد أقصى بالعربية، "
        "مع الحفاظ على أهم النقاط والأرقام والاستنتاجات الرئيسية بشكل مركز وواضح."
    )
    try:
        result = generate_text(system_prompt, full_text, max_chars=40000)
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return {"result": result}
