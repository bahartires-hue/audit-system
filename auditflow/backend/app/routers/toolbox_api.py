from __future__ import annotations

import io
import json
import zipfile
from typing import List
from urllib.parse import quote

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response

from ..auth_core import require_csrf, require_user, user_can_access_page_key
from ..db import SessionLocal
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
