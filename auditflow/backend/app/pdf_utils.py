"""
مولّد تقارير PDF احترافي من السيرفر — دعم كامل للعربي (تشكيل الحروف واتجاه RTL)،
شعار الشركة، اسم التقرير، تاريخ التوليد، اسم المستخدم، ترقيم صفحات تلقائي، A4.
"""
from __future__ import annotations

import datetime as dt
import io
import os as _os
from pathlib import Path as _Path
from urllib.parse import quote as _quote

import arabic_reshaper
from bidi.algorithm import get_display
from fastapi.responses import StreamingResponse
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

_ASSETS_DIR = _Path(__file__).resolve().parent / "assets" / "fonts"
_FONT_REGULAR = "NotoNaskh"
_FONT_BOLD = "NotoNaskh"  # نفس الخط الوحيد المتوفر حاليًا؛ الوزن الغامق يُحاكى بالتلوين/الحجم
_fonts_registered = False


def _ensure_fonts():
    global _fonts_registered
    if _fonts_registered:
        return
    regular_path = _ASSETS_DIR / "NotoNaskhArabic-Regular.ttf"
    if regular_path.exists():
        pdfmetrics.registerFont(TTFont(_FONT_REGULAR, str(regular_path)))
    _fonts_registered = True


def ar(text) -> str:
    """يشكّل النص العربي (اتصال الحروف) ثم يعيد ترتيبه بصريًا (RTL) للعرض الصحيح في PDF."""
    s = "" if text is None else str(text)
    if not s.strip():
        return s
    try:
        reshaped = arabic_reshaper.reshape(s)
        return get_display(reshaped)
    except Exception:
        return s


def _resolve_logo_path(logo_url: str | None) -> str | None:
    if not logo_url:
        default = _Path(__file__).resolve().parents[2] / "frontend" / "logo.png"
        return str(default) if default.exists() else None
    data_root = (_os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
    base_dir = _Path(__file__).resolve().parents[2]
    if logo_url.startswith("/uploads/"):
        upload_dir = (_Path(data_root) / "uploads") if data_root else (base_dir / "uploads")
        candidate = upload_dir / logo_url[len("/uploads/"):]
        if candidate.exists():
            return str(candidate)
        return None
    if logo_url.startswith("http://") or logo_url.startswith("https://"):
        return None  # لا نجلب صورًا خارجية أثناء توليد PDF لتفادي بطء/فشل الشبكة
    default = _Path(__file__).resolve().parents[2] / "frontend" / "logo.png"
    return str(default) if default.exists() else None


def generate_pdf_report(headers, rows, report_title, filename, company_name=None, logo_url=None, generated_by=None):
    """
    ينشئ تقرير PDF احترافي جاهز للتنزيل:
    - رأس فيه شعار الشركة (إن وجد) + اسم الشركة + اسم التقرير + تاريخ/وقت التوليد + اسم المستخدم
    - جدول بيانات كامل بدعم RTL وتشكيل عربي صحيح
    - ترقيم صفحات تلقائي + تقسيم صفحات تلقائي (pagination) لأي عدد صفوف
    - مقاس A4
    """
    _ensure_fonts()

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        rightMargin=1.3 * cm, leftMargin=1.3 * cm,
        topMargin=1.2 * cm, bottomMargin=1.6 * cm,
    )
    elements = []

    title_style = ParagraphStyle("title", fontName=_FONT_BOLD, fontSize=16, alignment=1, textColor=colors.HexColor("#1a1200"), spaceAfter=4)
    meta_style = ParagraphStyle("meta", fontName=_FONT_REGULAR, fontSize=9, alignment=1, textColor=colors.HexColor("#666666"), spaceAfter=2)
    company_style = ParagraphStyle("company", fontName=_FONT_BOLD, fontSize=13, alignment=1, textColor=colors.HexColor("#1a1200"), spaceAfter=2)

    logo_path = _resolve_logo_path(logo_url)
    if logo_path:
        try:
            img = Image(logo_path, width=2.2 * cm, height=2.2 * cm)
            img.hAlign = "CENTER"
            elements.append(img)
            elements.append(Spacer(1, 0.15 * cm))
        except Exception:
            pass

    if company_name:
        elements.append(Paragraph(ar(company_name), company_style))
    elements.append(Paragraph(ar(report_title), title_style))

    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    meta_line = f"تاريخ التوليد: {now_str}"
    if generated_by:
        meta_line += f"   |   المستخدم: {generated_by}"
    elements.append(Paragraph(ar(meta_line), meta_style))
    elements.append(Spacer(1, 0.4 * cm))

    # RTL: نعكس ترتيب الأعمدة بصريًا بحيث يبدأ العرض من اليمين مطابقًا لبقية النظام
    rev_headers = list(reversed(headers))
    table_data = [[ar(h) for h in rev_headers]]
    for row in rows:
        rev_row = list(reversed(row))
        table_data.append([ar(c) if isinstance(c, str) else ("" if c is None else str(c)) for c in rev_row])

    available_width = A4[0] - doc.leftMargin - doc.rightMargin
    col_width = available_width / max(1, len(headers))
    table = Table(table_data, colWidths=[col_width] * len(headers), repeatRows=1)
    table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), _FONT_REGULAR),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1200")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cccccc")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f5ef")]),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(table)

    def _footer(canvas, _doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#888888"))
        canvas.drawCentredString(A4[0] / 2, 0.9 * cm, f"{_doc.page}")
        canvas.restoreState()

    doc.build(elements, onFirstPage=_footer, onLaterPages=_footer)
    buf.seek(0)

    ascii_fallback = "report.pdf"
    disposition = f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{_quote(filename, safe='')}"
    return StreamingResponse(buf, media_type="application/pdf", headers={"Content-Disposition": disposition})
