from __future__ import annotations

import io
import logging
import re
import time
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger("uvicorn.error")

_CID_RE = re.compile(r"\(cid:\d+\)")
_REPLACEMENT_RE = re.compile("[�]")


class PdfExtractionError(Exception):
    """يُرفع فقط بعد فشل كل طرق الاستخراج: pdfplumber ثم PyMuPDF ثم OCR."""

    def __init__(self, user_message: str, technical_reason: str):
        super().__init__(technical_reason)
        self.user_message = user_message
        self.technical_reason = technical_reason


@dataclass
class PdfExtractionResult:
    pages: List[str]
    method: str  # "pdfplumber" | "pymupdf" | "ocr"
    duration_sec: float
    page_count: int
    note: Optional[str] = None
    fallback_used: bool = False

    @property
    def full_text(self) -> str:
        return "\n".join(self.pages)


def _cid_hits(text: str) -> int:
    return len(_CID_RE.findall(text or ""))


def _text_density_ok(pages: List[str]) -> bool:
    """كثافة نص معقولة تدل على وجود طبقة نص حقيقية (وليس ملفاً ممسوحاً ضوئياً بلا نص)."""
    if not pages:
        return False
    non_empty = sum(1 for p in pages if len((p or "").strip()) >= 20)
    return non_empty >= max(1, int(len(pages) * 0.5))


def _looks_garbled(text: str) -> bool:
    if not text or not text.strip():
        return True
    if _cid_hits(text) >= 3:
        return True
    total = len(text)
    repl = len(_REPLACEMENT_RE.findall(text))
    if total > 0 and (repl / total) > 0.02:
        return True
    return False


def _insufficiency_reason(pages: List[str], full: str) -> str:
    if not full.strip():
        return "نص فارغ"
    if _cid_hits(full) >= 3:
        return "ظهور (cid:NNNN) — الخط المضمّن بلا خريطة ToUnicode"
    if _looks_garbled(full):
        return "رموز بديلة/تالفة كثيرة في النص المستخرج"
    if not _text_density_ok(pages):
        return "كثافة نص منخفضة جداً (ملف ممسوح ضوئياً على الأرجح)"
    return "غير معروف"


def _extract_with_pdfplumber(content: bytes, max_pages: int) -> List[str]:
    import pdfplumber

    pages: List[str] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages[:max_pages]:
            pages.append(page.extract_text() or "")
    return pages


def _extract_with_pymupdf(content: bytes, max_pages: int) -> List[str]:
    import fitz  # PyMuPDF

    pages: List[str] = []
    doc = fitz.open(stream=content, filetype="pdf")
    try:
        for i, page in enumerate(doc):
            if i >= max_pages:
                break
            pages.append(page.get_text("text") or "")
    finally:
        doc.close()
    return pages


def _render_page_png(content: bytes, page_index: int, dpi: int = 200) -> Optional[bytes]:
    import fitz

    doc = fitz.open(stream=content, filetype="pdf")
    try:
        if page_index >= doc.page_count:
            return None
        page = doc[page_index]
        zoom = dpi / 72.0
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
        return pix.tobytes("png")
    finally:
        doc.close()


def _pdf_page_count(content: bytes) -> int:
    import fitz

    doc = fitz.open(stream=content, filetype="pdf")
    try:
        return doc.page_count
    finally:
        doc.close()


def _extract_with_ocr(content: bytes, max_pages: int, max_ocr_pages: int) -> List[str]:
    from .ocr_vision import extract_text_from_image, ocr_enabled

    if not ocr_enabled():
        raise RuntimeError("OCR غير متاح (مفتاح GEMINI_API_KEY غير موجود)")

    page_count = _pdf_page_count(content)
    total_pages = min(page_count, max_pages)
    ocr_pages = min(total_pages, max_ocr_pages)

    pages: List[str] = []
    failures = 0
    for i in range(ocr_pages):
        try:
            png = _render_page_png(content, i)
            if not png:
                pages.append("")
                continue
            txt = extract_text_from_image(png, mime="image/png")
            pages.append(txt or "")
        except Exception as e:
            failures += 1
            logger.warning("OCR فشل في الصفحة %s: %s", i + 1, e)
            pages.append("")

    if ocr_pages < total_pages:
        logger.info(
            "OCR: تمت معالجة %s من أصل %s صفحة فقط (حد أقصى %s صفحة للتحكم بالتكلفة/الوقت)",
            ocr_pages, total_pages, max_ocr_pages,
        )
    if ocr_pages > 0 and failures == ocr_pages:
        raise RuntimeError(f"فشل OCR في جميع الصفحات ({failures}/{ocr_pages})")
    return pages


def extract_pdf_text_with_fallback(
    content: bytes, max_pages: int = 200, max_ocr_pages: int = 30
) -> PdfExtractionResult:
    """
    يحاول استخراج نص PDF بالترتيب: pdfplumber ثم PyMuPDF ثم OCR (Gemini عبر ocr_vision).
    ينجح عند أول طريقة تُعطي نصاً كافياً وغير تالف؛ ويسجّل الطريقة الناجحة والمدة وعدد الصفحات.
    يرفع PdfExtractionError فقط إذا فشلت الطرق الثلاث جميعها.
    """
    t0 = time.monotonic()
    attempts: List[str] = []

    # 1) pdfplumber
    try:
        pages = _extract_with_pdfplumber(content, max_pages)
        full = "\n".join(pages)
        if full.strip() and not _looks_garbled(full) and _text_density_ok(pages):
            dur = time.monotonic() - t0
            logger.info(
                "استخراج PDF ناجح عبر pdfplumber | صفحات=%s | المدة=%.2fث", len(pages), dur
            )
            return PdfExtractionResult(pages=pages, method="pdfplumber", duration_sec=dur, page_count=len(pages))
        reason = _insufficiency_reason(pages, full)
        attempts.append(f"pdfplumber: غير كافٍ ({reason})")
        logger.warning("pdfplumber غير كافٍ (%s) — الانتقال إلى PyMuPDF", reason)
    except Exception as e:
        attempts.append(f"pdfplumber: استثناء ({e})")
        logger.warning("pdfplumber فشل بالكامل: %s — الانتقال إلى PyMuPDF", e)

    # 2) PyMuPDF
    try:
        pages = _extract_with_pymupdf(content, max_pages)
        full = "\n".join(pages)
        if full.strip() and not _looks_garbled(full) and _text_density_ok(pages):
            dur = time.monotonic() - t0
            logger.info(
                "استخراج PDF ناجح عبر PyMuPDF بعد فشل pdfplumber | صفحات=%s | المدة=%.2fث",
                len(pages), dur,
            )
            return PdfExtractionResult(
                pages=pages,
                method="pymupdf",
                duration_sec=dur,
                page_count=len(pages),
                note="تعذر استخراج النص مباشرة بالطريقة المعتادة، وتم استخدام محرك استخراج بديل بنجاح.",
                fallback_used=True,
            )
        reason = _insufficiency_reason(pages, full)
        attempts.append(f"PyMuPDF: غير كافٍ ({reason})")
        logger.warning("PyMuPDF أيضاً غير كافٍ (%s) — الانتقال إلى OCR", reason)
    except Exception as e:
        attempts.append(f"PyMuPDF: استثناء ({e})")
        logger.warning("PyMuPDF فشل بالكامل: %s — الانتقال إلى OCR", e)

    # 3) OCR
    try:
        pages = _extract_with_ocr(content, max_pages, max_ocr_pages)
        full = "\n".join(pages)
        if full.strip():
            dur = time.monotonic() - t0
            logger.info(
                "استخراج PDF ناجح عبر OCR بعد فشل pdfplumber و PyMuPDF | صفحات=%s | المدة=%.2fث",
                len(pages), dur,
            )
            return PdfExtractionResult(
                pages=pages,
                method="ocr",
                duration_sec=dur,
                page_count=len(pages),
                note="تعذر استخراج النص مباشرة، وتمت محاولة استخدام التعرف الضوئي على النصوص (OCR).",
                fallback_used=True,
            )
        attempts.append("OCR: نص فارغ من كل الصفحات")
        logger.error("OCR أعاد نصاً فارغاً من كل الصفحات")
    except Exception as e:
        attempts.append(f"OCR: استثناء ({e})")
        logger.error("OCR فشل بالكامل: %s", e)

    dur = time.monotonic() - t0
    technical_reason = " | ".join(attempts)
    logger.error("فشل استخراج نص PDF بكل الطرق الثلاث بعد %.2fث: %s", dur, technical_reason)
    raise PdfExtractionError(
        user_message=(
            "تعذّر استخراج النص من هذا الملف رغم تجربة عدة طرق (استخراج مباشر، محرك بديل، وتعرف ضوئي OCR). "
            "غالباً الخط المضمّن داخل ملف PDF الأصلي أو جودة المسح الضوئي تمنع أي طريقة من قراءته بشكل صحيح. "
            "الحل: أعد تصدير/طباعة الملف إلى PDF من البرنامج الأصلي الذي أنشأه، أو ارفع نسخة أوضح إن كانت ممسوحة ضوئياً."
        ),
        technical_reason=technical_reason,
    )
