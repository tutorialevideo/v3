"""
BPI (Buletinul Procedurilor de Insolvență) PDF Parser.
Extracts firm data from Romanian insolvency bulletin PDFs using PyMuPDF.
No external API — runs entirely locally.
"""
import re
import io
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

import database
import state

router = APIRouter()
logger = logging.getLogger(__name__)

BPI_UPLOADS_DIR = Path(__file__).parent.parent / "downloads" / "bpi"
BPI_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

# ─── BPI Text Extraction (PyMuPDF) ───────────────────────────────────────────

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract full text from PDF using PyMuPDF."""
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text("text"))
        doc.close()
        return "\n".join(text_parts)
    except ImportError:
        raise HTTPException(status_code=500, detail="PyMuPDF nu este instalat")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Eroare la citirea PDF: {str(e)}")


def extract_pdf_metadata(pdf_bytes: bytes) -> dict:
    """Extract page count and metadata."""
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        meta = {
            "pages": doc.page_count,
            "title": doc.metadata.get("title", ""),
            "author": doc.metadata.get("author", ""),
            "creation_date": doc.metadata.get("creationDate", ""),
        }
        doc.close()
        return meta
    except Exception:
        return {"pages": 0}


# ─── BPI Data Extraction (Regex patterns for Romanian BPI format) ─────────────

def _clean(s: str) -> str:
    return re.sub(r'\s+', ' ', s).strip() if s else ""


def extract_bpi_data(text: str, filename: str = "") -> List[dict]:
    """
    Extract structured data from BPI PDF text.
    BPI has multiple proceedings per file (one publication = many firms).
    Returns a list of extracted records.
    """
    records = []

    # Split by section markers — each BPI entry starts with a numbered section
    # Common BPI section patterns: "1.", "Nr. crt.", or "Dosar nr."
    sections = _split_into_sections(text)

    for section_text in sections:
        record = _extract_single_record(section_text)
        if record and (record.get("denumire_firma") or record.get("cui") or record.get("dosar")):
            records.append(record)

    # If no sections found, try to extract from the whole text
    if not records:
        record = _extract_single_record(text)
        if record and (record.get("denumire_firma") or record.get("cui")):
            records.append(record)

    return records


def _split_into_sections(text: str) -> List[str]:
    """Split BPI text into individual proceedings."""
    # Pattern: section headers in BPI
    patterns = [
        r'\n\s*\d+\.\s+(?=Debitor|Creditor|Dosar)',  # "1. Debitor..."
        r'\n\s*(?:Nr\.|Numărul)\s*\d+\s*\n',           # "Nr. 1\n"
        r'(?=Dosar\s+(?:nr\.?|număr)\s*[:·])',          # Start of Dosar
    ]

    # Try to split by a common pattern
    for pattern in patterns:
        parts = re.split(pattern, text, flags=re.IGNORECASE)
        if len(parts) > 1:
            return [p for p in parts if len(p.strip()) > 50]

    # Fallback: split by "Dosar nr."
    parts = re.split(r'(?=Dosar\s+nr\.?\s*[:·\-]?\s*\d)', text, flags=re.IGNORECASE)
    if len(parts) > 1:
        return [p for p in parts if len(p.strip()) > 50]

    return [text]  # Single section


def _extract_single_record(text: str) -> dict:
    """Extract structured data from a single BPI record text."""
    record = {
        "denumire_firma": None,
        "cui": None,
        "nr_reg_com": None,
        "adresa": None,
        "tribunal": None,
        "dosar": None,
        "judecator_sindic": None,
        "administrator_judiciar": None,
        "lichidator": None,
        "tip_procedura": None,
        "data_publicare": None,
        "termen": None,
        "creditori": [],
        "descriere": _clean(text[:500]) if text else None,
        "text_complet": _clean(text),
    }

    # ── Denumire firmă ────────────────────────────────────────────────────────
    # Patterns: "Debitoarea SC ... SRL", "debitor: ...", după "Debitor:"
    patterns_firma = [
        r'(?:Debitoar(?:ea|ul)|Debitor(?:ul)?)\s*[:·]?\s*([A-ZĂÂÎȘȚ][^\n,;\.]{3,80}(?:SRL|SA|SNC|SCS|RA|SCA|SPRL|SRB|LLC|LTD)\.?)',
        r'(?:Societat(?:ea|ii)|SOCIETATE)\s+COMERCIALA\s+([A-ZĂÂÎȘȚ][^\n]{3,60}(?:SRL|SA|SNC)\.?)',
        r'\b(SC\s+[A-ZĂÂÎȘȚ][^\n,;]{3,50}\s+(?:SRL|SA|SNC|SCS)(?:\s+PRIN\s+[^\n]{0,50})?)',
        r'([A-ZĂÂÎȘȚ][A-Z\s\-\.]{3,60}(?:SRL|SA|SNC|SCS|RA|SPRL)(?:\s+PRIN\s+[^\n]{0,30})?)',
    ]
    for pat in patterns_firma:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            name = _clean(m.group(1))
            if len(name) >= 5:
                record["denumire_firma"] = name
                break

    # ── CUI / CIF ─────────────────────────────────────────────────────────────
    patterns_cui = [
        r'(?:C\.?U\.?I\.?|C\.?I\.?F\.?|cod\s+unic|cod\s+fiscal)\s*[:·]?\s*(?:RO)?\s*(\d{6,10})',
        r'(?:J\d{2}/\d+/\d{4})\s*[,;]?\s*(?:RO)?(\d{6,10})',
        r'\bRO\s*(\d{6,10})\b',
        r'(?:nr\.?\s*(?:ORC|R\.?C\.?)\s*[:·]?\s*(?:J\d{2}/\d+/\d{4})\s*[,;]?\s*(?:CUI|CIF)?\s*[:·]?\s*(?:RO)?(\d{6,10}))',
    ]
    for pat in patterns_cui:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            cui_str = m.group(1) if m.lastindex else None
            if cui_str and 6 <= len(cui_str) <= 10:
                record["cui"] = cui_str
                break

    # ── Nr. Registrul Comerțului ───────────────────────────────────────────────
    m = re.search(r'(?:nr\.?\s*)?(?:J|F|C)\d{1,2}/\d+/\d{4}', text, re.IGNORECASE)
    if m:
        record["nr_reg_com"] = m.group(0)

    # ── Tribunal / Instanță ───────────────────────────────────────────────────
    m = re.search(r'(Tribunal(?:ul)?\s+[A-ZĂÂÎȘȚ][a-zA-ZăâîșțĂÂÎȘȚ\s]+?)(?:\s*[-,;\.–]|\s*dosar|\s*nr\.)', text, re.IGNORECASE)
    if m:
        record["tribunal"] = _clean(m.group(1))

    # ── Număr dosar ───────────────────────────────────────────────────────────
    m = re.search(r'(?:Dosar\s+(?:nr\.?|număr)\s*[:·]?\s*)(\d+/\d+/\d{4})', text, re.IGNORECASE)
    if m:
        record["dosar"] = m.group(1)
    else:
        # Generic dosar pattern
        m = re.search(r'\b(\d{1,5}/\d{2,4}/20\d{2})\b', text)
        if m:
            record["dosar"] = m.group(1)

    # ── Judecător sindic ──────────────────────────────────────────────────────
    m = re.search(r'(?:judecător?\s+sindic|judecătorul?\s+sindic)\s*[:·]?\s*([A-ZĂÂÎȘȚ][a-zA-ZăâîșțĂÂÎȘȚ\s\-\.]{5,50}?)(?:\s*[,;\.\n])', text, re.IGNORECASE)
    if m:
        record["judecator_sindic"] = _clean(m.group(1))

    # ── Administrator judiciar / Lichidator ───────────────────────────────────
    m = re.search(r'(?:administrator(?:ul)?\s+judiciar|practicianul?\s+în\s+insolvență)\s*[:·]?\s*([A-ZĂÂÎȘȚ][^\n,;]{5,80}?)(?:\s*[,;\.\n])', text, re.IGNORECASE)
    if m:
        record["administrator_judiciar"] = _clean(m.group(1))

    m = re.search(r'lichidatorul?\s+(?:judiciar\s*)?[:·]?\s*([A-ZĂÂÎȘȚ][^\n,;]{5,80}?)(?:\s*[,;\.\n])', text, re.IGNORECASE)
    if m:
        record["lichidator"] = _clean(m.group(1))

    # ── Tip procedură ─────────────────────────────────────────────────────────
    tip_patterns = [
        (r'\binsolvența?\b', 'Insolvență'),
        (r'\bfaliment(?:ul)?\b', 'Faliment'),
        (r'\breorganizare?\s+judiciar[ăa]\b', 'Reorganizare judiciară'),
        (r'\bconcordat\s+preventiv\b', 'Concordat preventiv'),
        (r'\blichidare\b', 'Lichidare'),
        (r'\bdeschiderea?\s+procedurii?\b', 'Deschidere procedură'),
        (r'\bînchiderea?\s+procedurii?\b', 'Închidere procedură'),
        (r'\btabel(?:ul)?\s+(?:creditor|definitiv|preliminar)\b', 'Tabel creditori'),
        (r'\badunarea?\s+creditor(?:ilor)?\b', 'Adunare creditori'),
    ]
    for pat, tip in tip_patterns:
        if re.search(pat, text, re.IGNORECASE):
            record["tip_procedura"] = tip
            break

    # ── Data publicare ────────────────────────────────────────────────────────
    m = re.search(r'(?:data\s+publicării?|publicat(?:\s+azi)?\s+la\s+data|publicare)\s*[:·]?\s*(\d{1,2}[./\-]\d{1,2}[./\-]\d{4})', text, re.IGNORECASE)
    if m:
        record["data_publicare"] = m.group(1)
    else:
        # Try to find any date
        dates = re.findall(r'\b(\d{1,2}[./\-]\d{1,2}[./\-]20\d{2})\b', text)
        if dates:
            record["data_publicare"] = dates[0]

    # ── Termen ────────────────────────────────────────────────────────────────
    m = re.search(r'(?:termenul?|termen\s+de\s+judecată|ora\s+fixată)\s*[:·]?\s*(\d{1,2}[./\-]\d{1,2}[./\-]\d{4}(?:\s+ora\s+\d{1,2}[:h]\d{2})?)', text, re.IGNORECASE)
    if m:
        record["termen"] = m.group(1)

    # ── Adresă ────────────────────────────────────────────────────────────────
    m = re.search(r'(?:cu\s+sediul|adresa\s+sediu(?:lui)?)\s*(?:social)?[^,\n]*,?\s*([A-ZĂÂÎȘȚ][^,\n]{10,100})', text, re.IGNORECASE)
    if m:
        record["adresa"] = _clean(m.group(1))

    return record


# ─── API Routes ───────────────────────────────────────────────────────────────

@router.post("/bpi/parse")
async def parse_bpi_pdf(file: UploadFile = File(...)):
    """
    Parse a BPI PDF file and extract firm data.
    Returns structured data extracted from the document.
    """
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Doar fișiere PDF sunt acceptate")

    pdf_bytes = await file.read()
    if len(pdf_bytes) == 0:
        raise HTTPException(status_code=400, detail="Fișierul PDF este gol")
    if len(pdf_bytes) > 50 * 1024 * 1024:  # 50MB max
        raise HTTPException(status_code=400, detail="Fișierul este prea mare (max 50MB)")

    logger.info(f"[BPI] Parsing PDF: {file.filename} ({len(pdf_bytes):,} bytes)")

    # Extract text
    text = extract_text_from_pdf(pdf_bytes)
    metadata = extract_pdf_metadata(pdf_bytes)

    if not text.strip():
        return {
            "success": False,
            "error": "PDF-ul nu conține text selectabil. Poate fi un scan (imagine). Încercați OCR.",
            "filename": file.filename,
            "pages": metadata.get("pages", 0),
            "records": []
        }

    # Extract BPI records
    records = extract_bpi_data(text, file.filename)
    logger.info(f"[BPI] Extracted {len(records)} records from {file.filename}")

    # Try to match each record to existing firms in DB
    if database.SessionLocal:
        db = database.SessionLocal()
        try:
            for record in records:
                record["firma_match"] = None
                Firma = database.Firma
                # Match by CUI first
                if record.get("cui"):
                    firma = db.query(Firma).filter(Firma.cui == record["cui"]).first()
                    if firma:
                        record["firma_match"] = {
                            "id": firma.id,
                            "denumire": firma.denumire,
                            "cui": firma.cui,
                            "match_type": "cui_exact"
                        }
                # Match by name if no CUI match
                if not record["firma_match"] and record.get("denumire_firma"):
                    from helpers import normalize_company_name
                    norm = normalize_company_name(record["denumire_firma"])
                    firma = db.query(Firma).filter(Firma.denumire_normalized == norm).first()
                    if firma:
                        record["firma_match"] = {
                            "id": firma.id,
                            "denumire": firma.denumire,
                            "cui": firma.cui,
                            "match_type": "denumire_exact"
                        }
        finally:
            db.close()

    return {
        "success": True,
        "filename": file.filename,
        "pages": metadata.get("pages", 0),
        "text_length": len(text),
        "records_count": len(records),
        "records": records,
        "raw_text_preview": text[:1000] if text else ""
    }


@router.post("/bpi/parse-batch")
async def parse_bpi_batch(files: List[UploadFile] = File(...)):
    """Parse multiple BPI PDFs at once."""
    if len(files) > 20:
        raise HTTPException(status_code=400, detail="Maxim 20 fișiere simultan")

    all_results = []
    for file in files:
        try:
            pdf_bytes = await file.read()
            text = extract_text_from_pdf(pdf_bytes)
            records = extract_bpi_data(text, file.filename)
            all_results.append({
                "filename": file.filename,
                "records_count": len(records),
                "records": records,
                "success": True
            })
        except Exception as e:
            all_results.append({
                "filename": file.filename,
                "success": False,
                "error": str(e),
                "records": []
            })

    total_records = sum(r["records_count"] for r in all_results if r["success"])
    return {
        "files_processed": len(files),
        "total_records": total_records,
        "results": all_results
    }


@router.post("/bpi/save-record")
async def save_bpi_record(record: dict):
    """
    Save a BPI record to MongoDB for history and optionally
    update the linked firm in PostgreSQL.
    """
    record["saved_at"] = datetime.utcnow().isoformat()
    await state.mongo_db.bpi_records.insert_one({
        k: v for k, v in record.items() if k != "_id"
    })
    return {"success": True, "message": "Record salvat în MongoDB"}


@router.get("/bpi/history")
async def get_bpi_history(limit: int = 50, skip: int = 0):
    """Get previously parsed BPI records."""
    records = await state.mongo_db.bpi_records.find(
        {}, {"_id": 0}
    ).sort("saved_at", -1).skip(skip).limit(limit).to_list(limit)
    total = await state.mongo_db.bpi_records.count_documents({})
    return {"total": total, "records": records}


@router.get("/bpi/stats")
async def get_bpi_stats():
    """Stats about parsed BPI records."""
    total = await state.mongo_db.bpi_records.count_documents({})
    by_tip = {}
    pipeline = [{"$group": {"_id": "$tip_procedura", "count": {"$sum": 1}}}]
    async for doc in state.mongo_db.bpi_records.aggregate(pipeline):
        by_tip[doc["_id"] or "Necunoscut"] = doc["count"]
    return {"total_records": total, "by_tip_procedura": by_tip}
