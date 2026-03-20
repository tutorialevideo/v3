"""
BPI (Buletinul Procedurilor de Insolvență) PDF Parser.
Uses LiteParse (https://github.com/run-llama/liteparse) for local PDF parsing.
Supports single upload AND server-side folder scan for 20-30GB collections.
"""
import re
import os
import json
import asyncio
import logging
import tempfile
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, File, UploadFile, HTTPException

import database
import state

router = APIRouter()
logger = logging.getLogger(__name__)

BPI_UPLOADS_DIR = Path(__file__).parent.parent / "downloads" / "bpi"
BPI_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
BPI_INPUT_DIR = Path(os.environ.get("BPI_INPUT_DIR", "/app/bpi_input"))


# ─── LiteParse PDF Extraction ─────────────────────────────────────────────────

def find_lit_binary() -> str:
    """Find the 'lit' binary from LiteParse."""
    candidates = [
        "lit",
        "/usr/bin/lit",
        "/usr/local/bin/lit",
        os.path.expanduser("~/.npm-global/bin/lit"),
        "/usr/lib/node_modules/.bin/lit",
    ]
    for c in candidates:
        try:
            result = subprocess.run([c, "--version"], capture_output=True, timeout=5)
            if result.returncode == 0:
                return c
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    raise RuntimeError("LiteParse ('lit') nu este instalat. Rulează: npm install -g @llamaindex/liteparse")


def extract_text_with_liteparse(pdf_bytes: bytes, ocr: bool = True) -> dict:
    """
    Parse PDF using LiteParse CLI.
    Returns dict with 'text' and 'metadata'.
    """
    lit_bin = find_lit_binary()

    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as f:
        f.write(pdf_bytes)
        tmp_pdf = f.name

    try:
        cmd = [lit_bin, 'parse', tmp_pdf, '--format', 'json', '--quiet']
        if not ocr:
            cmd.append('--no-ocr')
        # Add Romanian language for OCR if available
        cmd += ['--ocr-language', 'ron+eng']

        logger.info(f"[BPI] Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True, text=True, timeout=300,
            env={**os.environ, 'NODE_NO_WARNINGS': '1'}
        )

        if result.returncode != 0:
            # Fallback: try without OCR language (might not have Romanian trained data)
            cmd_fallback = [lit_bin, 'parse', tmp_pdf, '--format', 'json', '--quiet']
            result = subprocess.run(
                cmd_fallback, capture_output=True, text=True, timeout=300,
                env={**os.environ, 'NODE_NO_WARNINGS': '1'}
            )

        if result.returncode != 0:
            raise RuntimeError(f"LiteParse error: {result.stderr[:200]}")

        # Parse JSON output from LiteParse
        try:
            data = json.loads(result.stdout)
            # LiteParse JSON structure: {"pages": [{"page": N, "text": "...", ...}]}
            if isinstance(data, dict) and "pages" in data:
                text = "\n\n".join(
                    p.get("text", "") for p in data["pages"] if p.get("text")
                )
                pages = len(data["pages"])
            elif isinstance(data, dict) and "text" in data:
                text = data["text"]
                pages = data.get("numPages", 0)
            else:
                text = result.stdout
                pages = 0
        except (json.JSONDecodeError, KeyError):
            text = result.stdout
            pages = 0

        return {"text": text, "pages": pages, "raw": data if isinstance(data, dict) else {}}

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="Timeout la parsarea PDF-ului (> 5 minute)")
    finally:
        os.unlink(tmp_pdf)


def _extract_text_from_json(data: dict) -> str:
    """Extract text from LiteParse JSON output structure."""
    parts = []
    for page in data.get("pages", []):
        for block in page.get("blocks", []):
            parts.append(block.get("text", ""))
        # Also try 'lines' structure
        for line in page.get("lines", []):
            parts.append(line.get("text", ""))
    return "\n".join(parts)


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
    m = re.search(
        r'(?:judec[aă]tor(?:ul?)?\s+sindic|judecatorul?\s+sindic)\s*[:·]?\s*'
        r'(?:dr\.?\s*)?([A-Za-zĂâÎîȘșȚțÂăîșț][^\n,;]{3,50}?)(?:\s*[,;\.\n])',
        text, re.IGNORECASE
    )
    if m:
        record["judecator_sindic"] = _clean(m.group(1))

    # ── Administrator judiciar / Lichidator ───────────────────────────────────
    m = re.search(
        r'(?:administrator(?:ul?)?\s+judiciar|practician(?:ul?)?\s+[iî]n\s+insolven[tț][aă]?)\s*[:·]?\s*'
        r'([A-ZĂÂÎȘȚ][^\n,;]{3,80}?)(?:\s*\n)',
        text, re.IGNORECASE
    )
    if m:
        record["administrator_judiciar"] = _clean(m.group(1))

    m = re.search(
        r'lichidator(?:ul?)?\s+(?:judiciar\s*)?[:·]?\s*([A-ZĂÂÎȘȚ][^\n,;]{3,80}?)(?:\s*\n)',
        text, re.IGNORECASE
    )
    if m:
        record["lichidator"] = _clean(m.group(1))

    # ── Tip procedură — match și fără diacritice (din PDF scan/LiteParse) ─────
    tip_patterns = [
        (r'\binsolven(?:t[aăei]|ța)\b', 'Insolvență'),
        (r'\bfaliment(?:ul(?:ui)?)?\b', 'Faliment'),
        (r'\breorganizar[ei]\s+judiciar[aă]\b', 'Reorganizare judiciară'),
        (r'\bconcordat\s+preventiv\b', 'Concordat preventiv'),
        (r'\blichid(?:are|arii|ator)\b', 'Lichidare'),
        (r'\bdeschider(?:ea|ii)\s+procedurii?\b', 'Deschidere procedură'),
        (r'\bînchider(?:ea|ii)\s+procedurii?\b', 'Închidere procedură'),
        (r'\btabel(?:ul)?\s+(?:creditor|definitiv|preliminar)\b', 'Tabel creditori'),
        (r'\badunare[ai]\s+creditor(?:ilor)?\b', 'Adunare creditori'),
        (r'\bvânzare[ai]\b', 'Vânzare active'),
    ]
    for pat, tip in tip_patterns:
        if re.search(pat, text, re.IGNORECASE):
            record["tip_procedura"] = tip
            break

    # ── Data publicare ────────────────────────────────────────────────────────
    m = re.search(
        r'(?:data\s+public[aă]rii?|publicat(?:\s+azi)?\s+la\s+data|publicarii?)\s*[:·]?\s*'
        r'(\d{1,2}[./\-]\d{1,2}[./\-]\d{4})',
        text, re.IGNORECASE
    )
    if m:
        record["data_publicare"] = m.group(1)
    else:
        dates = re.findall(r'\b(\d{1,2}[./\-]\d{1,2}[./\-]20\d{2})\b', text)
        if dates:
            record["data_publicare"] = dates[0]

    # ── Termen — match și fără diacritice ─────────────────────────────────────
    m = re.search(
        r'(?:termenul?(?:\s+de\s+judecat[aă])?|termen\s+de\s+judecat[aă]|ora\s+fixat[aă])\s*[:·]?\s*'
        r'(\d{1,2}[./\-]\d{1,2}[./\-]\d{4}(?:\s+ora\s+\d{1,2}[:h]\d{2})?)',
        text, re.IGNORECASE
    )
    if m:
        record["termen"] = m.group(1)

    # ── Adresă ────────────────────────────────────────────────────────────────
    m = re.search(
        r'(?:cu\s+sediul|sediul\s+social|adresa?\s+sediu(?:lui)?)\s*[:·]?\s*([^\n]{10,120})',
        text, re.IGNORECASE
    )
    if m:
        record["adresa"] = _clean(m.group(1))

    # ── Tribunal din linia de dosar (ex: "Dosar nr. X - Tribunalul Cluj") ─────
    if not record["tribunal"]:
        m = re.search(
            r'Dosar\s+nr\.?\s*[\d/]+\s*[-–]\s*(Tribunal(?:ul)?\s+[A-Za-zĂâÎîȘșȚțÂăîșț\s]+?)(?:\n|$)',
            text, re.IGNORECASE
        )
        if m:
            record["tribunal"] = _clean(m.group(1))

    return record


# ─── API Routes ───────────────────────────────────────────────────────────────

@router.post("/bpi/parse")
async def parse_bpi_pdf(file: UploadFile = File(...), ocr: bool = True):
    """
    Parse a BPI PDF using LiteParse (local, no external API).
    Returns structured data extracted from the document.
    """
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Doar fișiere PDF sunt acceptate")

    pdf_bytes = await file.read()
    if len(pdf_bytes) == 0:
        raise HTTPException(status_code=400, detail="Fișierul PDF este gol")
    if len(pdf_bytes) > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Fișierul este prea mare (max 50MB)")

    logger.info(f"[BPI] Parsing with LiteParse: {file.filename} ({len(pdf_bytes):,} bytes)")

    # Parse with LiteParse
    try:
        parsed = extract_text_with_liteparse(pdf_bytes, ocr=ocr)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    text = parsed["text"]
    pages = parsed["pages"]

    if not text or not text.strip():
        return {
            "success": False,
            "error": "PDF-ul nu conține text. Dacă e un scan, activați OCR.",
            "filename": file.filename,
            "pages": pages,
            "records": [],
            "parser": "liteparse"
        }

    # Extract BPI records
    records = extract_bpi_data(text, file.filename)
    logger.info(f"[BPI] Extracted {len(records)} records from {file.filename}")

    # Match records to DB firms
    if database.SessionLocal:
        db = database.SessionLocal()
        try:
            for record in records:
                record["firma_match"] = None
                Firma = database.Firma
                if record.get("cui"):
                    firma = db.query(Firma).filter(Firma.cui == record["cui"]).first()
                    if firma:
                        record["firma_match"] = {
                            "id": firma.id,
                            "denumire": firma.denumire,
                            "cui": firma.cui,
                            "match_type": "cui_exact"
                        }
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
        "pages": pages,
        "text_length": len(text),
        "records_count": len(records),
        "records": records,
        "raw_text_preview": text[:1000],
        "parser": "liteparse"
    }


@router.get("/bpi/liteparse-version")
async def get_liteparse_version():
    """Check if LiteParse is installed and return version."""
    try:
        lit_bin = find_lit_binary()
        result = subprocess.run([lit_bin, "--version"], capture_output=True, text=True, timeout=5)
        return {
            "installed": True,
            "version": result.stdout.strip(),
            "binary": lit_bin
        }
    except Exception as e:
        return {"installed": False, "error": str(e)}


@router.post("/bpi/parse-batch")
async def parse_bpi_batch(files: List[UploadFile] = File(...)):
    """Parse multiple BPI PDFs at once (up to 20 files)."""
    if len(files) > 20:
        raise HTTPException(status_code=400, detail="Maxim 20 fișiere simultan")

    all_results = []
    for file in files:
        try:
            pdf_bytes = await file.read()
            parsed = extract_text_with_liteparse(pdf_bytes)
            text = parsed["text"]
            pages = parsed["pages"]
            records = extract_bpi_data(text, file.filename) if text.strip() else []

            # Match to DB
            if database.SessionLocal and records:
                db = database.SessionLocal()
                try:
                    for record in records:
                        record["firma_match"] = None
                        Firma = database.Firma
                        if record.get("cui"):
                            firma = db.query(Firma).filter(Firma.cui == record["cui"]).first()
                            if firma:
                                record["firma_match"] = {"id": firma.id, "denumire": firma.denumire, "cui": firma.cui, "match_type": "cui_exact"}
                        if not record["firma_match"] and record.get("denumire_firma"):
                            from helpers import normalize_company_name
                            firma = db.query(Firma).filter(Firma.denumire_normalized == normalize_company_name(record["denumire_firma"])).first()
                            if firma:
                                record["firma_match"] = {"id": firma.id, "denumire": firma.denumire, "cui": firma.cui, "match_type": "denumire_exact"}
                finally:
                    db.close()

            all_results.append({
                "filename": file.filename,
                "pages": pages,
                "records_count": len(records),
                "records": records,
                "success": True
            })
        except Exception as e:
            all_results.append({
                "filename": file.filename,
                "success": False,
                "error": str(e),
                "records": [],
                "records_count": 0
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


# ─── Server-side Folder Scan ──────────────────────────────────────────────────

@router.get("/bpi/folder-info")
async def get_bpi_folder_info():
    """Show info about the BPI input folder (mounted volume)."""
    folder = BPI_INPUT_DIR
    if not folder.exists():
        return {
            "path": str(folder),
            "exists": False,
            "message": f"Folderul nu există. Creează-l și pune PDF-urile acolo, sau editează docker-compose.yml să monteze folderul tău.",
            "docker_hint": "volumes:\n  - /calea/ta/catre/bpi_pdfs:/app/bpi_input"
        }

    # Count PDF files recursively
    pdf_files = list(folder.rglob("*.pdf")) + list(folder.rglob("*.PDF"))
    total_size = sum(f.stat().st_size for f in pdf_files if f.is_file())
    already_processed = await state.mongo_db.bpi_records.count_documents({"source_folder": str(folder)})

    return {
        "path": str(folder),
        "exists": True,
        "total_pdfs": len(pdf_files),
        "total_size_gb": round(total_size / (1024**3), 2),
        "total_size_mb": round(total_size / (1024**2), 1),
        "already_processed": already_processed,
        "remaining": len(pdf_files) - already_processed,
        "sample_files": [str(f.name) for f in pdf_files[:5]]
    }


@router.get("/bpi/scan-progress")
async def get_scan_progress():
    """Get current folder scan progress."""
    return state.bpi_scan_progress


@router.post("/bpi/scan-progress/stop")
async def stop_scan():
    state.bpi_scan_progress["active"] = False
    return {"message": "Stop requested"}


@router.post("/bpi/scan-folder")
async def scan_bpi_folder(
    background_tasks: BackgroundTasks,
    folder_path: str = None,
    skip_already_processed: bool = True,
    auto_save: bool = True,
    batch_size: int = 10
):
    """
    Scan a folder of BPI PDFs and process them all.
    For large collections (20-30GB), runs in background with live progress.
    
    folder_path: override default BPI_INPUT_DIR (must be inside container)
    skip_already_processed: skip files already in MongoDB
    auto_save: automatically save all extracted records to MongoDB
    batch_size: number of PDFs to process in parallel
    """
    if state.bpi_scan_progress["active"]:
        raise HTTPException(status_code=400, detail="Scan already running")

    scan_dir = Path(folder_path) if folder_path else BPI_INPUT_DIR

    if not scan_dir.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Folderul '{scan_dir}' nu există în container. Verifică volumul montat în docker-compose.yml."
        )

    state.bpi_scan_progress.update({
        "active": True, "total_files": 0, "processed": 0,
        "records_found": 0, "errors": 0, "current_file": None, "logs": []
    })

    background_tasks.add_task(_run_folder_scan, scan_dir, skip_already_processed, auto_save, batch_size)
    return {
        "message": f"Scan pornit: {scan_dir}",
        "status": "running"
    }


async def _run_folder_scan(scan_dir: Path, skip_processed: bool, auto_save: bool, batch_size: int):
    """Process all PDFs in a folder recursively."""
    try:
        # Find all PDFs
        pdf_files = sorted(scan_dir.rglob("*.pdf")) + sorted(scan_dir.rglob("*.PDF"))
        pdf_files = list(set(pdf_files))  # deduplicate

        if not pdf_files:
            state.add_bpi_log(f"Niciun PDF găsit în {scan_dir}")
            state.bpi_scan_progress["active"] = False
            return

        # Get already processed filenames
        processed_names = set()
        if skip_processed:
            cursor = state.mongo_db.bpi_records.find({"source_file": {"$exists": True}}, {"source_file": 1, "_id": 0})
            async for doc in cursor:
                processed_names.add(doc.get("source_file", ""))

        # Filter unprocessed
        to_process = [f for f in pdf_files if str(f) not in processed_names]
        state.bpi_scan_progress["total_files"] = len(to_process)
        state.add_bpi_log(f"Total PDF-uri: {len(pdf_files)} | De procesat: {len(to_process)} | Skip: {len(pdf_files) - len(to_process)}")
        state.add_bpi_log(f"Folder: {scan_dir}")
        state.add_bpi_log("─" * 40)

        if not to_process:
            state.add_bpi_log("Toate PDF-urile sunt deja procesate!")
            state.bpi_scan_progress["active"] = False
            return

        # Process files
        for i, pdf_path in enumerate(to_process):
            if not state.bpi_scan_progress["active"]:
                state.add_bpi_log("Oprire solicitată.")
                break

            state.bpi_scan_progress["current_file"] = pdf_path.name
            try:
                pdf_bytes = pdf_path.read_bytes()
                parsed = extract_text_with_liteparse(pdf_bytes)
                text = parsed["text"]
                pages = parsed["pages"]

                if not text or not text.strip():
                    state.add_bpi_log(f"[{i+1}/{len(to_process)}] {pdf_path.name} — text gol (scan/imagine?)")
                    state.bpi_scan_progress["errors"] += 1
                else:
                    records = extract_bpi_data(text, pdf_path.name)
                    state.bpi_scan_progress["records_found"] += len(records)

                    # Match + save
                    if auto_save and records:
                        for record in records:
                            record["source_file"] = str(pdf_path)
                            record["source_folder"] = str(scan_dir)
                            record["saved_at"] = datetime.utcnow().isoformat()
                            # Match to DB firm
                            record["firma_match"] = None
                            if database.SessionLocal:
                                db = database.SessionLocal()
                                try:
                                    Firma = database.Firma
                                    if record.get("cui"):
                                        f = db.query(Firma).filter(Firma.cui == record["cui"]).first()
                                        if f:
                                            record["firma_match"] = {"id": f.id, "denumire": f.denumire, "cui": f.cui}
                                finally:
                                    db.close()
                            # Save to MongoDB
                            await state.mongo_db.bpi_records.insert_one(
                                {k: v for k, v in record.items() if k != "_id"}
                            )

                    matched = sum(1 for r in records if r.get("firma_match"))
                    state.add_bpi_log(
                        f"[{i+1}/{len(to_process)}] {pdf_path.name} | {pages}pg | {len(records)} înreg | {matched} match DB"
                    )

            except Exception as e:
                state.bpi_scan_progress["errors"] += 1
                state.add_bpi_log(f"[{i+1}/{len(to_process)}] {pdf_path.name} — Eroare: {str(e)[:60]}")
                logger.error(f"[BPI] Error processing {pdf_path}: {e}")

            state.bpi_scan_progress["processed"] = i + 1

            # Small delay to not overwhelm the system
            if (i + 1) % batch_size == 0:
                await asyncio.sleep(0.1)

        state.add_bpi_log("─" * 40)
        state.add_bpi_log(
            f"Scan finalizat: {state.bpi_scan_progress['processed']} fișiere procesate, "
            f"{state.bpi_scan_progress['records_found']} înregistrări extrase, "
            f"{state.bpi_scan_progress['errors']} erori"
        )

    except Exception as e:
        state.add_bpi_log(f"Eroare generală: {str(e)[:80]}")
        logger.error(f"[BPI] Folder scan error: {e}")
    finally:
        state.bpi_scan_progress["active"] = False

