"""
Localitati routes: import judete/localitati reference data from GitHub,
normalize firma.judet and firma.localitate to canonical names.
"""
import re
import logging
import unicodedata
from datetime import datetime

import aiohttp
from fastapi import APIRouter, BackgroundTasks, HTTPException
from sqlalchemy import Column, Integer, String, BigInteger, Float, ForeignKey, Text
from sqlalchemy.orm import relationship

import database
import state

router = APIRouter()
logger = logging.getLogger(__name__)

LOCALITATI_SQL_URL = "https://raw.githubusercontent.com/romania/localitati/master/judete-orase.sql"

# ─── Extra SQLAlchemy models ──────────────────────────────────────────────────

class Judet(database.Base):
    __tablename__ = "judete"
    id = Column(Integer, primary_key=True)
    code = Column(String(4), unique=True, nullable=False, index=True)
    name = Column(String(64), nullable=False)
    name_normalized = Column(String(64), index=True)  # for fast lookup
    localitati = relationship("Localitate", back_populates="judet")

class Localitate(database.Base):
    __tablename__ = "localitati"
    id = Column(Integer, primary_key=True)
    county_id = Column(Integer, ForeignKey("judete.id"), nullable=False, index=True)
    siruta = Column(BigInteger, nullable=False, index=True)
    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    name = Column(String(128), nullable=False)
    name_normalized = Column(String(128), index=True)
    region = Column(String(64), nullable=True)
    judet = relationship("Judet", back_populates="localitati")


def _normalize_str(s: str) -> str:
    """Normalize Romanian string: remove diacritics, uppercase, strip."""
    if not s:
        return ""
    # Normalize unicode (NFD) and remove combining characters (diacritics)
    nfd = unicodedata.normalize('NFD', s)
    ascii_str = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    return ascii_str.upper().strip()


def _clean_judet_input(raw: str) -> str:
    """Clean a raw judet string: remove 'JUD.', 'JUDETUL', 'MUN.', etc."""
    if not raw:
        return ""
    s = raw.upper().strip()
    # Remove common prefixes
    for prefix in ["JUDETUL ", "JUD. ", "JUD.", "MUN. ", "MUN.", "MUNICIPIUL ",
                   "ORȘ. ", "ORS. ", "ORAS ", "ORASUL "]:
        if s.startswith(prefix.upper()):
            s = s[len(prefix):].strip()
    return s


def _clean_localitate_input(raw: str) -> str:
    """Clean a raw localitate string."""
    if not raw:
        return ""
    s = raw.upper().strip()
    for prefix in ["MUN. ", "MUN.", "MUNICIPIUL ", "ORȘ. ", "ORS. ", "ORAS ", "ORASUL ",
                   "COM. ", "COMUNA ", "SAT ", "STR. ", "SECTOR "]:
        if s.startswith(prefix.upper()):
            s = s[len(prefix):].strip()
    # Remove trailing content after comma (e.g., "BACĂU, MUN." → "BACĂU")
    if ',' in s:
        s = s.split(',')[0].strip()
    return s


# ─── Import endpoints ─────────────────────────────────────────────────────────

@router.get("/localitati/stats")
async def get_localitati_stats():
    """Get statistics about imported localities data."""
    db = database.get_db_session()
    if db is None:
        return {"available": False}
    try:
        judete_count = db.query(Judet).count()
        localitati_count = db.query(Localitate).count()
        # Firms with matched locality
        Firma = database.Firma
        firme_cu_siruta = db.query(Firma).filter(Firma.siruta.isnot(None)).count()
        firme_cu_judet = db.query(Firma).filter(Firma.judet.isnot(None), Firma.judet != '').count()
        firme_total = db.query(Firma).count()
        return {
            "available": judete_count > 0,
            "judete_count": judete_count,
            "localitati_count": localitati_count,
            "firme_total": firme_total,
            "firme_cu_judet": firme_cu_judet,
            "firme_cu_siruta": firme_cu_siruta,
            "firme_fara_siruta": firme_total - firme_cu_siruta,
        }
    finally:
        db.close()


@router.post("/localitati/import")
async def import_localitati(background_tasks: BackgroundTasks):
    """Download and import judete + localitati from GitHub reference data."""
    db = database.get_db_session()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    # Check if tables exist and have data
    try:
        count = db.query(Judet).count()
        db.close()
        if count > 0:
            return {"message": f"Datele sunt deja importate ({count} județe). Folosiți force=true pentru re-import.", "already_imported": True}
    except Exception:
        db.close()

    background_tasks.add_task(_run_import_localitati)
    return {"message": "Import pornit în background. Verificați /api/localitati/stats pentru progres."}


@router.post("/localitati/import/force")
async def force_import_localitati(background_tasks: BackgroundTasks):
    """Force re-import of localities data (overwrites existing)."""
    db = database.get_db_session()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    db.close()
    background_tasks.add_task(_run_import_localitati, force=True)
    return {"message": "Re-import forțat pornit în background."}


async def _run_import_localitati(force: bool = False):
    """Download the SQL file and parse+insert judete and localitati."""
    logger.info("[LOCALITATI] Starting import from GitHub...")
    db = database.SessionLocal()
    try:
        # Create tables
        try:
            database.Base.metadata.create_all(bind=database.engine, tables=[Judet.__table__, Localitate.__table__])
        except Exception as e:
            logger.warning(f"[LOCALITATI] Table creation: {e}")

        if force:
            db.query(Localitate).delete()
            db.query(Judet).delete()
            db.commit()
            logger.info("[LOCALITATI] Cleared existing data")

        # Download SQL file
        async with aiohttp.ClientSession() as session:
            async with session.get(LOCALITATI_SQL_URL, timeout=aiohttp.ClientTimeout(total=30)) as response:
                sql_content = await response.text()

        # Parse judete INSERT
        county_map = {}  # id -> Judet object
        county_re = re.finditer(r"\((\d+),\s*'([A-Z]+)',\s*'([^']+)'\)", sql_content)
        judete_inserted = 0
        for m in county_re:
            cid, code, name = int(m.group(1)), m.group(2), m.group(3)
            j = Judet(id=cid, code=code, name=name, name_normalized=_normalize_str(name))
            db.merge(j)
            county_map[cid] = j
            judete_inserted += 1
        db.commit()
        logger.info(f"[LOCALITATI] Inserted {judete_inserted} judete")

        # Parse localitati INSERT — handle multi-row INSERT
        city_pattern = re.compile(
            r"\((\d+),\s*(\d+),\s*(\d+),\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']*)'\)"
        )
        localitati_inserted = 0
        batch = []
        for m in city_pattern.finditer(sql_content):
            cid, county_id, siruta = int(m.group(1)), int(m.group(2)), int(m.group(3))
            lng, lat = float(m.group(4)), float(m.group(5))
            name, region = m.group(6), m.group(7)
            batch.append(Localitate(
                id=cid, county_id=county_id, siruta=siruta,
                longitude=lng, latitude=lat,
                name=name, name_normalized=_normalize_str(name),
                region=region
            ))
            localitati_inserted += 1
            if len(batch) >= 1000:
                for obj in batch:
                    db.merge(obj)
                db.commit()
                batch = []
                logger.info(f"[LOCALITATI] Inserted {localitati_inserted} localitati...")

        if batch:
            for obj in batch:
                db.merge(obj)
            db.commit()

        logger.info(f"[LOCALITATI] Import complete: {judete_inserted} județe, {localitati_inserted} localități")

    except Exception as e:
        logger.error(f"[LOCALITATI] Import error: {e}")
        db.rollback()
    finally:
        db.close()


# ─── Normalization ────────────────────────────────────────────────────────────

normalize_progress = {"active": False, "processed": 0, "total": 0, "matched_judet": 0, "matched_localitate": 0}


@router.get("/localitati/normalize/progress")
async def get_normalize_progress():
    return normalize_progress


@router.post("/localitati/normalize")
async def normalize_firme_adrese(
    background_tasks: BackgroundTasks,
    only_unmatched: bool = True,
    judet_filter: str = None,
    limit: int = None
):
    """
    Normalize firma.judet and firma.localitate using the reference locality database.
    Also sets firma.siruta (SIRUTA code) for matched localities.
    """
    db = database.get_db_session()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    judete_count = db.query(Judet).count()
    db.close()
    if judete_count == 0:
        raise HTTPException(status_code=400, detail="Datele de localități nu sunt importate. Apelați /api/localitati/import mai întâi.")

    if normalize_progress["active"]:
        raise HTTPException(status_code=400, detail="Normalizare deja în progres")

    background_tasks.add_task(_run_normalize, only_unmatched, judet_filter, limit)
    return {"message": "Normalizare pornită în background"}


async def _run_normalize(only_unmatched: bool, judet_filter: str, limit: int):
    normalize_progress.update({"active": True, "processed": 0, "matched_judet": 0, "matched_localitate": 0})
    db = database.SessionLocal()
    Firma = database.Firma

    try:
        # Build cache: normalized_name -> Judet
        judete = db.query(Judet).all()
        judet_cache = {j.name_normalized: j for j in judete}
        # Also add code-based lookup (for when judet field has the code like "HR")
        judet_code_cache = {j.code.upper(): j for j in judete}

        # Build localitate cache: (county_id, normalized_name) -> Localitate
        localitati = db.query(Localitate).all()
        loc_cache = {}
        for loc in localitati:
            key = (loc.county_id, loc.name_normalized)
            loc_cache[key] = loc

        # Query firms to normalize
        query = db.query(Firma)
        if only_unmatched:
            query = query.filter(Firma.siruta.is_(None))
        if judet_filter:
            query = query.filter(Firma.judet.ilike(f"%{judet_filter}%"))
        if limit:
            query = query.limit(limit)

        total = query.count()
        normalize_progress["total"] = total
        logger.info(f"[LOCALITATI] Normalizing {total} firms...")

        offset = 0
        batch_size = 500
        matched_judet = 0
        matched_loc = 0

        while offset < total and normalize_progress["active"]:
            batch = query.offset(offset).limit(batch_size).all()
            if not batch:
                break

            for firma in batch:
                changed = False

                # 1. Match county
                raw_judet = firma.judet or firma.anaf_sediu_judet or ""
                cleaned_judet = _clean_judet_input(raw_judet)
                norm_judet = _normalize_str(cleaned_judet)

                matched_j = judet_cache.get(norm_judet)

                # Fallback: try code lookup (e.g. "HR" for Harghita)
                if not matched_j and len(norm_judet) <= 3:
                    matched_j = judet_code_cache.get(norm_judet)

                # Fallback: partial match (starts with)
                if not matched_j and norm_judet:
                    for jn, j in judet_cache.items():
                        if jn.startswith(norm_judet) or norm_judet.startswith(jn):
                            matched_j = j
                            break

                if matched_j:
                    if firma.judet != matched_j.name:
                        firma.judet = matched_j.name
                        changed = True
                    matched_judet += 1

                    # 2. Match locality (only if county matched)
                    raw_loc = firma.localitate or ""
                    cleaned_loc = _clean_localitate_input(raw_loc)
                    norm_loc = _normalize_str(cleaned_loc)

                    if norm_loc:
                        matched_l = loc_cache.get((matched_j.id, norm_loc))

                        # Fallback: search all localities in this county
                        if not matched_l:
                            for loc in localitati:
                                if loc.county_id == matched_j.id:
                                    if loc.name_normalized == norm_loc:
                                        matched_l = loc
                                        break
                                    # Partial: locality name starts with search term
                                    if (norm_loc and len(norm_loc) >= 4 and
                                            (loc.name_normalized.startswith(norm_loc) or
                                             norm_loc.startswith(loc.name_normalized))):
                                        matched_l = loc
                                        break

                        if matched_l:
                            if firma.localitate != matched_l.name:
                                firma.localitate = matched_l.name
                                changed = True
                            if firma.siruta != matched_l.siruta:
                                firma.siruta = matched_l.siruta
                                changed = True
                            matched_loc += 1

            db.commit()
            offset += batch_size
            normalize_progress.update({
                "processed": min(offset, total),
                "matched_judet": matched_judet,
                "matched_localitate": matched_loc
            })
            logger.info(f"[LOCALITATI] {offset}/{total}: {matched_judet} județe, {matched_loc} localități normalizate")

        logger.info(f"[LOCALITATI] Normalizare completă: {matched_judet} județe, {matched_loc} localități")

    except Exception as e:
        logger.error(f"[LOCALITATI] Normalize error: {e}")
        db.rollback()
    finally:
        db.close()
        normalize_progress["active"] = False


@router.get("/localitati/judete")
async def get_judete():
    """Get list of all counties."""
    db = database.get_db_session()
    if db is None:
        return []
    try:
        judete = db.query(Judet).order_by(Judet.name).all()
        return [{"id": j.id, "code": j.code, "name": j.name} for j in judete]
    finally:
        db.close()


@router.get("/localitati/search")
async def search_localitati(q: str, judet_id: int = None, limit: int = 20):
    """Search localities by name."""
    db = database.get_db_session()
    if db is None:
        return []
    try:
        query = db.query(Localitate).filter(Localitate.name.ilike(f"%{q}%"))
        if judet_id:
            query = query.filter(Localitate.county_id == judet_id)
        results = query.limit(limit).all()
        return [{"id": r.id, "name": r.name, "siruta": r.siruta,
                 "county_id": r.county_id, "region": r.region,
                 "lat": r.latitude, "lng": r.longitude} for r in results]
    finally:
        db.close()
