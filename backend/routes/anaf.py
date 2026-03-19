"""
ANAF sync routes: sync progress, stats, start/stop sync, test CUI endpoints.
"""
import asyncio
import logging
from datetime import datetime

import aiohttp
from fastapi import APIRouter, BackgroundTasks, HTTPException

import state
import database
from constants import ANAF_API_URL, ANAF_BATCH_SIZE, ANAF_RATE_LIMIT_SECONDS, \
    ANAF_TIMEOUT_SECONDS, ANAF_RETRY_DELAYS, ANAF_PAUSE_AFTER_FAILS

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/anaf/sync-progress")
async def get_anaf_sync_progress():
    return state.anaf_sync_progress


@router.get("/anaf/stats")
async def get_anaf_stats():
    if database.SessionLocal is None:
        return {
            "total_firme_cu_cui": 0, "synced": 0, "not_synced": 0,
            "found": 0, "not_found": 0, "errors": 0, "active": 0,
            "radiate": 0, "platitori_tva": 0, "e_factura": 0,
            "fara_timestamp": 0, "db_available": False
        }
    db = database.SessionLocal()
    try:
        from sqlalchemy import or_
        db.expire_all()
        Firma = database.Firma
        total = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '').count()

        # "Synced" = has anaf_last_sync OR has anaf_sync_status (for old syncs without timestamp)
        synced = db.query(Firma).filter(
            Firma.cui.isnot(None), Firma.cui != '',
            or_(Firma.anaf_last_sync.isnot(None), Firma.anaf_sync_status.isnot(None))
        ).count()

        # Firms with sync_status but NO timestamp (old syncs) — can be fixed
        fara_timestamp = db.query(Firma).filter(
            Firma.cui.isnot(None), Firma.cui != '',
            Firma.anaf_sync_status.isnot(None),
            Firma.anaf_last_sync.is_(None)
        ).count()

        found = db.query(Firma).filter(Firma.anaf_sync_status == 'found').count()
        not_found = db.query(Firma).filter(Firma.anaf_sync_status == 'not_found').count()
        errors = db.query(Firma).filter(Firma.anaf_sync_status == 'error').count()
        active = db.query(Firma).filter(
            Firma.anaf_stare.ilike('%ACTIV%'),
            ~Firma.anaf_stare.ilike('%INACTIV%'),
            ~Firma.anaf_stare.ilike('%RADIERE%')
        ).count()
        radiate = db.query(Firma).filter(Firma.anaf_stare.ilike('%RADIERE%')).count()
        platitori_tva = db.query(Firma).filter(Firma.anaf_platitor_tva == True).count()
        e_factura = db.query(Firma).filter(Firma.anaf_e_factura == True).count()
        return {
            "total_firme_cu_cui": total,
            "synced": synced,
            "not_synced": total - synced,
            "fara_timestamp": fara_timestamp,
            "found": found, "not_found": not_found, "errors": errors,
            "active": active, "radiate": radiate,
            "platitori_tva": platitori_tva, "e_factura": e_factura,
            "db_available": True
        }
    finally:
        db.close()


@router.post("/anaf/sync")
async def start_anaf_sync(
    background_tasks: BackgroundTasks,
    limit: int = None,
    only_unsynced: bool = True,
    judet: str = None
):
    if database.SessionLocal is None:
        raise HTTPException(status_code=503, detail="Database not available")
    if state.anaf_sync_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync already in progress")

    state.anaf_sync_progress.clear()
    state.anaf_sync_progress.update({
        "active": True, "total_firms": 0, "processed": 0,
        "found": 0, "not_found": 0, "errors": 0,
        "current_batch": 0, "total_batches": 0,
        "last_update": datetime.utcnow().isoformat(),
        "eta_seconds": None, "logs": []
    })
    background_tasks.add_task(run_anaf_sync, limit, only_unsynced, judet)
    return {"message": "ANAF sync started", "status": "running"}


async def run_anaf_sync(limit: int, only_unsynced: bool, judet: str):
    state.anaf_sync_progress["logs"] = []
    state.add_anaf_log("Pornire sincronizare ANAF...")

    Firma = database.Firma

    # ─── Collect IDs first (lightweight, no full objects in memory) ──────────
    # Use a short-lived session just to get IDs
    id_db = database.SessionLocal()
    try:
        query_start = datetime.utcnow()
        base_query = id_db.query(Firma.id).filter(Firma.cui.isnot(None), Firma.cui != '')
        if only_unsynced:
            from sqlalchemy import and_
            base_query = base_query.filter(
                and_(Firma.anaf_last_sync.is_(None), Firma.anaf_sync_status.is_(None))
            )
        if judet:
            base_query = base_query.filter(Firma.judet.ilike(f"%{judet}%"))

        # Get only IDs — memory-efficient, fast
        all_ids = [row[0] for row in base_query.order_by(Firma.id).all()]
        query_time = (datetime.utcnow() - query_start).total_seconds()
    finally:
        id_db.close()

    if limit:
        all_ids = all_ids[:limit]

    total = len(all_ids)
    state.add_anaf_log(f"IDs colectate în {query_time:.1f}s — {total:,} firme de sincronizat")
    state.anaf_sync_progress["total_firms"] = total
    state.anaf_sync_progress["total_batches"] = (total + ANAF_BATCH_SIZE - 1) // ANAF_BATCH_SIZE

    start_time = datetime.utcnow()
    consecutive_failures = 0   # track consecutive failed batches

    # ─── Process in batches — FRESH SESSION per batch ────────────────────────
    for batch_idx, offset in enumerate(range(0, total, ANAF_BATCH_SIZE)):
        if not state.anaf_sync_progress["active"]:
            state.add_anaf_log("Oprire solicitată.")
            break

        batch_ids = all_ids[offset:offset + ANAF_BATCH_SIZE]
        current_batch = batch_idx + 1
        state.anaf_sync_progress["current_batch"] = current_batch

        # Fresh session per batch — prevents identity map accumulation
        db = database.SessionLocal()
        try:
            batch = db.query(Firma).filter(Firma.id.in_(batch_ids)).all()
            if not batch:
                continue

            today = datetime.utcnow().strftime("%Y-%m-%d")
            request_data = [{"cui": int(f.cui), "data": today} for f in batch if f.cui and f.cui.isdigit()]
            if not request_data:
                continue

            success = False
            for attempt in range(3):
                try:
                    if attempt == 0:
                        state.add_anaf_log(f"Batch {current_batch}/{state.anaf_sync_progress['total_batches']}: Trimit {len(request_data)} CUI-uri...")
                    else:
                        retry_delay = ANAF_RETRY_DELAYS[attempt - 1]
                        state.add_anaf_log(f"Batch {current_batch}: Retry {attempt + 1}/3 (aștept {retry_delay}s)...")
                        await asyncio.sleep(retry_delay)

                    batch_start = datetime.utcnow()
                    async with aiohttp.ClientSession() as http_session:
                        async with http_session.post(
                            ANAF_API_URL, json=request_data,
                            headers={"Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=ANAF_TIMEOUT_SECONDS)
                        ) as response:
                            api_time = (datetime.utcnow() - batch_start).total_seconds()
                            if response.status == 200:
                                data = await response.json()
                                found_map = {
                                    str(item.get("date_generale", {}).get("cui", "")): item
                                    for item in data.get("found", [])
                                }
                                state.add_anaf_log(f"✓ Batch {current_batch}: {len(found_map)} găsite, {len(batch)-len(found_map)} negăsite ({api_time:.1f}s)")

                                now = datetime.utcnow()
                                for firma in batch:
                                    if firma.cui in found_map:
                                        item = found_map[firma.cui]
                                        dg = item.get("date_generale", {})
                                        tva = item.get("inregistrare_scop_Tva", {})
                                        rtvai = item.get("inregistrare_RTVAI", {})
                                        inactiv = item.get("stare_inactiv", {})
                                        split = item.get("inregistrare_SplitTVA", {})
                                        sediu = item.get("adresa_sediu_social", {})
                                        firma.anaf_denumire = dg.get("denumire")
                                        firma.anaf_adresa = dg.get("adresa")
                                        firma.anaf_nr_reg_com = dg.get("nrRegCom")
                                        firma.anaf_telefon = dg.get("telefon")
                                        firma.anaf_fax = dg.get("fax")
                                        firma.anaf_cod_postal = dg.get("codPostal")
                                        firma.anaf_stare = dg.get("stare_inregistrare")
                                        firma.anaf_data_inregistrare = dg.get("data_inregistrare")
                                        firma.anaf_cod_caen = dg.get("cod_CAEN")
                                        firma.anaf_forma_juridica = dg.get("forma_juridica")
                                        firma.anaf_forma_organizare = dg.get("forma_organizare")
                                        firma.anaf_forma_proprietate = dg.get("forma_de_proprietate")
                                        firma.anaf_organ_fiscal = dg.get("organFiscalCompetent")
                                        firma.anaf_platitor_tva = tva.get("scpTVA", False)
                                        firma.anaf_tva_incasare = rtvai.get("statusTvaIncasare", False)
                                        firma.anaf_split_tva = split.get("statusSplitTVA", False)
                                        firma.anaf_inactiv = inactiv.get("statusInactivi", False)
                                        firma.anaf_e_factura = dg.get("statusRO_e_Factura", False)
                                        firma.anaf_sediu_judet = sediu.get("sdenumire_Judet")
                                        firma.anaf_sediu_localitate = sediu.get("sdenumire_Localitate")
                                        firma.anaf_sediu_strada = sediu.get("sdenumire_Strada")
                                        firma.anaf_sediu_numar = sediu.get("snumar_Strada")
                                        firma.anaf_sediu_cod_postal = sediu.get("scod_Postal")
                                        firma.anaf_last_sync = now
                                        firma.anaf_sync_status = "found"
                                        state.anaf_sync_progress["found"] += 1
                                    else:
                                        firma.anaf_last_sync = now
                                        firma.anaf_sync_status = "not_found"
                                        state.anaf_sync_progress["not_found"] += 1
                                    state.anaf_sync_progress["processed"] += 1

                                db.commit()
                                consecutive_failures = 0  # reset on success
                                success = True
                                break
                            elif response.status == 429:
                                wait = 60 * (attempt + 1)
                                state.add_anaf_log(f"⚠️ Batch {current_batch}: Rate limit (429), aștept {wait}s...")
                                await asyncio.sleep(wait)
                            else:
                                state.add_anaf_log(f"✗ Batch {current_batch}: HTTP {response.status}")
                except asyncio.TimeoutError:
                    state.add_anaf_log(f"⏱ Batch {current_batch}: Timeout ({ANAF_TIMEOUT_SECONDS}s) - attempt {attempt+1}/3")
                except Exception as e:
                    error_msg = str(e)[:60] or "connection error"
                    state.add_anaf_log(f"⚠️ Batch {current_batch}: {error_msg} - attempt {attempt+1}/3")

            if not success:
                consecutive_failures += 1
                for firma in batch:
                    firma.anaf_sync_status = "error"
                    state.anaf_sync_progress["errors"] += 1
                    state.anaf_sync_progress["processed"] += 1
                db.commit()
                state.add_anaf_log(f"✗ Batch {current_batch}: Eșuat după 3 încercări (consecutive: {consecutive_failures})")

                # After 3 consecutive failures → pause 5 minutes
                if consecutive_failures >= 3:
                    state.add_anaf_log(f"⏸ {consecutive_failures} batch-uri consecutive eșuate — pauză {ANAF_PAUSE_AFTER_FAILS}s (ANAF suprasolicitat)")
                    await asyncio.sleep(ANAF_PAUSE_AFTER_FAILS)
                    consecutive_failures = 0

        except Exception as e:
            state.add_anaf_log(f"✗ Batch {current_batch}: Exception - {str(e)[:60]}")
            logger.error(f"[ANAF] Batch error: {e}")
            try:
                db.rollback()
            except Exception:
                pass
        finally:
            db.close()

        # Update ETA
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        processed = state.anaf_sync_progress["processed"]
        if processed > 0 and elapsed > 0:
            rate = processed / elapsed
            remaining = total - processed
            state.anaf_sync_progress["eta_seconds"] = int(remaining / rate) if rate > 0 else None
        state.anaf_sync_progress["last_update"] = datetime.utcnow().isoformat()

        if current_batch % 10 == 0:
            state.add_anaf_log(f"📊 Progres: {processed:,}/{total:,}")

        await asyncio.sleep(ANAF_RATE_LIMIT_SECONDS)

    state.add_anaf_log(
        f"✅ Sincronizare completă: {state.anaf_sync_progress['found']:,} găsite, "
        f"{state.anaf_sync_progress['not_found']:,} negăsite, "
        f"{state.anaf_sync_progress['errors']:,} erori"
    )
    state.anaf_sync_progress["active"] = False


@router.post("/anaf/sync-stop")
async def stop_anaf_sync():
    state.anaf_sync_progress["active"] = False
    return {"message": "Sync stop requested"}


@router.post("/anaf/fix-timestamps")
async def fix_anaf_timestamps():
    """
    Setează anaf_last_sync pentru firmele care au anaf_sync_status dar nu au timestamp.
    Folosit pentru sincronizări vechi care au salvat date ANAF fără timestamp.
    """
    db = database.SessionLocal()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    Firma = database.Firma
    try:
        now = datetime.utcnow()
        # Find firms with sync status but no timestamp
        result = db.query(Firma).filter(
            Firma.anaf_sync_status.isnot(None),
            Firma.anaf_last_sync.is_(None)
        ).update({Firma.anaf_last_sync: now}, synchronize_session=False)
        db.commit()
        state.add_anaf_log(f"✓ Timestamp reparat pentru {result:,} firme sincronizate anterior")
        return {
            "fixed": result,
            "message": f"Timestamp setat pentru {result:,} firme sincronizate anterior (fără timestamp)"
        }
    finally:
        db.close()


@router.post("/anaf/reset-sync-status")
async def reset_anaf_sync_status(judet: str = None):
    """
    Resetează anaf_last_sync și anaf_sync_status pentru re-sincronizare completă.
    Opțional: doar pentru un județ specific.
    """
    db = database.SessionLocal()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    Firma = database.Firma
    try:
        query = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '')
        if judet:
            query = query.filter(Firma.judet.ilike(f"%{judet}%"))
        count = query.count()
        query.update({
            Firma.anaf_last_sync: None,
            Firma.anaf_sync_status: None
        }, synchronize_session=False)
        db.commit()
        msg = f"Reset status sync pentru {count:,} firme" + (f" din județul {judet}" if judet else "")
        state.add_anaf_log(f"⚠️ {msg}")
        return {"reset": count, "message": msg}
    finally:
        db.close()


@router.get("/anaf/test/{cui}")
async def test_anaf_api(cui: str):
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                ANAF_API_URL, json=[{"cui": int(cui), "data": today}],
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                data = await response.json()
                if data.get("found"):
                    item = data["found"][0]
                    state.add_anaf_log(f"🔍 Test CUI {cui}: Găsit!")
                    state.add_anaf_log(f"   → Secțiuni: {list(item.keys())}")
                    for section, values in item.items():
                        if isinstance(values, dict):
                            non_empty = {k: v for k, v in values.items() if v}
                            state.add_anaf_log(f"   → {section}: {len(non_empty)} câmpuri cu date")
                else:
                    state.add_anaf_log(f"🔍 Test CUI {cui}: Negăsit în ANAF")
                return data
    except Exception as e:
        state.add_anaf_log(f"🔍 Test CUI {cui}: Eroare - {str(e)[:50]}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anaf/test-full/{cui}")
async def test_anaf_api_full(cui: str):
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                ANAF_API_URL, json=[{"cui": int(cui), "data": today}],
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                data = await response.json()
                analysis = {
                    "cui": cui,
                    "found": len(data.get("found", [])) > 0,
                    "raw_response": data,
                    "sections_analysis": {}
                }
                if data.get("found"):
                    for section, values in data["found"][0].items():
                        if isinstance(values, dict):
                            analysis["sections_analysis"][section] = {
                                "total_fields": len(values),
                                "non_empty_fields": len({k: v for k, v in values.items() if v}),
                                "fields": {k: {"value": v, "has_data": bool(v)} for k, v in values.items()}
                            }
                        else:
                            analysis["sections_analysis"][section] = {"value": values}
                return analysis
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
