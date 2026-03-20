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
from constants import ANAF_API_URL, ANAF_BATCH_SIZE, ANAF_CHUNK_SIZE, \
    ANAF_RATE_LIMIT_SECONDS, ANAF_TIMEOUT_SECONDS, ANAF_RETRY_DELAYS, ANAF_PAUSE_AFTER_FAILS

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/anaf/sync-progress")
async def get_anaf_sync_progress():
    return state.anaf_sync_progress


@router.get("/anaf/diagnose-cui")
async def diagnose_cui_formats():
    """Show sample CUI formats from DB to debug parsing issues."""
    db = database.SessionLocal()
    if db is None:
        raise HTTPException(status_code=503, detail="DB unavailable")
    Firma = database.Firma
    try:
        from sqlalchemy import func
        # Sample random CUIs
        sample = db.query(Firma.cui, Firma.anaf_sync_status).filter(
            Firma.cui.isnot(None), Firma.cui != ''
        ).limit(20).all()
        
        invalid = db.query(Firma.cui).filter(
            Firma.anaf_sync_status == 'invalid_cui'
        ).limit(10).all()
        
        counts = {
            'total_with_cui': db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '').count(),
            'found': db.query(Firma).filter(Firma.anaf_sync_status == 'found').count(),
            'not_found': db.query(Firma).filter(Firma.anaf_sync_status == 'not_found').count(),
            'invalid_cui': db.query(Firma).filter(Firma.anaf_sync_status == 'invalid_cui').count(),
            'unsynced': db.query(Firma).filter(
                Firma.anaf_sync_status.is_(None),
                Firma.anaf_last_sync.is_(None),
                Firma.cui.isnot(None), Firma.cui != ''
            ).count(),
        }
        
        return {
            "counts": counts,
            "cui_samples": [{"cui": r.cui, "status": r.anaf_sync_status} for r in sample],
            "invalid_cui_samples": [r.cui for r in invalid],
            "parse_test": {r.cui: str(_parse_cui(r.cui)) for r in invalid[:5]}
        }
    finally:
        db.close()


@router.post("/anaf/reset-invalid-cui")
async def reset_invalid_cui():
    """Reset invalid_cui status back to NULL so they can be re-tried after CUI format fix."""
    db = database.SessionLocal()
    if db is None:
        raise HTTPException(status_code=503, detail="DB unavailable")
    Firma = database.Firma
    try:
        count = db.query(Firma).filter(Firma.anaf_sync_status == 'invalid_cui').count()
        db.query(Firma).filter(Firma.anaf_sync_status == 'invalid_cui').update(
            {Firma.anaf_sync_status: None, Firma.anaf_last_sync: None},
            synchronize_session=False
        )
        db.commit()
        return {"reset": count, "message": f"Reset {count:,} firme cu CUI invalid → NULL"}
    finally:
        db.close()
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
    from sqlalchemy import and_

    # ─── Count total (lightweight) ────────────────────────────────────────────
    count_db = database.SessionLocal()
    try:
        count_q = count_db.query(Firma.id).filter(Firma.cui.isnot(None), Firma.cui != '')
        if only_unsynced:
            count_q = count_q.filter(and_(Firma.anaf_last_sync.is_(None), Firma.anaf_sync_status.is_(None)))
        if judet:
            count_q = count_q.filter(Firma.judet.ilike(f"%{judet}%"))
        total_available = count_q.count()
    finally:
        count_db.close()

    total = min(limit, total_available) if limit else total_available
    state.anaf_sync_progress["total_firms"] = total
    state.anaf_sync_progress["total_batches"] = (total + ANAF_BATCH_SIZE - 1) // ANAF_BATCH_SIZE
    state.add_anaf_log(f"Total firme de sincronizat: {total:,} (disponibile: {total_available:,})")

    start_time = datetime.utcnow()
    consecutive_failures = 0
    processed_total = 0
    last_id = 0
    chunk_num = 0

    # ─── Chunk loop: load 10K IDs at a time ──────────────────────────────────
    # Uses while True + break when no more chunks — doesn't stop early based on stale total
    while state.anaf_sync_progress["active"]:

        # Check limit
        if limit and processed_total >= limit:
            state.add_anaf_log(f"Limita de {limit:,} firme atinsă.")
            break

        # Load next CHUNK_SIZE IDs using keyset pagination (WHERE id > last_id)
        chunk_db = database.SessionLocal()
        try:
            chunk_q = chunk_db.query(Firma.id).filter(
                Firma.cui.isnot(None), Firma.cui != '', Firma.id > last_id
            )
            if only_unsynced:
                chunk_q = chunk_q.filter(and_(Firma.anaf_last_sync.is_(None), Firma.anaf_sync_status.is_(None)))
            if judet:
                chunk_q = chunk_q.filter(Firma.judet.ilike(f"%{judet}%"))

            fetch_limit = ANAF_CHUNK_SIZE
            if limit:
                fetch_limit = min(ANAF_CHUNK_SIZE, limit - processed_total)
            chunk_ids = [row[0] for row in chunk_q.order_by(Firma.id).limit(fetch_limit).all()]
        finally:
            chunk_db.close()

        if not chunk_ids:
            state.add_anaf_log("Nu mai sunt firme de sincronizat.")
            break

        chunk_num += 1
        last_id = chunk_ids[-1]
        state.add_anaf_log(f"── Chunk {chunk_num}: {len(chunk_ids):,} ID-uri (last_id={last_id:,}) ──")

        # ─── Process chunk in batches of 100 ──────────────────────────────────
        for batch_offset in range(0, len(chunk_ids), ANAF_BATCH_SIZE):
            if not state.anaf_sync_progress["active"]:
                state.add_anaf_log("Oprire solicitată.")
                break

            batch_ids = chunk_ids[batch_offset:batch_offset + ANAF_BATCH_SIZE]
            current_batch = state.anaf_sync_progress["current_batch"] + 1
            state.anaf_sync_progress["current_batch"] = current_batch

            # Fresh session per batch — prevents identity map accumulation
            db = database.SessionLocal()
            try:
                batch = db.query(Firma).filter(Firma.id.in_(batch_ids)).all()
                if not batch:
                    continue

                today = datetime.utcnow().strftime("%Y-%m-%d")

                def _parse_cui(cui_raw: str):
                    """Parse CUI regardless of format: '14918042', 'RO14918042', '14918042.0', ' 14918042 '"""
                    if not cui_raw:
                        return None
                    c = cui_raw.strip().upper()
                    if c.startswith("RO"):
                        c = c[2:].strip()
                    c = c.split(".")[0].strip()          # remove decimal part
                    c = ''.join(ch for ch in c if ch.isdigit())  # keep only digits
                    try:
                        val = int(c)
                        return val if val > 0 else None
                    except (ValueError, OverflowError):
                        return None

                request_data = []
                cui_map = {}  # cui_int_str -> firma (for matching response back)
                for f in batch:
                    cui_int = _parse_cui(f.cui)
                    if cui_int:
                        request_data.append({"cui": cui_int, "data": today})
                        cui_map[str(cui_int)] = f

                if not request_data:
                    # All CUIs in batch are invalid — mark as error and continue
                    for f in batch:
                        f.anaf_sync_status = "invalid_cui"
                        state.anaf_sync_progress["errors"] += 1
                        state.anaf_sync_progress["processed"] += 1
                    db.commit()
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
                                    state.add_anaf_log(f"✓ Batch {current_batch}: {len(found_map)} găsite, {len(request_data)-len(found_map)} negăsite ({api_time:.1f}s)")

                                    now = datetime.utcnow()
                                    for firma in batch:
                                        cui_int = _parse_cui(firma.cui)
                                        cui_key = str(cui_int) if cui_int else ""
                                        if cui_key in found_map:
                                            item = found_map[cui_key]
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
                                            # Extra fields
                                            firma.anaf_iban = dg.get("iban") or None
                                            firma.anaf_data_efactura = dg.get("data_inreg_Reg_RO_e_Factura") or None
                                            firma.anaf_data_inactivare = inactiv.get("dataInactivare") or None
                                            firma.anaf_data_reactivare = inactiv.get("dataReactivare") or None
                                            firma.anaf_data_radiere = inactiv.get("dataRadiere") or None
                                            firma.anaf_data_inceput_tva_inc = rtvai.get("dataInceputTvaInc") or None
                                            firma.anaf_data_sfarsit_tva_inc = rtvai.get("dataSfarsitTvaInc") or None
                                            firma.anaf_data_inceput_split_tva = split.get("dataInceputSplitTVA") or None
                                            df = item.get("adresa_domiciliu_fiscal", {})
                                            firma.anaf_df_judet = df.get("ddenumire_Judet") or None
                                            firma.anaf_df_localitate = df.get("ddenumire_Localitate") or None
                                            firma.anaf_df_strada = df.get("ddenumire_Strada") or None
                                            firma.anaf_df_numar = df.get("dnumar_Strada") or None
                                            firma.anaf_df_cod_postal = df.get("dcod_Postal") or None
                                            firma.anaf_last_sync = now
                                            firma.anaf_sync_status = "found"
                                            state.anaf_sync_progress["found"] += 1
                                        else:
                                            # Firm is in batch but cui not in found_map
                                            # (either invalid CUI or not found by ANAF)
                                            cui_int = _parse_cui(firma.cui)
                                            if cui_int:
                                                firma.anaf_last_sync = now
                                                firma.anaf_sync_status = "not_found"
                                                state.anaf_sync_progress["not_found"] += 1
                                            else:
                                                firma.anaf_sync_status = "invalid_cui"
                                                state.anaf_sync_progress["errors"] += 1
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

                    if consecutive_failures >= 3:
                        state.add_anaf_log(f"⏸ {consecutive_failures} eșuate consecutiv — pauză {ANAF_PAUSE_AFTER_FAILS}s")
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

            # ETA update
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            processed = state.anaf_sync_progress["processed"]
            if processed > 0 and elapsed > 0:
                rate = processed / elapsed
                remaining = total - processed
                state.anaf_sync_progress["eta_seconds"] = int(remaining / rate) if rate > 0 else None
            state.anaf_sync_progress["last_update"] = datetime.utcnow().isoformat()

            if current_batch % 10 == 0:
                state.add_anaf_log(
                    f"📊 Progres: {state.anaf_sync_progress['processed']:,} procesate | "
                    f"{state.anaf_sync_progress['found']:,} găsite | "
                    f"{state.anaf_sync_progress['errors']:,} CUI invalid"
                )

            await asyncio.sleep(ANAF_RATE_LIMIT_SECONDS)
            processed_total += len(batch_ids)

            # Update total dynamically (some firms may have been added)
            state.anaf_sync_progress["total_firms"] = max(total, processed_total)

        # ── End of batch loop ──────────────────────────────────────────────────

    # ── End of chunk loop ─────────────────────────────────────────────────────

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
