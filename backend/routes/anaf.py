"""
ANAF sync routes — MongoDB backend.
"""
import asyncio
import logging
import re
from datetime import datetime

import aiohttp
from fastapi import APIRouter, BackgroundTasks, HTTPException

import mongo_db as mdb
import state
from constants import ANAF_API_URL, ANAF_BATCH_SIZE, ANAF_CHUNK_SIZE, \
    ANAF_RATE_LIMIT_SECONDS, ANAF_TIMEOUT_SECONDS, ANAF_RETRY_DELAYS, ANAF_PAUSE_AFTER_FAILS

router = APIRouter()
logger = logging.getLogger(__name__)


def _s(value) -> str:
    if not isinstance(value, str):
        return value
    return value.replace('\x00', '').replace('\u0000', '').strip() or None


def _parse_cui(cui_raw: str):
    if not cui_raw:
        return None
    c = cui_raw.strip().upper()
    if c.startswith("RO"):
        c = c[2:].strip()
    c = c.split(".")[0].strip()
    c = ''.join(ch for ch in c if ch.isdigit())
    try:
        val = int(c)
        return val if val > 0 else None
    except (ValueError, OverflowError):
        return None


@router.get("/anaf/diagnose-cui")
async def diagnose_cui_formats():
    total = await mdb.firme_col.count_documents({"cui": {"$ne": None, "$exists": True}})
    found = await mdb.firme_col.count_documents({"anaf_sync_status": "found"})
    not_found = await mdb.firme_col.count_documents({"anaf_sync_status": "not_found"})
    invalid = await mdb.firme_col.count_documents({"anaf_sync_status": "invalid_cui"})
    unsynced = await mdb.firme_col.count_documents({"anaf_sync_status": None, "cui": {"$ne": None}})
    samples = await mdb.firme_col.find(
        {}, {"_id": 0, "cui": 1, "anaf_sync_status": 1}
    ).limit(10).to_list(10)
    invalid_samples = await mdb.firme_col.find(
        {"anaf_sync_status": "invalid_cui"}, {"_id": 0, "cui": 1}
    ).limit(5).to_list(5)
    return {
        "counts": {
            "total_with_cui": total, "found": found, "not_found": not_found,
            "invalid_cui": invalid, "unsynced": unsynced
        },
        "cui_samples": samples,
        "invalid_cui_samples": [r["cui"] for r in invalid_samples],
        "parse_test": {r["cui"]: str(_parse_cui(r["cui"])) for r in invalid_samples}
    }


@router.post("/anaf/reset-invalid-cui")
async def reset_invalid_cui():
    result = await mdb.firme_col.update_many(
        {"anaf_sync_status": "invalid_cui"},
        {"$set": {"anaf_sync_status": None, "anaf_last_sync": None}}
    )
    return {"reset": result.modified_count}


@router.get("/anaf/sync-progress")
async def get_anaf_sync_progress():
    return state.anaf_sync_progress


@router.get("/anaf/stats")
async def get_anaf_stats():
    base = {"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}}
    total = await mdb.firme_col.count_documents(base)
    synced = await mdb.firme_col.count_documents({
        **base,
        "$or": [{"anaf_last_sync": {"$ne": None}}, {"anaf_sync_status": {"$ne": None}}]
    })
    fara_timestamp = await mdb.firme_col.count_documents({
        **base, "anaf_sync_status": {"$ne": None}, "anaf_last_sync": None
    })
    found = await mdb.firme_col.count_documents({"anaf_sync_status": "found"})
    not_found = await mdb.firme_col.count_documents({"anaf_sync_status": "not_found"})
    errors = await mdb.firme_col.count_documents({"anaf_sync_status": "error"})
    active = await mdb.firme_col.count_documents({
        "anaf_stare": {"$regex": "^INREGISTRAT"},
        "anaf_inactiv": {"$ne": True}
    })
    radiate = await mdb.firme_col.count_documents({"anaf_stare": {"$regex": "RADIERE", "$options": "i"}})
    platitori = await mdb.firme_col.count_documents({"anaf_platitor_tva": True})
    efactura = await mdb.firme_col.count_documents({"anaf_e_factura": True})
    return {
        "total_firme_cu_cui": total, "synced": synced, "not_synced": total - synced,
        "fara_timestamp": fara_timestamp, "found": found, "not_found": not_found,
        "errors": errors, "active": active, "radiate": radiate,
        "platitori_tva": platitori, "e_factura": efactura, "db_available": True
    }


@router.post("/anaf/sync")
async def start_anaf_sync(background_tasks: BackgroundTasks,
                           limit: int = None, only_unsynced: bool = True, judet: str = None):
    if not mdb.client:
        raise HTTPException(status_code=503, detail="MongoDB not available")
    if state.anaf_sync_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync already in progress")

    state.anaf_sync_progress.clear()
    state.anaf_sync_progress.update({
        "active": True, "total_firms": 0, "processed": 0,
        "found": 0, "not_found": 0, "errors": 0,
        "current_batch": 0, "total_batches": 0,
        "last_update": datetime.utcnow().isoformat(), "eta_seconds": None, "logs": []
    })
    background_tasks.add_task(run_anaf_sync, limit, only_unsynced, judet)
    return {"message": "ANAF sync started", "status": "running"}


@router.post("/anaf/sync-stop")
async def stop_anaf_sync():
    state.anaf_sync_progress["active"] = False
    return {"message": "Sync stop requested"}


@router.post("/anaf/fix-timestamps")
async def fix_anaf_timestamps():
    now = datetime.utcnow()
    result = await mdb.firme_col.update_many(
        {"anaf_sync_status": {"$ne": None}, "anaf_last_sync": None},
        {"$set": {"anaf_last_sync": now}}
    )
    state.add_anaf_log(f"✓ Timestamp reparat pentru {result.modified_count:,} firme")
    return {"fixed": result.modified_count}


@router.post("/anaf/reset-sync-status")
async def reset_anaf_sync_status(judet: str = None):
    query = {"cui": {"$ne": None}}
    if judet:
        query["judet"] = {"$regex": judet, "$options": "i"}
    result = await mdb.firme_col.update_many(
        query, {"$set": {"anaf_last_sync": None, "anaf_sync_status": None}}
    )
    return {"reset": result.modified_count}


@router.get("/anaf/test/{cui}")
async def test_anaf_api(cui: str):
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        async with aiohttp.ClientSession() as session:
            async with session.post(ANAF_API_URL, json=[{"cui": int(cui), "data": today}],
                                    headers={"Content-Type": "application/json"},
                                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                data = await r.json()
                if data.get("found"):
                    state.add_anaf_log(f"🔍 Test CUI {cui}: Găsit!")
                else:
                    state.add_anaf_log(f"🔍 Test CUI {cui}: Negăsit în ANAF")
                return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anaf/test-full/{cui}")
async def test_anaf_api_full(cui: str):
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        async with aiohttp.ClientSession() as session:
            async with session.post(ANAF_API_URL, json=[{"cui": int(cui), "data": today}],
                                    headers={"Content-Type": "application/json"},
                                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                data = await r.json()
                analysis = {"cui": cui, "found": len(data.get("found", [])) > 0,
                            "raw_response": data, "sections_analysis": {}}
                if data.get("found"):
                    for section, values in data["found"][0].items():
                        if isinstance(values, dict):
                            analysis["sections_analysis"][section] = {
                                "total_fields": len(values),
                                "non_empty_fields": len({k: v for k, v in values.items() if v}),
                                "fields": {k: {"value": v, "has_data": bool(v)} for k, v in values.items()}
                            }
                return analysis
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Main sync function ───────────────────────────────────────────────────────

async def run_anaf_sync(limit: int, only_unsynced: bool, judet: str):
    state.anaf_sync_progress["logs"] = []
    state.add_anaf_log("Pornire sincronizare ANAF...")

    # Build query
    query = {"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}}
    if only_unsynced:
        query["$and"] = [{"anaf_last_sync": None}, {"anaf_sync_status": None}]
    if judet:
        query["judet"] = {"$regex": judet, "$options": "i"}

    # Count total
    total_available = await mdb.firme_col.count_documents(query)
    total = min(limit, total_available) if limit else total_available
    state.anaf_sync_progress["total_firms"] = total
    state.anaf_sync_progress["total_batches"] = (total + ANAF_BATCH_SIZE - 1) // ANAF_BATCH_SIZE
    state.add_anaf_log(f"Total firme: {total:,} (disponibile: {total_available:,})")

    start_time = datetime.utcnow()
    consecutive_failures = 0
    processed_total = 0
    last_id = 0
    chunk_num = 0

    while processed_total < total and state.anaf_sync_progress["active"]:
        # Load chunk of IDs
        chunk_query = {**query, "id": {"$gt": last_id}}
        fetch_limit = min(ANAF_CHUNK_SIZE, total - processed_total)
        chunk_docs = await mdb.firme_col.find(
            chunk_query, {"_id": 0, "id": 1, "cui": 1}
        ).sort("id", 1).limit(fetch_limit).to_list(fetch_limit)

        if not chunk_docs:
            break

        chunk_num += 1
        last_id = chunk_docs[-1]["id"]
        state.add_anaf_log(f"── Chunk {chunk_num}: {len(chunk_docs):,} ID-uri (last_id={last_id:,}) ──")

        # Process in batches of 100
        for batch_offset in range(0, len(chunk_docs), ANAF_BATCH_SIZE):
            if not state.anaf_sync_progress["active"]:
                state.add_anaf_log("Oprire solicitată.")
                break

            batch_slice = chunk_docs[batch_offset:batch_offset + ANAF_BATCH_SIZE]
            current_batch = state.anaf_sync_progress["current_batch"] + 1
            state.anaf_sync_progress["current_batch"] = current_batch

            today = datetime.utcnow().strftime("%Y-%m-%d")
            request_data = []
            cui_to_doc = {}

            for doc in batch_slice:
                cui_int = _parse_cui(doc.get("cui"))
                if cui_int:
                    request_data.append({"cui": cui_int, "data": today})
                    cui_to_doc[str(cui_int)] = doc

            if not request_data:
                # Mark all as invalid_cui
                ids = [d["id"] for d in batch_slice]
                await mdb.firme_col.update_many(
                    {"id": {"$in": ids}},
                    {"$set": {"anaf_sync_status": "invalid_cui"}}
                )
                state.anaf_sync_progress["errors"] += len(batch_slice)
                state.anaf_sync_progress["processed"] += len(batch_slice)
                processed_total += len(batch_slice)
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
                                bulk_ops = []
                                from pymongo import UpdateOne

                                for cui_key, doc in cui_to_doc.items():
                                    if cui_key in found_map:
                                        item = found_map[cui_key]
                                        dg = item.get("date_generale", {})
                                        tva = item.get("inregistrare_scop_Tva", {})
                                        rtvai = item.get("inregistrare_RTVAI", {})
                                        inactiv = item.get("stare_inactiv", {})
                                        split = item.get("inregistrare_SplitTVA", {})
                                        sediu = item.get("adresa_sediu_social", {})
                                        df = item.get("adresa_domiciliu_fiscal", {})
                                        update_fields = {
                                            "anaf_denumire": _s(dg.get("denumire")),
                                            "anaf_adresa": _s(dg.get("adresa")),
                                            "anaf_nr_reg_com": _s(dg.get("nrRegCom")),
                                            "anaf_telefon": _s(dg.get("telefon")),
                                            "anaf_fax": _s(dg.get("fax")),
                                            "anaf_stare": _s(dg.get("stare_inregistrare")),
                                            "anaf_data_inregistrare": _s(dg.get("data_inregistrare")),
                                            "anaf_cod_caen": _s(dg.get("cod_CAEN")),
                                            "anaf_forma_juridica": _s(dg.get("forma_juridica")),
                                            "anaf_forma_organizare": _s(dg.get("forma_organizare")),
                                            "anaf_forma_proprietate": _s(dg.get("forma_de_proprietate")),
                                            "anaf_organ_fiscal": _s(dg.get("organFiscalCompetent")),
                                            "anaf_platitor_tva": tva.get("scpTVA", False),
                                            "anaf_tva_incasare": rtvai.get("statusTvaIncasare", False),
                                            "anaf_split_tva": split.get("statusSplitTVA", False),
                                            "anaf_inactiv": inactiv.get("statusInactivi", False),
                                            "anaf_e_factura": dg.get("statusRO_e_Factura", False),
                                            "anaf_sediu_judet": _s(sediu.get("sdenumire_Judet")),
                                            "anaf_sediu_localitate": _s(sediu.get("sdenumire_Localitate")),
                                            "anaf_sediu_strada": _s(sediu.get("sdenumire_Strada")),
                                            "anaf_sediu_numar": _s(sediu.get("snumar_Strada")),
                                            "anaf_sediu_cod_postal": _s(sediu.get("scod_Postal")),
                                            "anaf_iban": _s(dg.get("iban")),
                                            "anaf_data_efactura": _s(dg.get("data_inreg_Reg_RO_e_Factura")),
                                            "anaf_data_inactivare": _s(inactiv.get("dataInactivare")),
                                            "anaf_data_reactivare": _s(inactiv.get("dataReactivare")),
                                            "anaf_data_radiere": _s(inactiv.get("dataRadiere")),
                                            "anaf_data_inceput_tva_inc": _s(rtvai.get("dataInceputTvaInc")),
                                            "anaf_data_sfarsit_tva_inc": _s(rtvai.get("dataSfarsitTvaInc")),
                                            "anaf_df_judet": _s(df.get("ddenumire_Judet")),
                                            "anaf_df_localitate": _s(df.get("ddenumire_Localitate")),
                                            "anaf_df_strada": _s(df.get("ddenumire_Strada")),
                                            "anaf_df_numar": _s(df.get("dnumar_Strada")),
                                            "anaf_df_cod_postal": _s(df.get("dcod_Postal")),
                                            "anaf_last_sync": now,
                                            "anaf_sync_status": "found",
                                            "updated_at": now
                                        }
                                        bulk_ops.append(UpdateOne({"id": doc["id"]}, {"$set": update_fields}))
                                        state.anaf_sync_progress["found"] += 1
                                    else:
                                        cui_int = _parse_cui(doc.get("cui"))
                                        if cui_int:
                                            bulk_ops.append(UpdateOne({"id": doc["id"]}, {"$set": {
                                                "anaf_last_sync": now, "anaf_sync_status": "not_found", "updated_at": now
                                            }}))
                                            state.anaf_sync_progress["not_found"] += 1
                                        else:
                                            bulk_ops.append(UpdateOne({"id": doc["id"]}, {"$set": {"anaf_sync_status": "invalid_cui"}}))
                                            state.anaf_sync_progress["errors"] += 1
                                    state.anaf_sync_progress["processed"] += 1

                                if bulk_ops:
                                    await mdb.firme_col.bulk_write(bulk_ops, ordered=False)
                                consecutive_failures = 0
                                success = True
                                break
                            elif response.status == 429:
                                wait = 60 * (attempt + 1)
                                state.add_anaf_log(f"⚠️ Rate limit (429), aștept {wait}s...")
                                await asyncio.sleep(wait)
                except asyncio.TimeoutError:
                    state.add_anaf_log(f"⏱ Batch {current_batch}: Timeout - attempt {attempt+1}/3")
                    try:
                        pass
                    except Exception:
                        pass
                except Exception as e:
                    state.add_anaf_log(f"⚠️ Batch {current_batch}: {str(e)[:80]} - attempt {attempt+1}/3")

            if not success:
                consecutive_failures += 1
                ids = [d["id"] for d in batch_slice]
                await mdb.firme_col.update_many(
                    {"id": {"$in": ids}},
                    {"$set": {"anaf_sync_status": "error"}}
                )
                state.anaf_sync_progress["errors"] += len(batch_slice)
                state.anaf_sync_progress["processed"] += len(batch_slice)
                state.add_anaf_log(f"✗ Batch {current_batch}: Eșuat (consecutive: {consecutive_failures})")
                if consecutive_failures >= 3:
                    state.add_anaf_log(f"⏸ Pauză {ANAF_PAUSE_AFTER_FAILS}s...")
                    await asyncio.sleep(ANAF_PAUSE_AFTER_FAILS)
                    consecutive_failures = 0

            # ETA
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            processed = state.anaf_sync_progress["processed"]
            if processed > 0 and elapsed > 0:
                rate = processed / elapsed
                state.anaf_sync_progress["eta_seconds"] = int((total - processed) / rate) if rate > 0 else None
            state.anaf_sync_progress["last_update"] = datetime.utcnow().isoformat()

            if current_batch % 10 == 0:
                state.add_anaf_log(
                    f"📊 Progres: {processed:,}/{total:,} | "
                    f"Găsite: {state.anaf_sync_progress['found']:,} | "
                    f"Erori: {state.anaf_sync_progress['errors']:,}"
                )

            await asyncio.sleep(ANAF_RATE_LIMIT_SECONDS)
            processed_total += len(batch_slice)

    state.add_anaf_log(
        f"✅ Sincronizare completă: {state.anaf_sync_progress['found']:,} găsite, "
        f"{state.anaf_sync_progress['not_found']:,} negăsite, "
        f"{state.anaf_sync_progress['errors']:,} erori"
    )
    state.anaf_sync_progress["active"] = False
