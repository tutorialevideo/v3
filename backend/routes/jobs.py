"""
Job management routes: config, run/stop/logs, files, stats, cron, institutions.
Also contains SOAP fetching logic and PostgreSQL save logic.
"""
import asyncio
import csv
import io
import json
import logging
from datetime import datetime, timezone

import aiohttp
from apscheduler.triggers.cron import CronTrigger
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, StreamingResponse

import state
from constants import (
    INSTITUTII, SOAP_URL, SOAP_ACTION_DOSARE2, DOWNLOADS_DIR
)
from helpers import build_soap_request, parse_soap_response, extract_companies_from_parti, parse_date, normalize_company_name
from schemas import JobConfig, JobConfigUpdate, JobRun, SearchRequest, CATEGORII_CAZ, CATEGORII_NUME

router = APIRouter()
logger = logging.getLogger(__name__)


# ─── SOAP service functions ───────────────────────────────────────────────────

async def fetch_dosare(session: aiohttp.ClientSession, nume_parte: str, institutie: str,
                       date_start: str = "", date_end: str = ""):
    soap_body = build_soap_request(nume_parte, institutie, date_start, date_end)
    headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': f'"{SOAP_ACTION_DOSARE2}"'}
    try:
        async with session.post(SOAP_URL, data=soap_body, headers=headers, timeout=60) as response:
            if response.status == 200:
                return parse_soap_response(await response.text())
    except Exception as e:
        logger.error(f"Error fetching from {institutie}: {e}")
    return []


def _build_firma_cache(db=None, Firma=None) -> dict:
    """Legacy stub — cache is built in save_to_mongo via MongoDB."""
    return {}


async def save_to_mongo(dosare: list, search_term: str,
                       categorie_caz: str = None,
                       only_match_existing: bool = False) -> dict:
    """Save dosare and firms to MongoDB."""
    import mongo_db as mdb
    from pymongo import UpdateOne
    stats = {
        'firme_new': 0, 'firme_existing': 0, 'firme_matched_anaf': 0,
        'dosare_new': 0, 'timeline_new': 0,
        'skipped_categorie': 0, 'skipped_no_match': 0
    }

    # Build match cache if only_match_existing
    norm_cache = {}
    if only_match_existing:
        async for doc in mdb.firme_col.find(
            {"cui": {"$ne": None}}, {"_id": 0, "id": 1, "denumire": 1, "denumire_normalized": 1, "cui": 1, "anaf_denumire": 1}
        ):
            if doc.get("denumire_normalized"):
                norm_cache[doc["denumire_normalized"]] = doc
            if doc.get("anaf_denumire"):
                norm_cache[normalize_company_name(doc["anaf_denumire"])] = doc
            # Also index by shorter names (first 2-3 words) for fuzzy match
            if doc.get("denumire_normalized"):
                words = doc["denumire_normalized"].split()
                if len(words) >= 2:
                    short_key = " ".join(words[:3])
                    if short_key not in norm_cache:
                        norm_cache[short_key] = doc

    from pymongo import InsertOne, UpdateOne
    bulk_dosare = []
    bulk_timeline = []

    for dosar_data in dosare:
        if categorie_caz and dosar_data.get('categorieCaz', '') != categorie_caz:
            stats['skipped_categorie'] += 1
            continue
        parti = dosar_data.get('parti', [])
        if not isinstance(parti, list):
            continue
        companies = extract_companies_from_parti(parti)
        if not companies:
            continue

        for company in companies:
            firma_doc = None
            if only_match_existing:
                norm = company['denumire_normalized']
                # Strategy 1: exact match
                firma_doc = norm_cache.get(norm)
                # Strategy 2: match by first 3 words (covers "FIRMA SRL" vs "FIRMA SRL PRIN ADMIN...")
                if not firma_doc and len(norm) >= 5:
                    words = norm.split()
                    if len(words) >= 2:
                        short_key = " ".join(words[:3])
                        firma_doc = norm_cache.get(short_key)
                # Strategy 3: prefix match (DB name starts with portal name or vice versa)
                if not firma_doc and len(norm) >= 8:
                    for k, v in norm_cache.items():
                        if len(k) >= 8 and (k.startswith(norm[:25]) or norm.startswith(k[:25])):
                            firma_doc = v; break
                # Strategy 4: contains match (portal name contains DB firm name)
                if not firma_doc and len(norm) >= 10:
                    for k, v in norm_cache.items():
                        if len(k) >= 6 and k in norm:
                            firma_doc = v; break
                if not firma_doc:
                    stats['skipped_no_match'] += 1; continue
                stats['firme_existing'] += 1
            else:
                firma_doc = await mdb.get_firma_by_denumire_norm(company['denumire_normalized'])
                if not firma_doc:
                    firma_doc = await mdb.upsert_firma_by_norm(company['denumire'], company['denumire_normalized'])
                    stats['firme_new'] += 1
                else:
                    stats['firme_existing'] += 1

            numar_dosar = dosar_data.get('numar', '')
            existing_dosar = await mdb.get_dosar_by_firma_and_numar(firma_doc['id'], numar_dosar)
            if existing_dosar:
                continue

            dosar_id = await mdb.next_id("dosare")
            dosar_doc = {
                "id": dosar_id, "firma_id": firma_doc['id'],
                "numar_dosar": numar_dosar,
                "institutie": dosar_data.get('institutie', ''),
                "obiect": dosar_data.get('obiect', ''),
                "data_dosar": parse_date(dosar_data.get('data', '')),
                "stadiu": dosar_data.get('stadiuProcesual', ''),
                "categorie": dosar_data.get('categorieCaz', ''),
                "materie": dosar_data.get('materie', ''),
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
            bulk_dosare.append(InsertOne(dosar_doc))
            stats['dosare_new'] += 1

            for sedinta in (dosar_data.get('sedinte', []) or []):
                t_id = await mdb.next_id("timeline")
                bulk_timeline.append(InsertOne({
                    "id": t_id, "dosar_id": dosar_id, "tip": "sedinta",
                    "data": parse_date(sedinta.get('data', '')),
                    "descriere": sedinta.get('solutie', '') or sedinta.get('complet', ''),
                    "detalii": sedinta, "created_at": datetime.now(timezone.utc)
                }))
                stats['timeline_new'] += 1

    if bulk_dosare:
        await mdb.dosare_col.bulk_write(bulk_dosare, ordered=False)
    if bulk_timeline:
        await mdb.timeline_col.bulk_write(bulk_timeline, ordered=False)

    return stats


# ─── Download job ─────────────────────────────────────────────────────────────

async def run_download_job(search_term: str, job_run_id: str,
                           date_start: str = "", date_end: str = "",
                           triggered_by: str = "manual", categorie_caz: str = None,
                           only_match_existing: bool = False):
    state.download_job_stop_flag = False
    label_info = search_term if search_term and search_term.strip() else f"{date_start or '*'} → {date_end or '*'}"
    cat_label = CATEGORII_NUME.get(categorie_caz, categorie_caz) if categorie_caz else "Toate categoriile"
    logger.info(f"Starting download job: {label_info} | categorie: {cat_label} | match_existing={only_match_existing}")

    state.download_job_progress["active"] = True
    state.download_job_progress.update({"processed": 0, "total": len(INSTITUTII), "dosare_found": 0, "firme_new": 0, "logs": []})
    state.add_download_log(f"Job pornit: {label_info}")
    state.add_download_log(f"Categorie: {cat_label}")
    if only_match_existing:
        state.add_download_log("Mod: Match local — doar firme existente în DB (fără firme noi)")
    state.add_download_log(f"Procesare {len(INSTITUTII)} instituții...")

    all_dosare = []
    processed = 0
    total_stats = {'firme_new': 0, 'firme_existing': 0, 'dosare_new': 0, 'timeline_new': 0, 'skipped_categorie': 0}

    async with aiohttp.ClientSession() as session:
        for institutie in INSTITUTII:
            if state.download_job_stop_flag:
                state.add_download_log("Oprire solicitată de utilizator.")
                break
            dosare = await fetch_dosare(session, search_term, institutie, date_start, date_end)
            if dosare:
                stats = await save_to_mongo(dosare, search_term, categorie_caz, only_match_existing)
                for key in total_stats:
                    total_stats[key] = total_stats.get(key, 0) + stats.get(key, 0)
                all_dosare.extend(d for d in dosare if not categorie_caz or d.get('categorieCaz') == categorie_caz)
                saved = stats['dosare_new']
                skipped_cat = stats.get('skipped_categorie', 0)
                skipped_match = stats.get('skipped_no_match', 0)
                if saved > 0 or skipped_match > 0:
                    msg = f"[{processed+1}/{len(INSTITUTII)}] {institutie}: {len(dosare)} total"
                    if saved > 0:
                        msg += f", {saved} salvate"
                    if skipped_cat > 0:
                        msg += f" ({skipped_cat} alte categorii)"
                    if skipped_match > 0:
                        msg += f" ({skipped_match} fără match)"
                    state.add_download_log(msg)
            processed += 1
            state.download_job_progress.update({"processed": processed, "dosare_found": len(all_dosare), "firme_new": total_stats['firme_new']})
            await state.mongo_db.job_runs.update_one(
                {"id": job_run_id},
                {"$set": {
                    "records_downloaded": len(all_dosare),
                    "firme_count": total_stats['firme_new'],
                    "dosare_count": total_stats['dosare_new'],
                    "timeline_count": total_stats['timeline_new'],
                    "progress_message": f"Procesare {processed}/{len(INSTITUTII)} instituții"
                }}
            )
            await asyncio.sleep(0.5)

    state.download_job_progress["active"] = False

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    label = search_term.replace(' ', '_') if search_term and search_term.strip() else f"{date_start or 'all'}_{date_end or 'all'}"
    filename = f"dosare_{label}_{timestamp}.json"
    with open(DOWNLOADS_DIR / filename, 'w', encoding='utf-8') as f:
        json.dump({"search_term": search_term, "date_start": date_start, "date_end": date_end,
                   "stats": total_stats, "total_records": len(all_dosare), "dosare": all_dosare},
                  f, ensure_ascii=False, indent=2)

    final_status = "stopped" if state.download_job_stop_flag else "completed"
    state.add_download_log(f"Job finalizat: {total_stats['dosare_new']} dosare noi, {total_stats['firme_new']} firme noi.")
    await state.mongo_db.job_runs.update_one(
        {"id": job_run_id},
        {"$set": {
            "status": final_status,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(all_dosare),
            "firme_count": total_stats['firme_new'],
            "dosare_count": total_stats['dosare_new'],
            "timeline_count": total_stats['timeline_new'],
            "files_created": [filename]
        }}
    )
    logger.info(f"Job {final_status}: {total_stats}")
    return total_stats


async def scheduled_job():
    config = await state.mongo_db.job_config.find_one({}, {"_id": 0})
    if not config or not config.get('cron_enabled'):
        return
    has_search = bool(config.get('search_term', '').strip())
    has_dates = bool(config.get('date_start') or config.get('date_end'))
    if not has_search and not has_dates:
        return
    if await state.mongo_db.job_runs.find_one({"status": "running"}, {"_id": 0}):
        return
    job_run = JobRun(triggered_by="cron")
    job_run_dict = job_run.model_dump()
    job_run_dict['started_at'] = job_run_dict['started_at'].isoformat()
    await state.mongo_db.job_runs.insert_one(job_run_dict)
    await run_download_job(config.get('search_term', ''), job_run.id,
                           config.get('date_start', ''), config.get('date_end', ''),
                           "cron", config.get('categorie_caz'))


def update_scheduler(hour: int, minute: int, enabled: bool):
    if state.scheduler.get_job('daily_download'):
        state.scheduler.remove_job('daily_download')
    if enabled:
        state.scheduler.add_job(scheduled_job, CronTrigger(hour=hour, minute=minute),
                                id='daily_download', replace_existing=True)


# ─── API Routes ───────────────────────────────────────────────────────────────

@router.get("/")
async def root():
    return {"message": "Portal JUST Downloader API - PostgreSQL Edition"}


@router.get("/config")
async def get_config():
    config = await state.mongo_db.job_config.find_one({}, {"_id": 0})
    if not config:
        default = JobConfig().model_dump()
        default['created_at'] = default['created_at'].isoformat()
        default['updated_at'] = default['updated_at'].isoformat()
        await state.mongo_db.job_config.insert_one(default)
        return default
    return config


@router.put("/config")
async def update_config(update: JobConfigUpdate):
    update_data = {k: v for k, v in update.model_dump().items() if v is not None}
    # Allow explicitly clearing categorie_caz by sending empty string
    if 'categorie_caz' in update.model_dump() and update.model_dump()['categorie_caz'] == '':
        update_data['categorie_caz'] = None
    update_data['updated_at'] = datetime.now(timezone.utc).isoformat()
    result = await state.mongo_db.job_config.find_one_and_update(
        {}, {"$set": update_data}, return_document=True, projection={"_id": 0}
    )
    if not result:
        config = JobConfig(**update_data)
        config_dict = config.model_dump()
        config_dict['created_at'] = config_dict['created_at'].isoformat()
        config_dict['updated_at'] = config_dict['updated_at'].isoformat()
        await state.mongo_db.job_config.insert_one(config_dict)
        result = config_dict
    if any(k in update_data for k in ['schedule_hour', 'schedule_minute', 'cron_enabled']):
        update_scheduler(result.get('schedule_hour', 2), result.get('schedule_minute', 0), result.get('cron_enabled', False))
    return result


@router.get("/categorii-caz")
async def get_categorii_caz():
    """List all available CategorieCaz values from portalquery.just.ro"""
    return [{"value": v, "label": CATEGORII_NUME.get(v, v)} for v in CATEGORII_CAZ]


@router.post("/run")
async def trigger_run(background_tasks: BackgroundTasks):
    config = await state.mongo_db.job_config.find_one({}, {"_id": 0})
    if not config:
        raise HTTPException(status_code=400, detail="No configuration. Save configuration first.")
    has_search = bool(config.get('search_term', '').strip())
    has_dates = bool(config.get('date_start') or config.get('date_end'))
    if not has_search and not has_dates:
        raise HTTPException(status_code=400, detail="Configurați cel puțin o perioadă sau un nume de firmă")
    if await state.mongo_db.job_runs.find_one({"status": "running"}, {"_id": 0}):
        raise HTTPException(status_code=409, detail="A job is already running")
    job_run = JobRun(triggered_by="manual")
    job_run_dict = job_run.model_dump()
    job_run_dict['started_at'] = job_run_dict['started_at'].isoformat()
    await state.mongo_db.job_runs.insert_one(job_run_dict)
    background_tasks.add_task(
        run_download_job,
        config.get('search_term', ''), job_run.id,
        config.get('date_start', ''), config.get('date_end', ''),
        "manual", config.get('categorie_caz'),
        bool(config.get('only_match_existing', False))
    )
    return {"message": "Job started", "job_id": job_run.id}


@router.post("/run/stop")
async def stop_download_job():
    state.download_job_stop_flag = True
    state.add_download_log("Cerere de oprire primită...")
    await state.mongo_db.job_runs.update_many(
        {"status": "running"},
        {"$set": {"status": "stopped", "finished_at": datetime.now(timezone.utc).isoformat()}}
    )
    state.download_job_progress["active"] = False
    return {"message": "Stop requested"}


@router.get("/run/logs")
async def get_download_logs():
    return {
        "active": state.download_job_progress["active"],
        "processed": state.download_job_progress["processed"],
        "total": state.download_job_progress["total"],
        "dosare_found": state.download_job_progress["dosare_found"],
        "firme_new": state.download_job_progress["firme_new"],
        "logs": state.download_job_progress["logs"][-50:]
    }


@router.post("/run/fix-stuck")
async def fix_stuck_jobs():
    result = await state.mongo_db.job_runs.update_many(
        {"status": "running"},
        {"$set": {"status": "failed", "finished_at": datetime.now(timezone.utc).isoformat(), "error_message": "Job marcat ca eșuat (orfan)"}}
    )
    state.download_job_progress["active"] = False
    return {"fixed": result.modified_count}


@router.get("/runs")
async def get_runs():
    return await state.mongo_db.job_runs.find({}, {"_id": 0}).sort("started_at", -1).to_list(50)


@router.get("/runs/current")
async def get_current_run():
    return await state.mongo_db.job_runs.find_one({"status": "running"}, {"_id": 0})


@router.post("/search")
async def search_dosare(request: SearchRequest):
    company = request.company_name or ""
    async with aiohttp.ClientSession() as session:
        if not request.institutie:
            all_dosare = []
            for inst in ["TribunalulBUCURESTI", "CurteadeApelBUCURESTI", "TribunalulCLUJ"]:
                dosare = await fetch_dosare(session, company, inst, request.date_start or "", request.date_end or "")
                all_dosare.extend(dosare)
                if len(all_dosare) >= 20:
                    break
            return {"total": len(all_dosare), "dosare": all_dosare[:20]}
        dosare = await fetch_dosare(session, company, request.institutie, request.date_start or "", request.date_end or "")
        return {"total": len(dosare), "dosare": dosare[:20]}


@router.get("/files")
async def list_files():
    files = []
    for f in DOWNLOADS_DIR.iterdir():
        if f.is_file() and f.suffix == '.json':
            stat = f.stat()
            files.append({"name": f.name, "size": stat.st_size,
                         "created": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()})
    return sorted(files, key=lambda x: x['created'], reverse=True)


@router.get("/files/{filename}")
async def download_file(filename: str):
    filepath = DOWNLOADS_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(filepath, filename=filename, media_type='application/json')


@router.delete("/files/{filename}")
async def delete_file(filename: str):
    filepath = DOWNLOADS_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="File not found")
    filepath.unlink()
    return {"message": "File deleted"}


@router.get("/stats")
async def get_stats():
    total_runs = await state.mongo_db.job_runs.count_documents({})
    completed_runs = await state.mongo_db.job_runs.count_documents({"status": "completed"})
    failed_runs = await state.mongo_db.job_runs.count_documents({"status": "failed"})
    config = await state.mongo_db.job_config.find_one({}, {"_id": 0})
    last_run = await state.mongo_db.job_runs.find_one({}, {"_id": 0}, sort=[("started_at", -1)])
    total_files = sum(1 for f in DOWNLOADS_DIR.iterdir() if f.is_file() and f.suffix == '.json')
    total_size = sum(f.stat().st_size for f in DOWNLOADS_DIR.iterdir() if f.is_file() and f.suffix == '.json')
    job = state.scheduler.get_job('daily_download')
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    firme_count = 0
    dosare_count = 0
    try:
        import mongo_db as mdb
        firme_count = await mdb.firme_col.count_documents({})
        dosare_count = await mdb.dosare_col.count_documents({})
    except Exception as e:
        logger.warning(f"Could not get MongoDB stats: {e}")
    return {
        "total_runs": total_runs, "completed_runs": completed_runs, "failed_runs": failed_runs,
        "total_files": total_files, "total_size_mb": round(total_size / (1024 * 1024), 2),
        "cron_enabled": config.get('cron_enabled', False) if config else False,
        "next_scheduled_run": next_run, "last_run": last_run,
        "db_firme": firme_count, "db_dosare": dosare_count
    }


@router.get("/cron/status")
async def get_cron_status():
    job = state.scheduler.get_job('daily_download')
    config = await state.mongo_db.job_config.find_one({}, {"_id": 0})
    return {
        "enabled": config.get('cron_enabled', False) if config else False,
        "schedule_hour": config.get('schedule_hour', 2) if config else 2,
        "schedule_minute": config.get('schedule_minute', 0) if config else 0,
        "next_run": job.next_run_time.isoformat() if job and job.next_run_time else None,
        "job_active": job is not None
    }


@router.get("/institutions")
async def get_institutions():
    return INSTITUTII


# ─── Sync Dosare per Firmă ────────────────────────────────────────────────────

@router.get("/sync-dosare/progress")
async def get_sync_dosare_progress():
    return state.sync_dosare_progress


@router.post("/sync-dosare/stop")
async def stop_sync_dosare():
    state.sync_dosare_progress["active"] = False
    return {"message": "Stop requested"}


@router.post("/sync-dosare/start")
async def start_sync_dosare(
    background_tasks: BackgroundTasks,
    limit: int = 100,
    date_start: str = None,
    date_end: str = None,
    categorie_caz: str = None,
    only_without_dosare: bool = False,
    judet: str = None
):
    """
    Sync dosare from Portal JUST for each ANAF-active firm.
    Searches Portal JUST using anaf_denumire as numeParte.
    """
    if state.sync_dosare_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync already running")

    state.sync_dosare_progress.update({
        "active": True, "total_firms": 0, "processed": 0,
        "firms_with_dosare": 0, "dosare_new": 0, "firme_new": 0,
        "errors": 0, "current_firma": None, "logs": []
    })

    background_tasks.add_task(
        _run_sync_dosare_per_firma,
        limit, date_start, date_end, categorie_caz, only_without_dosare, judet
    )
    return {"message": "Sync dosare pornit", "status": "running"}


async def _run_sync_dosare_per_firma(
    limit: int,
    date_start: str,
    date_end: str,
    categorie_caz: str,
    only_without_dosare: bool,
    judet: str
):
    """
    For each active firm (anaf_sync_status='found' AND active),
    search Portal JUST using anaf_denumire as numeParte.
    """
    import mongo_db as mdb

    try:
        mf_query = {
            "anaf_sync_status": "found",
            "anaf_denumire": {"$ne": None, "$not": {"$in": [None, ""]}},
            "anaf_stare": {"$regex": "^INREGISTRAT"},
            "anaf_inactiv": {"$ne": True}
        }
        if judet:
            mf_query["$or"] = [
                {"anaf_sediu_judet": {"$regex": judet, "$options": "i"}},
                {"judet": {"$regex": judet, "$options": "i"}}
            ]

        all_firms = await mdb.firme_col.find(mf_query, {"_id": 0}).sort("id", 1).limit(limit).to_list(limit)
        
        if only_without_dosare:
            # Exclude firms that already have dosare
            firm_ids_with_dosare = set(await mdb.dosare_col.distinct("firma_id"))
            all_firms = [f for f in all_firms if f["id"] not in firm_ids_with_dosare]

        total = len(all_firms)
        firms = all_firms

        state.sync_dosare_progress["total_firms"] = len(firms)
        state.add_sync_dosare_log(f"Firme active ANAF cu denumire: {total:,} total")
        state.add_sync_dosare_log(f"Procesăm: {len(firms)} firme")
        if categorie_caz:
            from schemas import CATEGORII_NUME
            state.add_sync_dosare_log(f"Filtru categorie: {CATEGORII_NUME.get(categorie_caz, categorie_caz)}")
        if date_start or date_end:
            state.add_sync_dosare_log(f"Perioadă: {date_start or '*'} → {date_end or '*'}")
        state.add_sync_dosare_log("─" * 40)

        async with aiohttp.ClientSession() as http_session:
            for firma in firms:
                if not state.sync_dosare_progress["active"]:
                    state.add_sync_dosare_log("Oprire solicitată.")
                    break

                idx = state.sync_dosare_progress["processed"] + 1
                search_name = firma.get("anaf_denumire", "")
                state.sync_dosare_progress["current_firma"] = search_name

                try:
                    # Search ALL institutions for this firm
                    all_dosare = []
                    for institutie in INSTITUTII:
                        if not state.sync_dosare_progress["active"]:
                            break
                        dosare = await fetch_dosare(
                            http_session, search_name, institutie,
                            date_start or "", date_end or ""
                        )
                        if dosare:
                            # Filter by category if needed
                            if categorie_caz:
                                dosare = [d for d in dosare if d.get('categorieCaz') == categorie_caz]
                            if dosare:
                                all_dosare.extend(dosare)

                    if all_dosare:
                        # Save dosare — but link them to THIS firma directly
                        new_count = await _save_dosare_for_firma(
                            db, firma, all_dosare, categorie_caz
                        )
                        state.sync_dosare_progress["dosare_new"] += new_count
                        state.sync_dosare_progress["firms_with_dosare"] += 1
                        state.add_sync_dosare_log(
                            f"[{idx}/{len(firms)}] {firma.cui} | {search_name[:45]} | {new_count} dosare noi"
                        )
                    else:
                        state.add_sync_dosare_log(
                            f"[{idx}/{len(firms)}] {firma.cui} | {search_name[:45]} | niciun dosar"
                        )

                except Exception as e:
                    state.sync_dosare_progress["errors"] += 1
                    state.add_sync_dosare_log(
                        f"[{idx}/{len(firms)}] {firma.cui} | Eroare: {str(e)[:50]}"
                    )
                    logger.error(f"[SYNC_DOSARE] Error for {firma.cui}: {e}")

                state.sync_dosare_progress["processed"] += 1
                # Small delay to be polite with the API
                await asyncio.sleep(0.3)

        state.add_sync_dosare_log("─" * 40)
        state.add_sync_dosare_log(
            f"Finalizat: {state.sync_dosare_progress['firms_with_dosare']} firme cu dosare, "
            f"{state.sync_dosare_progress['dosare_new']} dosare noi salvate"
        )

    except Exception as e:
        state.add_sync_dosare_log(f"Eroare generală: {str(e)[:60]}")
        logger.error(f"[SYNC_DOSARE] Error: {e}")
        state.sync_dosare_progress["active"] = False


async def _save_dosare_for_firma(firma: dict, dosare: list, categorie_caz: str = None) -> int:
    """Save dosare linked to a firma in MongoDB."""
    import mongo_db as mdb
    from pymongo import InsertOne
    firma_id = firma["id"]
    new_count = 0
    bulk_dosare = []
    bulk_timeline = []

    for dosar_data in dosare:
        if categorie_caz and dosar_data.get('categorieCaz') != categorie_caz:
            continue
        numar_dosar = dosar_data.get('numar', '')
        if not numar_dosar:
            continue
        existing = await mdb.get_dosar_by_firma_and_numar(firma_id, numar_dosar)
        if existing:
            continue
        dosar_id = await mdb.next_id("dosare")
        bulk_dosare.append(InsertOne({
            "id": dosar_id, "firma_id": firma_id,
            "numar_dosar": numar_dosar,
            "institutie": dosar_data.get('institutie', ''),
            "obiect": dosar_data.get('obiect', ''),
            "data_dosar": parse_date(dosar_data.get('data', '')),
            "stadiu": dosar_data.get('stadiuProcesual', ''),
            "categorie": dosar_data.get('categorieCaz', ''),
            "materie": dosar_data.get('materie', ''),
            "created_at": datetime.now(timezone.utc),
        }))
        new_count += 1
        for sedinta in (dosar_data.get('sedinte', []) or []):
            t_id = await mdb.next_id("timeline")
            bulk_timeline.append(InsertOne({"id": t_id, "dosar_id": dosar_id, "tip": "sedinta",
                "data": parse_date(sedinta.get('data', '')),
                "descriere": sedinta.get('solutie', '') or sedinta.get('complet', ''),
                "detalii": sedinta, "created_at": datetime.now(timezone.utc)}))

    if bulk_dosare:
        try:
            await mdb.dosare_col.bulk_write(bulk_dosare, ordered=False)
            if bulk_timeline:
                await mdb.timeline_col.bulk_write(bulk_timeline, ordered=False)
        except Exception as e:
            logger.error(f"[SYNC_DOSARE] Save error: {e}")
    return new_count



# ─── Bulk Download (month by month) ──────────────────────────────────────────

@router.post("/bulk-download")
async def start_bulk_download(
    background_tasks: BackgroundTasks,
    date_start: str = "2020-01-01",
    date_end: str = None,
    categorie_caz: str = None,
):
    """
    Download ALL dosare from Portal JUST, month by month.
    Saves raw JSON files without matching to DB.
    Designed for later offline matching on Docker.
    """
    if state.download_job_progress["active"]:
        raise HTTPException(status_code=409, detail="A download job is already running")

    if not date_end:
        date_end = datetime.now().strftime("%Y-%m-%d")

    state.download_job_stop_flag = False
    state.download_job_progress.update({
        "active": True, "processed": 0, "total": 0,
        "dosare_found": 0, "firme_new": 0, "logs": []
    })
    background_tasks.add_task(_run_bulk_download, date_start, date_end, categorie_caz)
    return {"message": f"Bulk download started: {date_start} → {date_end}", "status": "running"}


async def _run_bulk_download(date_start: str, date_end: str, categorie_caz: str = None):
    """Download dosare month by month, save raw JSON per month."""
    from dateutil.relativedelta import relativedelta

    start = datetime.strptime(date_start, "%Y-%m-%d")
    end = datetime.strptime(date_end, "%Y-%m-%d")
    cat_label = CATEGORII_NUME.get(categorie_caz, categorie_caz) if categorie_caz else "Toate"

    # Calculate months
    months = []
    current = start.replace(day=1)
    while current <= end:
        month_end = (current + relativedelta(months=1)) - relativedelta(days=1)
        if month_end > end:
            month_end = end
        months.append((current.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")))
        current = current + relativedelta(months=1)

    state.download_job_progress["total"] = len(months) * len(INSTITUTII)
    state.add_download_log(f"BULK DOWNLOAD: {date_start} → {date_end}")
    state.add_download_log(f"Categorie: {cat_label}")
    state.add_download_log(f"Luni de procesat: {len(months)} | Instituții: {len(INSTITUTII)}")
    state.add_download_log("─" * 50)

    total_dosare = 0
    total_companies = 0
    processed = 0

    async with aiohttp.ClientSession() as session:
        for month_start, month_end in months:
            if state.download_job_stop_flag:
                state.add_download_log("Oprire solicitată.")
                break

            month_label = month_start[:7]  # "2024-01"
            state.add_download_log(f"Luna {month_label}: {month_start} → {month_end}")
            month_dosare = []
            month_institutions_with_data = 0

            for institutie in INSTITUTII:
                if state.download_job_stop_flag:
                    break
                try:
                    dosare = await fetch_dosare(session, "", institutie, month_start, month_end)
                    if dosare:
                        # Filter by category if specified
                        if categorie_caz:
                            dosare = [d for d in dosare if d.get('categorieCaz') == categorie_caz]
                        if dosare:
                            month_dosare.extend(dosare)
                            month_institutions_with_data += 1
                except Exception as e:
                    logger.error(f"[BULK] Error {institutie} {month_label}: {e}")

                processed += 1
                state.download_job_progress["processed"] = processed
                await asyncio.sleep(0.3)

            # Save month file
            if month_dosare:
                filename = f"bulk_{month_label}_{len(month_dosare)}dosare.json"
                filepath = DOWNLOADS_DIR / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump({
                        "month": month_label,
                        "date_start": month_start,
                        "date_end": month_end,
                        "categorie_caz": categorie_caz,
                        "total_dosare": len(month_dosare),
                        "institutions_with_data": month_institutions_with_data,
                        "dosare": month_dosare
                    }, f, ensure_ascii=False, indent=2)
                total_dosare += len(month_dosare)
                # Count unique companies
                companies_set = set()
                for d in month_dosare:
                    for p in d.get('parti', []):
                        name = p.get('nume', '') or p.get('numeParte', '')
                        if name:
                            companies_set.add(name[:50])
                total_companies += len(companies_set)
                state.add_download_log(f"  → {len(month_dosare)} dosare salvate ({month_institutions_with_data} instituții) → {filename}")
            else:
                state.add_download_log(f"  → 0 dosare")

            state.download_job_progress["dosare_found"] = total_dosare

    state.add_download_log("─" * 50)
    state.add_download_log(f"FINALIZAT: {total_dosare:,} dosare descărcate, ~{total_companies:,} părți unice")
    state.download_job_progress["active"] = False


@router.get("/bulk-download/progress")
async def get_bulk_progress():
    return {
        "active": state.download_job_progress["active"],
        "processed": state.download_job_progress["processed"],
        "total": state.download_job_progress["total"],
        "dosare_found": state.download_job_progress["dosare_found"],
        "logs": state.download_job_progress["logs"][-80:]
    }


@router.post("/bulk-download/stop")
async def stop_bulk_download():
    state.download_job_stop_flag = True
    return {"message": "Stop requested"}


# ─── Match from saved files ──────────────────────────────────────────────────

@router.post("/match-from-files")
async def match_from_saved_files(
    background_tasks: BackgroundTasks,
    categorie_caz: str = None,
):
    """
    Process ALL saved bulk_*.json files and match companies against DB.
    Creates firms + dosare for matched companies.
    """
    if state.download_job_progress["active"]:
        raise HTTPException(status_code=409, detail="A job is already running")

    bulk_files = sorted([f for f in DOWNLOADS_DIR.iterdir() if f.name.startswith("bulk_") and f.suffix == ".json"])
    if not bulk_files:
        raise HTTPException(status_code=404, detail="Nu există fișiere bulk_*.json de procesat")

    state.download_job_stop_flag = False
    state.download_job_progress.update({
        "active": True, "processed": 0, "total": len(bulk_files),
        "dosare_found": 0, "firme_new": 0, "logs": []
    })
    background_tasks.add_task(_run_match_from_files, bulk_files, categorie_caz)
    return {"message": f"Match started: {len(bulk_files)} fișiere", "status": "running"}


async def _run_match_from_files(bulk_files: list, categorie_caz: str = None):
    """Process bulk JSON files and match/save to MongoDB."""
    import mongo_db as mdb
    state.add_download_log(f"MATCH FROM FILES: {len(bulk_files)} fișiere")
    state.add_download_log("─" * 50)

    total_stats = {'firme_new': 0, 'firme_existing': 0, 'dosare_new': 0, 'timeline_new': 0, 'skipped_no_match': 0}

    for i, filepath in enumerate(bulk_files):
        if state.download_job_stop_flag:
            state.add_download_log("Oprire solicitată.")
            break

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            dosare = data.get("dosare", [])
            if not dosare:
                state.add_download_log(f"[{i+1}/{len(bulk_files)}] {filepath.name}: 0 dosare, skip")
                state.download_job_progress["processed"] = i + 1
                continue

            stats = await save_to_mongo(dosare, "", categorie_caz, only_match_existing=False)
            for key in total_stats:
                total_stats[key] = total_stats.get(key, 0) + stats.get(key, 0)

            state.add_download_log(
                f"[{i+1}/{len(bulk_files)}] {filepath.name}: "
                f"{stats['dosare_new']} dosare, {stats['firme_new']} firme noi, "
                f"{stats['firme_existing']} existente"
            )
        except Exception as e:
            state.add_download_log(f"[{i+1}/{len(bulk_files)}] {filepath.name}: EROARE {str(e)[:60]}")

        state.download_job_progress["processed"] = i + 1
        state.download_job_progress["dosare_found"] = total_stats['dosare_new']
        state.download_job_progress["firme_new"] = total_stats['firme_new']

    state.add_download_log("─" * 50)
    state.add_download_log(
        f"FINALIZAT: {total_stats['dosare_new']:,} dosare noi, "
        f"{total_stats['firme_new']:,} firme noi, "
        f"{total_stats['firme_existing']:,} firme existente"
    )
    state.download_job_progress["active"] = False
