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
import database
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


async def save_to_postgres(dosare: list, search_term: str, categorie_caz: str = None) -> dict:
    db = database.SessionLocal()
    Firma = database.Firma
    Dosar = database.Dosar
    TimelineEvent = database.TimelineEvent
    stats = {'firme_new': 0, 'firme_existing': 0, 'dosare_new': 0, 'timeline_new': 0, 'skipped_categorie': 0}
    try:
        for dosar_data in dosare:
            # Filter by categorie if specified
            if categorie_caz:
                dosar_categorie = dosar_data.get('categorieCaz', '')
                if dosar_categorie != categorie_caz:
                    stats['skipped_categorie'] += 1
                    continue

            parti = dosar_data.get('parti', [])
            if not isinstance(parti, list):
                continue
            companies = extract_companies_from_parti(parti)
            if not companies:
                continue
            for company in companies:
                firma = db.query(Firma).filter(Firma.denumire_normalized == company['denumire_normalized']).first()
                if not firma:
                    firma = Firma(denumire=company['denumire'], denumire_normalized=company['denumire_normalized'])
                    db.add(firma)
                    db.flush()
                    stats['firme_new'] += 1
                else:
                    stats['firme_existing'] += 1
                numar_dosar = dosar_data.get('numar', '')
                if db.query(Dosar).filter(Dosar.firma_id == firma.id, Dosar.numar_dosar == numar_dosar).first():
                    continue
                dosar = Dosar(
                    firma_id=firma.id, numar_dosar=numar_dosar,
                    institutie=dosar_data.get('institutie', ''), obiect=dosar_data.get('obiect', ''),
                    data_dosar=parse_date(dosar_data.get('data', '')), stadiu=dosar_data.get('stadiuProcesual', ''),
                    categorie=dosar_data.get('categorieCaz', ''), materie=dosar_data.get('materie', ''),
                    raw_data=dosar_data
                )
                db.add(dosar)
                db.flush()
                stats['dosare_new'] += 1
                for sedinta in (dosar_data.get('sedinte', []) or []):
                    db.add(TimelineEvent(
                        dosar_id=dosar.id, tip='sedinta',
                        data=parse_date(sedinta.get('data', '')),
                        descriere=sedinta.get('solutie', '') or sedinta.get('complet', ''),
                        detalii=sedinta
                    ))
                    stats['timeline_new'] += 1
                for cale in (dosar_data.get('caiAtac', []) or []):
                    db.add(TimelineEvent(
                        dosar_id=dosar.id, tip='cale_atac',
                        data=parse_date(cale.get('dataDeclarare', '')),
                        descriere=cale.get('tipCaleAtac', ''), detalii=cale
                    ))
                    stats['timeline_new'] += 1
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error saving to PostgreSQL: {e}")
        raise
    finally:
        db.close()
    return stats


# ─── Download job ─────────────────────────────────────────────────────────────

async def run_download_job(search_term: str, job_run_id: str,
                           date_start: str = "", date_end: str = "",
                           triggered_by: str = "manual", categorie_caz: str = None):
    state.download_job_stop_flag = False
    label_info = search_term if search_term and search_term.strip() else f"{date_start or '*'} → {date_end or '*'}"
    cat_label = CATEGORII_NUME.get(categorie_caz, categorie_caz) if categorie_caz else "Toate categoriile"
    logger.info(f"Starting download job: {label_info} | categorie: {cat_label}")

    state.download_job_progress["active"] = True
    state.download_job_progress.update({"processed": 0, "total": len(INSTITUTII), "dosare_found": 0, "firme_new": 0, "logs": []})
    state.add_download_log(f"Job pornit: {label_info}")
    state.add_download_log(f"Categorie: {cat_label}")
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
                stats = await save_to_postgres(dosare, search_term, categorie_caz)
                for key in total_stats:
                    total_stats[key] = total_stats.get(key, 0) + stats.get(key, 0)
                # Only count dosare that passed the category filter
                all_dosare.extend(d for d in dosare if not categorie_caz or d.get('categorieCaz') == categorie_caz)
                saved = stats['dosare_new']
                skipped = stats.get('skipped_categorie', 0)
                if saved > 0 or (dosare and not skipped):
                    msg = f"[{processed+1}/{len(INSTITUTII)}] {institutie}: {len(dosare)} total"
                    if categorie_caz:
                        msg += f", {saved} salvate ({skipped} alte categorii)"
                    else:
                        msg += f", {saved} firme noi"
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
        "manual", config.get('categorie_caz')
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
    if database.SessionLocal is not None:
        try:
            db = database.SessionLocal()
            firme_count = db.query(database.Firma).count()
            dosare_count = db.query(database.Dosar).count()
            db.close()
        except Exception as e:
            logger.warning(f"Could not get DB stats: {e}")
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
