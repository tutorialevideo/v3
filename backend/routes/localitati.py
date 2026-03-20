"""
Localitati routes — MongoDB backend.
42 judete + 13,749 localitati stored in MongoDB collections.
"""
import re
import logging
import unicodedata
from datetime import datetime

import aiohttp
from fastapi import APIRouter, BackgroundTasks, HTTPException

import mongo_db as mdb
import state

router = APIRouter()
logger = logging.getLogger(__name__)

LOCALITATI_SQL_URL = "https://raw.githubusercontent.com/romania/localitati/master/judete-orase.sql"

normalize_progress = {"active": False, "processed": 0, "total": 0, "matched_judet": 0, "matched_localitate": 0}


def _normalize_str(s: str) -> str:
    if not s:
        return ""
    nfd = unicodedata.normalize('NFD', s)
    ascii_str = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    return ascii_str.upper().strip()


def _clean_judet_input(raw: str) -> str:
    if not raw:
        return ""
    s = raw.upper().strip()
    for prefix in ["JUDETUL ", "JUD. ", "JUD.", "MUN. ", "MUN.", "MUNICIPIUL "]:
        if s.startswith(prefix):
            s = s[len(prefix):].strip()
    return s


def _clean_localitate_input(raw: str) -> str:
    if not raw:
        return ""
    s = raw.upper().strip()
    for prefix in ["MUN. ", "MUN.", "MUNICIPIUL ", "ORȘ. ", "ORS. ", "ORAS ", "COM. ", "COMUNA "]:
        if s.startswith(prefix):
            s = s[len(prefix):].strip()
    if ',' in s:
        s = s.split(',')[0].strip()
    return s


@router.get("/localitati/stats")
async def get_localitati_stats():
    judete_count = await mdb.judete_col.count_documents({})
    localitati_count = await mdb.localitati_col.count_documents({})
    firme_cu_siruta = await mdb.firme_col.count_documents({"siruta": {"$ne": None}})
    firme_total = await mdb.firme_col.count_documents({})
    firme_cu_judet = await mdb.firme_col.count_documents({"judet": {"$ne": None, "$exists": True}})
    return {
        "available": judete_count > 0,
        "judete_count": judete_count,
        "localitati_count": localitati_count,
        "firme_total": firme_total,
        "firme_cu_judet": firme_cu_judet,
        "firme_cu_siruta": firme_cu_siruta,
        "firme_fara_siruta": firme_total - firme_cu_siruta,
    }


@router.post("/localitati/import")
async def import_localitati(background_tasks: BackgroundTasks):
    count = await mdb.judete_col.count_documents({})
    if count > 0:
        return {"message": f"Datele sunt deja importate ({count} județe).", "already_imported": True}
    background_tasks.add_task(_run_import_localitati)
    return {"message": "Import pornit în background."}


@router.post("/localitati/import/force")
async def force_import_localitati(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_import_localitati, force=True)
    return {"message": "Re-import forțat pornit."}


async def _run_import_localitati(force: bool = False):
    logger.info("[LOCALITATI] Starting import from GitHub...")
    try:
        if force:
            await mdb.localitati_col.drop()
            await mdb.judete_col.drop()

        async with aiohttp.ClientSession() as session:
            async with session.get(LOCALITATI_SQL_URL, timeout=aiohttp.ClientTimeout(total=30)) as r:
                sql_content = await r.text()

        county_map = {}
        county_re = re.finditer(r"\((\d+),\s*'([A-Z]+)',\s*'([^']+)'\)", sql_content)
        judete_inserted = 0
        for m in county_re:
            cid, code, name = int(m.group(1)), m.group(2), m.group(3)
            doc = {"id": cid, "code": code, "name": name, "name_normalized": _normalize_str(name)}
            await mdb.judete_col.replace_one({"id": cid}, doc, upsert=True)
            county_map[cid] = doc
            judete_inserted += 1

        city_pattern = re.compile(r"\((\d+),\s*(\d+),\s*(\d+),\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']*)'\)")
        localitati_inserted = 0
        batch = []
        for m in city_pattern.finditer(sql_content):
            cid, county_id, siruta = int(m.group(1)), int(m.group(2)), int(m.group(3))
            lng, lat = float(m.group(4)), float(m.group(5))
            name, region = m.group(6), m.group(7)
            batch.append({
                "id": cid, "county_id": county_id, "siruta": siruta,
                "longitude": lng, "latitude": lat,
                "name": name, "name_normalized": _normalize_str(name), "region": region
            })
            localitati_inserted += 1
            if len(batch) >= 1000:
                from pymongo import ReplaceOne
                ops = [ReplaceOne({"id": d["id"]}, d, upsert=True) for d in batch]
                await mdb.localitati_col.bulk_write(ops, ordered=False)
                batch = []
                logger.info(f"[LOCALITATI] Inserted {localitati_inserted} localitati...")

        if batch:
            from pymongo import ReplaceOne
            ops = [ReplaceOne({"id": d["id"]}, d, upsert=True) for d in batch]
            await mdb.localitati_col.bulk_write(ops, ordered=False)

        # Create indexes
        await mdb.judete_col.create_index("code", unique=True)
        await mdb.judete_col.create_index("name_normalized")
        await mdb.localitati_col.create_index("county_id")
        await mdb.localitati_col.create_index("siruta")
        await mdb.localitati_col.create_index("name_normalized")

        logger.info(f"[LOCALITATI] Import complete: {judete_inserted} județe, {localitati_inserted} localități")
    except Exception as e:
        logger.error(f"[LOCALITATI] Import error: {e}")


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
    judete_count = await mdb.judete_col.count_documents({})
    if judete_count == 0:
        raise HTTPException(status_code=400, detail="Importați localitățile mai întâi.")
    if normalize_progress["active"]:
        raise HTTPException(status_code=400, detail="Normalizare deja în progres")
    background_tasks.add_task(_run_normalize, only_unmatched, judet_filter, limit)
    return {"message": "Normalizare pornită în background"}


async def _run_normalize(only_unmatched: bool, judet_filter: str, limit: int):
    normalize_progress.update({"active": True, "processed": 0, "matched_judet": 0, "matched_localitate": 0})
    try:
        # Build county cache
        judete = await mdb.judete_col.find({}, {"_id": 0}).to_list(None)
        judet_cache = {j["name_normalized"]: j for j in judete}
        judet_code_cache = {j["code"].upper(): j for j in judete}

        # Build locality cache
        localitati = await mdb.localitati_col.find({}, {"_id": 0}).to_list(None)
        loc_cache = {}
        for loc in localitati:
            key = (loc["county_id"], loc["name_normalized"])
            loc_cache[key] = loc

        # Query firms
        query = {}
        if only_unmatched:
            query["siruta"] = None
        if judet_filter:
            query["judet"] = {"$regex": judet_filter, "$options": "i"}

        total = await mdb.firme_col.count_documents(query)
        normalize_progress["total"] = total

        from pymongo import UpdateOne
        offset = 0
        batch_size = 500
        matched_judet = 0
        matched_loc = 0

        while offset < total and normalize_progress["active"]:
            batch = await mdb.firme_col.find(query, {"_id": 0, "id": 1, "judet": 1, "localitate": 1, "anaf_sediu_judet": 1}).skip(offset).limit(batch_size).to_list(batch_size)
            if not batch:
                break

            bulk_ops = []
            for firma in batch:
                update = {}
                raw_judet = firma.get("judet") or firma.get("anaf_sediu_judet") or ""
                cleaned_judet = _clean_judet_input(raw_judet)
                norm_judet = _normalize_str(cleaned_judet)

                matched_j = judet_cache.get(norm_judet)
                if not matched_j and len(norm_judet) <= 3:
                    matched_j = judet_code_cache.get(norm_judet)
                if not matched_j and norm_judet:
                    for jn, j in judet_cache.items():
                        if jn.startswith(norm_judet) or norm_judet.startswith(jn):
                            matched_j = j; break

                if matched_j:
                    if firma.get("judet") != matched_j["name"]:
                        update["judet"] = matched_j["name"]
                    matched_judet += 1

                    raw_loc = firma.get("localitate") or ""
                    norm_loc = _normalize_str(_clean_localitate_input(raw_loc))
                    if norm_loc:
                        matched_l = loc_cache.get((matched_j["id"], norm_loc))
                        if not matched_l:
                            for loc in localitati:
                                if loc["county_id"] == matched_j["id"] and len(norm_loc) >= 4:
                                    if loc["name_normalized"].startswith(norm_loc[:min(len(norm_loc), 15)]):
                                        matched_l = loc; break
                        if matched_l:
                            if firma.get("localitate") != matched_l["name"]:
                                update["localitate"] = matched_l["name"]
                            update["siruta"] = matched_l["siruta"]
                            matched_loc += 1

                if update:
                    bulk_ops.append(UpdateOne({"id": firma["id"]}, {"$set": update}))

            if bulk_ops:
                await mdb.firme_col.bulk_write(bulk_ops, ordered=False)

            offset += batch_size
            normalize_progress.update({"processed": min(offset, total), "matched_judet": matched_judet, "matched_localitate": matched_loc})

        logger.info(f"[LOCALITATI] Normalizare completă: {matched_judet} județe, {matched_loc} localități")
    except Exception as e:
        logger.error(f"[LOCALITATI] Normalize error: {e}")
    finally:
        normalize_progress["active"] = False


@router.get("/localitati/judete")
async def get_judete():
    judete = await mdb.judete_col.find({}, {"_id": 0}).sort("name", 1).to_list(None)
    return judete


@router.get("/localitati/search")
async def search_localitati(q: str, judet_id: int = None, limit: int = 20):
    query = {"name": {"$regex": q, "$options": "i"}}
    if judet_id:
        query["county_id"] = judet_id
    results = await mdb.localitati_col.find(query, {"_id": 0}).limit(limit).to_list(limit)
    return results
