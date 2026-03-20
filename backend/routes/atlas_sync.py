"""
MongoDB Atlas Sync — transfers processed data from local MongoDB to Atlas cloud.
Local Docker = processing power, Atlas = production clean view.

Collections synced: firme (active only), bilanturi, dosare (optional)
"""
import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import InsertOne, ReplaceOne

import mongo_db as mdb
import state

router = APIRouter()
logger = logging.getLogger(__name__)

ATLAS_URL = os.environ.get(
    "ATLAS_MONGO_URL",
    "mongodb+srv://aaaaaaaaaaaaaaaaaaaaaaaa:4GURTj9ZKNfZNDqy@cluster0.ck0gbc.mongodb.net/justportal"
)
ATLAS_DB_NAME = "justportal"

# ─── Atlas sync state ──────────────────────────────────────────────────────────
atlas_sync_progress = {
    "active": False,
    "phase": "",
    "total": 0,
    "processed": 0,
    "upserted": 0,
    "errors": 0,
    "logs": []
}


def add_atlas_log(message: str):
    ts = datetime.now().strftime("%H:%M:%S")
    atlas_sync_progress["logs"].append(f"[{ts}] {message}")
    if len(atlas_sync_progress["logs"]) > 200:
        atlas_sync_progress["logs"] = atlas_sync_progress["logs"][-200:]


def get_atlas_client():
    return AsyncIOMotorClient(ATLAS_URL, serverSelectionTimeoutMS=10000)


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/atlas/status")
async def get_atlas_status():
    """Check Atlas connection + counts comparison."""
    atlas_ok = False
    atlas_counts = {}
    error = None
    try:
        client = get_atlas_client()
        atlas_db = client[ATLAS_DB_NAME]
        await client.admin.command('ping')
        atlas_counts = {
            "firme": await atlas_db.firme.count_documents({}),
            "bilanturi": await atlas_db.bilanturi.count_documents({}),
            "dosare": await atlas_db.dosare.count_documents({}),
        }
        atlas_ok = True
        client.close()
    except Exception as e:
        error = str(e)[:150]

    local_counts = {}
    try:
        base = {"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}}
        active = {**base, "anaf_stare": {"$regex": "ACTIV", "$options": "i"},
                  "$nor": [{"anaf_stare": {"$regex": "INACTIV", "$options": "i"}},
                           {"anaf_stare": {"$regex": "RADIERE", "$options": "i"}}]}
        local_counts = {
            "firme_total": await mdb.firme_col.count_documents({}),
            "firme_active": await mdb.firme_col.count_documents(active),
            "bilanturi": await mdb.bilanturi_col.count_documents({}),
            "dosare": await mdb.dosare_col.count_documents({}),
        }
    except Exception:
        pass

    return {
        "atlas_connected": atlas_ok,
        "atlas_error": error,
        "atlas_counts": atlas_counts,
        "local_counts": local_counts,
        "sync_active": atlas_sync_progress["active"],
        "progress": atlas_sync_progress,
        "logs": atlas_sync_progress["logs"][-60:]
    }


@router.get("/atlas/sync-progress")
async def get_atlas_sync_progress():
    return {**atlas_sync_progress, "logs": atlas_sync_progress["logs"][-80:]}


@router.post("/atlas/sync-stop")
async def stop_atlas_sync():
    atlas_sync_progress["active"] = False
    return {"message": "Stop requested"}


@router.post("/atlas/init-indexes")
async def init_atlas_indexes():
    """Create indexes in Atlas for performance."""
    try:
        client = get_atlas_client()
        atlas_db = client[ATLAS_DB_NAME]
        await atlas_db.firme.create_index("id", unique=True)
        await atlas_db.firme.create_index("cui", sparse=True)
        await atlas_db.firme.create_index("denumire_normalized")
        await atlas_db.firme.create_index("anaf_sync_status")
        await atlas_db.dosare.create_index("firma_id")
        await atlas_db.bilanturi.create_index([("firma_id", 1), ("an", 1)])
        client.close()
        return {"success": True, "message": "Indexuri Atlas create!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/atlas/sync")
async def start_atlas_sync(
    background_tasks: BackgroundTasks,
    only_active: bool = True,
    sync_bilanturi: bool = True,
    sync_dosare: bool = False,
    clean_first: bool = False,
    batch_size: int = 500,
):
    if atlas_sync_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync deja în progres")

    atlas_sync_progress.update({
        "active": True, "phase": "Inițializare",
        "total": 0, "processed": 0, "upserted": 0, "errors": 0, "logs": []
    })

    background_tasks.add_task(_run_atlas_sync, only_active, sync_bilanturi, sync_dosare, clean_first, batch_size)
    return {"message": "Sync Atlas pornit", "status": "running"}


# ─── Sync logic ───────────────────────────────────────────────────────────────

async def _run_atlas_sync(only_active, sync_bilanturi, sync_dosare, clean_first, batch_size):
    try:
        add_atlas_log("Conectare la MongoDB Atlas...")
        client = get_atlas_client()
        atlas_db = client[ATLAS_DB_NAME]

        try:
            await client.admin.command('ping')
            add_atlas_log("✅ Conectat la Atlas!")
        except Exception as e:
            add_atlas_log(f"❌ Eroare conexiune Atlas: {str(e)[:80]}")
            atlas_sync_progress["active"] = False
            return

        # Create indexes
        try:
            await atlas_db.firme.create_index("id", unique=True)
            await atlas_db.firme.create_index("cui", sparse=True)
            await atlas_db.bilanturi.create_index([("firma_id", 1), ("an", 1)])
            await atlas_db.dosare.create_index("firma_id")
        except Exception:
            pass

        if clean_first:
            add_atlas_log("🗑️ Ștergere date vechi din Atlas...")
            await atlas_db.firme.drop()
            await atlas_db.bilanturi.drop()
            if sync_dosare:
                await atlas_db.dosare.drop()
            add_atlas_log("✅ Atlas curățat")

        # ── Sync firme ─────────────────────────────────────────────────────────
        atlas_sync_progress["phase"] = "Firme"
        query = {}
        if only_active:
            query = {
                "cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}},
                "anaf_stare": {"$regex": "ACTIV", "$options": "i"},
                "$nor": [{"anaf_stare": {"$regex": "INACTIV", "$options": "i"}},
                         {"anaf_stare": {"$regex": "RADIERE", "$options": "i"}}]
            }
            add_atlas_log("Filtru: doar firme ACTIVE ANAF")
        else:
            add_atlas_log("Filtru: toate firmele")

        total = await mdb.firme_col.count_documents(query)
        atlas_sync_progress["total"] = total
        add_atlas_log(f"Firme de sincronizat: {total:,}")

        offset = 0
        upserted = 0
        while offset < total and atlas_sync_progress["active"]:
            batch = await mdb.firme_col.find(query, {"_id": 0}).sort("id", 1).skip(offset).limit(batch_size).to_list(batch_size)
            if not batch:
                break

            ops = [ReplaceOne({"id": doc["id"]}, doc, upsert=True) for doc in batch]
            result = await atlas_db.firme.bulk_write(ops, ordered=False)
            upserted += result.upserted_count + result.modified_count
            atlas_sync_progress["upserted"] = upserted
            atlas_sync_progress["processed"] = offset + len(batch)

            if offset % (batch_size * 10) == 0 and offset > 0:
                pct = min(offset / total * 100, 100)
                add_atlas_log(f"📊 Firme: {offset:,}/{total:,} ({pct:.0f}%) | Upserted: {upserted:,}")

            offset += batch_size
            await asyncio.sleep(0.05)

        add_atlas_log(f"✅ Firme sincronizate: {upserted:,}")

        # ── Sync bilanturi ─────────────────────────────────────────────────────
        if sync_bilanturi and atlas_sync_progress["active"]:
            atlas_sync_progress["phase"] = "Bilanțuri"
            # Only sync bilanturi for firms in Atlas
            atlas_firma_ids = set()
            async for doc in atlas_db.firme.find({}, {"id": 1}):
                atlas_firma_ids.add(doc["id"])

            total_b = await mdb.bilanturi_col.count_documents({"firma_id": {"$in": list(atlas_firma_ids)}}) if atlas_firma_ids else 0
            add_atlas_log(f"Bilanțuri de sincronizat: {total_b:,}")

            offset = 0
            upserted_b = 0
            while offset < total_b and atlas_sync_progress["active"]:
                batch = await mdb.bilanturi_col.find(
                    {"firma_id": {"$in": list(atlas_firma_ids)}}, {"_id": 0}
                ).skip(offset).limit(batch_size).to_list(batch_size)
                if not batch:
                    break
                ops = [ReplaceOne({"id": doc["id"]}, doc, upsert=True) for doc in batch]
                result = await atlas_db.bilanturi.bulk_write(ops, ordered=False)
                upserted_b += result.upserted_count + result.modified_count
                offset += batch_size
                await asyncio.sleep(0.05)
            add_atlas_log(f"✅ Bilanțuri sincronizate: {upserted_b:,}")

        # ── Sync dosare ────────────────────────────────────────────────────────
        if sync_dosare and atlas_sync_progress["active"]:
            atlas_sync_progress["phase"] = "Dosare"
            atlas_firma_ids = set()
            async for doc in atlas_db.firme.find({}, {"id": 1}):
                atlas_firma_ids.add(doc["id"])
            total_d = await mdb.dosare_col.count_documents({"firma_id": {"$in": list(atlas_firma_ids)}}) if atlas_firma_ids else 0
            add_atlas_log(f"Dosare de sincronizat: {total_d:,}")
            offset = 0
            upserted_d = 0
            while offset < total_d and atlas_sync_progress["active"]:
                batch = await mdb.dosare_col.find(
                    {"firma_id": {"$in": list(atlas_firma_ids)}}, {"_id": 0}
                ).skip(offset).limit(batch_size).to_list(batch_size)
                if not batch:
                    break
                ops = [ReplaceOne({"id": doc["id"]}, doc, upsert=True) for doc in batch]
                await atlas_db.dosare.bulk_write(ops, ordered=False)
                upserted_d += len(batch)
                offset += batch_size
                await asyncio.sleep(0.05)
            add_atlas_log(f"✅ Dosare sincronizate: {upserted_d:,}")

        client.close()
        add_atlas_log("─" * 40)
        add_atlas_log(f"✅ Sync Atlas complet: {atlas_sync_progress['upserted']:,} documente trimise")

    except Exception as e:
        add_atlas_log(f"❌ Eroare: {str(e)[:80]}")
        logger.error(f"[ATLAS SYNC] Error: {e}")
    finally:
        atlas_sync_progress["active"] = False
