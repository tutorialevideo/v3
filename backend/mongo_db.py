"""
mongo_db.py — MongoDB collections replacing PostgreSQL/SQLAlchemy.
All data (firme, dosare, bilanturi, timeline) stored in MongoDB.
Uses Motor for async access.
"""
import os
import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)

MONGO_URL = os.environ.get('MONGO_URL', 'mongodb://localhost:27017/')
DB_NAME = os.environ.get('DB_NAME', 'justportal')

# Motor client + collections
client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]

# Collections
firme_col = db['firme']
dosare_col = db['dosare']
bilanturi_col = db['bilanturi']
timeline_col = db['timeline_events']
judete_col = db['judete']
localitati_col = db['localitati']
counters_col = db['counters']
counters_col = db['counters']


# ─── Auto-increment IDs ───────────────────────────────────────────────────────

async def next_id(collection: str) -> int:
    """Get next auto-increment ID for a collection."""
    result = await counters_col.find_one_and_update(
        {"_id": collection},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=True
    )
    return result["seq"]


# ─── Indexes (run once at startup) ───────────────────────────────────────────

async def create_indexes():
    """Create MongoDB indexes for performance."""
    try:
        # firme indexes
        await firme_col.create_index("id", unique=True)
        await firme_col.create_index("cui", sparse=True)
        await firme_col.create_index("denumire_normalized")
        await firme_col.create_index("anaf_sync_status")
        await firme_col.create_index([("anaf_sync_status", 1), ("anaf_stare", 1)])
        await firme_col.create_index("judet")
        await firme_col.create_index("siruta", sparse=True)

        # dosare indexes
        await dosare_col.create_index("id", unique=True)
        await dosare_col.create_index("firma_id")
        await dosare_col.create_index("numar_dosar")

        # bilanturi indexes
        await bilanturi_col.create_index("id", unique=True)
        await bilanturi_col.create_index("firma_id")
        await bilanturi_col.create_index([("firma_id", 1), ("an", 1)])

        # localitati indexes
        await localitati_col.create_index("county_id")
        await localitati_col.create_index("siruta")
        await localitati_col.create_index("name_normalized")

        logger.info("[MongoDB] Indexes created")
    except Exception as e:
        logger.warning(f"[MongoDB] Index creation: {e}")


# ─── Helper: strip _id from docs ─────────────────────────────────────────────

def clean_doc(doc: dict) -> dict:
    """Remove MongoDB _id from document for API responses."""
    if doc and '_id' in doc:
        doc.pop('_id', None)
    return doc or {}


def clean_docs(docs: list) -> list:
    return [clean_doc(d) for d in docs]


# ─── Firma helpers ────────────────────────────────────────────────────────────

async def get_firma_by_id(firma_id: int) -> dict:
    doc = await firme_col.find_one({"id": firma_id}, {"_id": 0})
    return doc


async def get_firma_by_cui(cui: str) -> dict:
    doc = await firme_col.find_one({"cui": cui}, {"_id": 0})
    return doc


async def get_firma_by_denumire_norm(norm: str) -> dict:
    doc = await firme_col.find_one({"denumire_normalized": norm}, {"_id": 0})
    return doc


async def upsert_firma_by_norm(denumire: str, denumire_norm: str) -> dict:
    """Find or create firma by normalized name. Returns the firma doc."""
    existing = await get_firma_by_denumire_norm(denumire_norm)
    if existing:
        return existing
    firma_id = await next_id("firme")
    doc = {
        "id": firma_id,
        "cui": None,
        "denumire": denumire,
        "denumire_normalized": denumire_norm,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    await firme_col.insert_one(doc)
    doc.pop('_id', None)
    return doc


async def count_firme(query: dict = None) -> int:
    return await firme_col.count_documents(query or {})


# ─── Dosar helpers ────────────────────────────────────────────────────────────

async def get_dosar_by_firma_and_numar(firma_id: int, numar: str) -> dict:
    return await dosare_col.find_one({"firma_id": firma_id, "numar_dosar": numar}, {"_id": 0})


async def count_dosare_for_firma(firma_id: int) -> int:
    return await dosare_col.count_documents({"firma_id": firma_id})


# ─── Bilant helpers ───────────────────────────────────────────────────────────

async def get_bilant(firma_id: int, an: str) -> dict:
    return await bilanturi_col.find_one({"firma_id": firma_id, "an": an}, {"_id": 0})


async def count_bilanturi(query: dict = None) -> int:
    return await bilanturi_col.count_documents(query or {})
