"""
Portal JUST Downloader - FastAPI entry point.
Imports all route modules and registers them on the app.
"""
import logging
import os

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

import state
import database
from routes.jobs import router as jobs_router, update_scheduler
from routes.firme import router as firme_router
from routes.anaf import router as anaf_router
from routes.mfinante import router as mfinante_router
from routes.diagnostics import router as diagnostics_router
from routes.localitati import router as localitati_router
from routes.bpi import router as bpi_router
from routes.crawler import router as crawler_router
from routes.supabase_sync import router as supabase_router

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()
app.router.redirect_slashes = False

# ─── CORS ─────────────────────────────────────────────────────────────────────
cors_origins_env = os.environ.get('CORS_ORIGINS', '')
cors_origins = (
    [o.strip() for o in cors_origins_env.split(',')]
    if cors_origins_env and cors_origins_env != '*'
    else ["*"]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=cors_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────────────────────
PREFIX = "/api"
app.include_router(jobs_router, prefix=PREFIX)
app.include_router(firme_router, prefix=PREFIX)
app.include_router(anaf_router, prefix=PREFIX)
app.include_router(mfinante_router, prefix=PREFIX)
app.include_router(diagnostics_router, prefix=PREFIX)
app.include_router(localitati_router, prefix=PREFIX)
app.include_router(bpi_router, prefix=PREFIX)
app.include_router(crawler_router, prefix=PREFIX)
app.include_router(supabase_router, prefix=PREFIX)


# ─── Lifecycle ────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    # MongoDB indexes
    try:
        import mongo_db as mdb
        await mdb.create_indexes()
        logger.info("[MongoDB] Indexes created")
    except Exception as e:
        logger.warning(f"[MongoDB] Index creation failed: {e}")

    # Async PostgreSQL (Supabase) — optional, used only for Supabase sync
    if database.database is not None:
        try:
            await database.database.connect()
            logger.info("[DB] Async connection established")
        except Exception as e:
            logger.warning(f"[DB] Async connection failed (non-fatal): {e}")

    state.scheduler.start()

    try:
        config = await state.mongo_db.job_config.find_one({}, {"_id": 0})
        if config and config.get('cron_enabled'):
            update_scheduler(config.get('schedule_hour', 2), config.get('schedule_minute', 0), True)
    except Exception as e:
        logger.warning(f"[MongoDB] Could not load config: {e}")

    # Restore MFinante session from MongoDB
    try:
        from routes.mfinante import _load_session_from_db
        await _load_session_from_db()
    except Exception as e:
        logger.warning(f"[MFINANTE] Could not restore session: {e}")

    logger.info("Application started")


@app.on_event("shutdown")
async def shutdown_event():
    if database.database is not None:
        try:
            await database.database.disconnect()
        except Exception:
            pass
    state.scheduler.shutdown()
    try:
        state.mongo_client.close()
    except Exception:
        pass
