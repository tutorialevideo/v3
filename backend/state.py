"""
Global mutable state: progress trackers, stop flags, sessions, scheduler, mongo client.
All route modules import this module and reference state.variable to ensure
mutations propagate correctly across the app.
"""
import os
import logging
from datetime import datetime

from dotenv import load_dotenv
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

logger = logging.getLogger(__name__)

# ─── MongoDB ──────────────────────────────────────────────────────────────────
mongo_client = AsyncIOMotorClient(os.environ['MONGO_URL'])
mongo_db = mongo_client[os.environ['DB_NAME']]

# ─── Scheduler ────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

# ─── Download job state ───────────────────────────────────────────────────────
download_job_stop_flag = False
download_job_progress = {
    "active": False,
    "processed": 0,
    "total": 0,
    "dosare_found": 0,
    "firme_new": 0,
    "logs": []
}


def add_download_log(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    download_job_progress["logs"].append(f"[{timestamp}] {message}")
    if len(download_job_progress["logs"]) > 200:
        download_job_progress["logs"] = download_job_progress["logs"][-200:]


# ─── CSV import state ─────────────────────────────────────────────────────────
import_progress = {
    "active": False,
    "filename": "",
    "total_rows": 0,
    "processed": 0,
    "created_new": 0,
    "updated": 0,
    "skipped_not_company": 0,
    "skipped_no_cui": 0,
    "last_update": None
}

# ─── ANAF sync state ──────────────────────────────────────────────────────────
anaf_sync_progress = {
    "active": False,
    "total_firms": 0,
    "processed": 0,
    "found": 0,
    "not_found": 0,
    "errors": 0,
    "current_batch": 0,
    "total_batches": 0,
    "last_update": None,
    "eta_seconds": None,
    "logs": []
}


def add_anaf_log(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    anaf_sync_progress["logs"].append(f"[{timestamp}] {message}")
    if len(anaf_sync_progress["logs"]) > 100:
        anaf_sync_progress["logs"] = anaf_sync_progress["logs"][-100:]


# ─── Sync Dosare per Firmă state ──────────────────────────────────────────────
sync_dosare_progress = {
    "active": False,
    "total_firms": 0,
    "processed": 0,
    "firms_with_dosare": 0,
    "dosare_new": 0,
    "firme_new": 0,
    "errors": 0,
    "current_firma": None,
    "logs": []
}


def add_sync_dosare_log(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    sync_dosare_progress["logs"].append(f"[{timestamp}] {message}")
    if len(sync_dosare_progress["logs"]) > 300:
        sync_dosare_progress["logs"] = sync_dosare_progress["logs"][-300:]


# ─── MFinante state ───────────────────────────────────────────────────────────
mfinante_sync_progress = {
    "active": False,
    "session_valid": False,
    "total_firms": 0,
    "processed": 0,
    "found": 0,
    "not_found": 0,
    "skipped": 0,
    "errors": 0,
    "last_update": None,
    "last_cui": None,
    "logs": []
}


def add_mfinante_log(message: str):
    from datetime import datetime
    timestamp = datetime.now().strftime("%H:%M:%S")
    mfinante_sync_progress["logs"].append(f"[{timestamp}] {message}")
    if len(mfinante_sync_progress["logs"]) > 300:
        mfinante_sync_progress["logs"] = mfinante_sync_progress["logs"][-300:]

mfinante_session = {
    "jsessionid": None,
    "cookies": {}
}

captcha_session = {
    "cookies": None,
    "jsessionid": None
}

# ─── BPI folder scan state ────────────────────────────────────────────────────
bpi_scan_progress = {
    "active": False,
    "total_files": 0,
    "processed": 0,
    "records_found": 0,
    "errors": 0,
    "current_file": None,
    "logs": []
}


def add_bpi_log(message: str):
    from datetime import datetime
    timestamp = datetime.now().strftime("%H:%M:%S")
    bpi_scan_progress["logs"].append(f"[{timestamp}] {message}")
    if len(bpi_scan_progress["logs"]) > 200:
        bpi_scan_progress["logs"] = bpi_scan_progress["logs"][-200:]
