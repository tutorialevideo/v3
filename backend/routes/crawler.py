"""
MFirme crawler: logs into mfirme.ro admin, scrapes all company CUIs,
matches against local DB and adds new firms.
Credentials come from env vars (MFIRME_URL, MFIRME_USER, MFIRME_PASS).
"""
import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup
from fastapi import APIRouter, BackgroundTasks, HTTPException

import mongo_db as mdb
import state

router = APIRouter()
logger = logging.getLogger(__name__)

MFIRME_URL = os.environ.get("MFIRME_URL", "https://www.mfirme.ro/control/admin/mfirme/company/")
MFIRME_USER = os.environ.get("MFIRME_USER", "")
MFIRME_PASS = os.environ.get("MFIRME_PASS", "")
LOGIN_URL = "https://www.mfirme.ro/control/admin/login/"

CHECKPOINT_FILE = Path(__file__).parent.parent / "downloads" / "mfirme_checkpoint.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8",
}


def _save_checkpoint(page: int, cuis_new: int):
    try:
        CHECKPOINT_FILE.parent.mkdir(exist_ok=True)
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"last_page": page, "cuis_new": cuis_new, "saved_at": datetime.utcnow().isoformat()}, f)
    except Exception:
        pass


def _load_checkpoint() -> dict:
    try:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {"last_page": 0, "cuis_new": 0}


def _clear_checkpoint():
    try:
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
    except Exception:
        pass


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/crawler/mfirme/status")
async def get_mfirme_status():
    checkpoint = _load_checkpoint()
    return {
        "configured": bool(MFIRME_USER and MFIRME_PASS),
        "active": state.mfirme_crawl_progress["active"],
        "progress": state.mfirme_crawl_progress,
        "checkpoint": checkpoint,
        "logs": state.mfirme_crawl_progress["logs"][-60:]
    }


@router.post("/crawler/mfirme/start")
async def start_mfirme_crawl(
    background_tasks: BackgroundTasks,
    resume: bool = True,
    concurrent: int = 5,
    only_new: bool = True,
    max_pages: int = None,
):
    if not MFIRME_USER or not MFIRME_PASS:
        raise HTTPException(status_code=400, detail="MFIRME_USER / MFIRME_PASS nu sunt configurate în .env")
    if state.mfirme_crawl_progress["active"]:
        raise HTTPException(status_code=400, detail="Crawl deja în progres")

    checkpoint = _load_checkpoint() if resume else {"last_page": 0, "cuis_new": 0}
    start_page = checkpoint.get("last_page", 0) + 1 if resume and checkpoint.get("last_page") else 1

    state.mfirme_crawl_progress.update({
        "active": True, "total_pages": 0, "current_page": start_page,
        "cuis_found": 0, "cuis_new": checkpoint.get("cuis_new", 0),
        "cuis_skipped": 0, "errors": 0,
        "last_page_saved": checkpoint.get("last_page", 0),
        "logs": []
    })

    if resume and checkpoint.get("last_page"):
        state.add_mfirme_log(f"Reluare de la pagina {start_page} (checkpoint: {checkpoint.get('last_page')})")
    else:
        state.add_mfirme_log("Start crawl de la pagina 1")

    background_tasks.add_task(_run_crawl, start_page, concurrent, only_new, max_pages)
    return {"message": f"Crawl pornit de la pagina {start_page}", "checkpoint": checkpoint}


@router.post("/crawler/mfirme/stop")
async def stop_mfirme_crawl():
    state.mfirme_crawl_progress["active"] = False
    return {"message": "Stop requested — se va opri după pagina curentă"}


@router.delete("/crawler/mfirme/checkpoint")
async def clear_checkpoint():
    _clear_checkpoint()
    return {"message": "Checkpoint șters — next run starts from page 1"}


# ─── Crawler logic ────────────────────────────────────────────────────────────

async def _login(session: aiohttp.ClientSession) -> bool:
    """Login to mfirme.ro and return True if successful."""
    try:
        async with session.get(
            f"{LOGIN_URL}?next={MFIRME_URL}",
            headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)
        ) as r:
            html = await r.text()
            soup = BeautifulSoup(html, "html.parser")
            csrf_el = soup.find("input", {"name": "csrfmiddlewaretoken"})
            if not csrf_el:
                logger.error("[MFIRME] No CSRF token on login page")
                return False
            csrf_token = csrf_el["value"]

        async with session.post(
            f"{LOGIN_URL}?next={MFIRME_URL}",
            data={"csrfmiddlewaretoken": csrf_token, "username": MFIRME_USER, "password": MFIRME_PASS},
            headers={**HEADERS, "Referer": LOGIN_URL, "Content-Type": "application/x-www-form-urlencoded"},
            allow_redirects=True, timeout=aiohttp.ClientTimeout(total=20)
        ) as r:
            html = await r.text()
            if "company" in str(r.url) or "company" in html[:500]:
                return True
            if "login" in str(r.url).lower() or "parola" in html.lower() or "password" in html.lower():
                logger.error("[MFIRME] Login failed — wrong credentials?")
                return False
            return True
    except Exception as e:
        logger.error(f"[MFIRME] Login error: {e}")
        return False


def _parse_companies_from_page(html: str) -> list:
    """Extract CUI, name, nr_reg_com, stare from a company list page.
    
    Column layout (8 cells):
    [0] checkbox | [1] CUI (called 'ID') | [2] Denumire | [3] Nr.Reg.Com
    [4] Adresa | [5] Cod Postal | [6] Active | [7] Stare
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    companies = []
    for row in table.find_all("tr")[1:]:  # skip header
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        try:
            cui = cells[1].get_text(strip=True)   # Django "ID" = CUI
            name = cells[2].get_text(strip=True)   # Company name
            nr_reg = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            stare = cells[7].get_text(strip=True) if len(cells) > 7 else ""

            # Validate CUI: 4-10 digits
            if cui and re.match(r"^\d{4,10}$", cui.replace("RO", "").strip()):
                clean_cui = re.sub(r"[^\d]", "", cui)
                companies.append({
                    "cui": clean_cui,
                    "denumire": name[:500] if name else f"FIRMA {clean_cui}",
                    "nr_reg_com": nr_reg[:50] if nr_reg and nr_reg != "-" else None,
                    "stare": stare[:200] if stare else None,
                })
        except Exception:
            continue
    return companies


def _get_total_pages(html: str) -> int:
    """Extract total number of pages from pagination."""
    soup = BeautifulSoup(html, "html.parser")
    page_links = soup.find_all("a", href=re.compile(r"\?p=\d+"))
    nums = [int(re.search(r"\?p=(\d+)", a["href"]).group(1)) for a in page_links if re.search(r"\?p=(\d+)", a["href"])]
    return max(nums) if nums else 1


async def _fetch_page(session: aiohttp.ClientSession, page: int) -> Optional[str]:
    """Fetch one page, retry up to 3 times."""
    url = f"{MFIRME_URL}?p={page}"
    for attempt in range(3):
        try:
            async with session.get(
                url, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                if r.status == 200:
                    html = await r.text()
                    # Check if still logged in
                    if "login" in str(r.url).lower() or 'id="login-form"' in html:
                        return None  # Session expired
                    return html
                elif r.status == 429:
                    await asyncio.sleep(30)
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(5)
            else:
                logger.warning(f"[MFIRME] Page {page} error: {e}")
    return None


async def _save_companies_to_db(companies: list, only_new: bool) -> tuple:
    """Save companies to MongoDB. Returns (new_count, skipped_count)."""
    import mongo_db as mdb
    from pymongo import UpdateOne, InsertOne
    if not companies:
        return 0, 0

    cuis = [c["cui"] for c in companies]
    new_count = 0
    skipped = 0

    # Find existing CUIs in one query
    existing_docs = await mdb.firme_col.find(
        {"cui": {"$in": cuis}}, {"_id": 0, "cui": 1}
    ).to_list(None)
    existing_cuis = {d["cui"] for d in existing_docs}

    bulk_ops = []
    for company in companies:
        if company["cui"] in existing_cuis:
            skipped += 1
            continue
        firma_id = await mdb.next_id("firme")
        from helpers import normalize_company_name
        bulk_ops.append(InsertOne({
            "id": firma_id,
            "cui": company["cui"],
            "denumire": company["denumire"],
            "denumire_normalized": normalize_company_name(company["denumire"]),
            "cod_inregistrare": company.get("nr_reg_com"),
            "anaf_stare": company.get("stare"),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }))
        new_count += 1

    if bulk_ops:
        try:
            await mdb.firme_col.bulk_write(bulk_ops, ordered=False)
        except Exception as e:
            logger.error(f"[MFIRME] DB save error: {e}")

    return new_count, skipped


async def _run_crawl(start_page: int, concurrent: int, only_new: bool, max_pages: Optional[int]):
    """Main crawl loop with concurrent page fetching."""
    state.add_mfirme_log(f"Login la mfirme.ro...")

    jar = aiohttp.CookieJar()
    connector = aiohttp.TCPConnector(limit=concurrent + 2)

    async with aiohttp.ClientSession(cookie_jar=jar, connector=connector) as session:
        # Login
        if not await _login(session):
            state.add_mfirme_log("❌ Login eșuat — verifică credențialele în .env")
            state.mfirme_crawl_progress["active"] = False
            return

        state.add_mfirme_log("✅ Login reușit!")

        # Get first page to determine total pages
        first_html = await _fetch_page(session, start_page)
        if not first_html:
            state.add_mfirme_log("❌ Nu s-a putut încărca prima pagină")
            state.mfirme_crawl_progress["active"] = False
            return

        total_pages = _get_total_pages(first_html)
        if max_pages:
            total_pages = min(total_pages, start_page + max_pages - 1)

        state.mfirme_crawl_progress["total_pages"] = total_pages
        state.add_mfirme_log(f"Total pagini: {total_pages:,} (~{total_pages * 100:,} firme)")
        state.add_mfirme_log(f"Concurrent: {concurrent} pagini simultan")
        state.add_mfirme_log("─" * 40)

        # Process first page
        companies = _parse_companies_from_page(first_html)
        if companies:
            new_c, skip_c = await _save_companies_to_db(companies, only_new)
            state.mfirme_crawl_progress["cuis_found"] += len(companies)
            state.mfirme_crawl_progress["cuis_new"] += new_c
            state.mfirme_crawl_progress["cuis_skipped"] += skip_c

        # Process remaining pages in concurrent batches
        page = start_page + 1
        session_refreshes = 0

        while page <= total_pages and state.mfirme_crawl_progress["active"]:
            # Batch of concurrent pages
            batch_pages = list(range(page, min(page + concurrent, total_pages + 1)))
            tasks = [_fetch_page(session, p) for p in batch_pages]
            results = await asyncio.gather(*tasks)

            session_expired = False
            for p, html in zip(batch_pages, results):
                if html is None:
                    if session_expired is False:
                        # Try re-login once
                        state.add_mfirme_log(f"⚠️ Sesiune expirată la pagina {p}, re-login...")
                        if await _login(session):
                            session_expired = True
                            session_refreshes += 1
                            # Retry this page
                            html = await _fetch_page(session, p)
                        else:
                            state.add_mfirme_log("❌ Re-login eșuat, oprire")
                            state.mfirme_crawl_progress["active"] = False
                            break

                if html:
                    companies = _parse_companies_from_page(html)
                    if companies:
                        new_c, skip_c = await _save_companies_to_db(companies, only_new)
                        state.mfirme_crawl_progress["cuis_found"] += len(companies)
                        state.mfirme_crawl_progress["cuis_new"] += new_c
                        state.mfirme_crawl_progress["cuis_skipped"] += skip_c
                else:
                    state.mfirme_crawl_progress["errors"] += 1

            state.mfirme_crawl_progress["current_page"] = batch_pages[-1]

            # Log progress every 100 pages
            if batch_pages[-1] % 100 == 0:
                pct = batch_pages[-1] / total_pages * 100
                state.add_mfirme_log(
                    f"📊 Pagina {batch_pages[-1]:,}/{total_pages:,} ({pct:.1f}%) | "
                    f"Noi: {state.mfirme_crawl_progress['cuis_new']:,} | "
                    f"Skip: {state.mfirme_crawl_progress['cuis_skipped']:,}"
                )

            # Save checkpoint every 500 pages
            if batch_pages[-1] % 500 == 0:
                _save_checkpoint(batch_pages[-1], state.mfirme_crawl_progress["cuis_new"])
                state.add_mfirme_log(f"💾 Checkpoint salvat la pagina {batch_pages[-1]:,}")

            page += concurrent
            await asyncio.sleep(0.2)  # polite delay

        # Final
        _save_checkpoint(state.mfirme_crawl_progress["current_page"], state.mfirme_crawl_progress["cuis_new"])
        state.add_mfirme_log("─" * 40)
        state.add_mfirme_log(
            f"✅ Crawl {'complet' if not state.mfirme_crawl_progress['active'] else 'oprit'}: "
            f"{state.mfirme_crawl_progress['cuis_found']:,} găsite | "
            f"{state.mfirme_crawl_progress['cuis_new']:,} adăugate | "
            f"{state.mfirme_crawl_progress['cuis_skipped']:,} deja existente"
        )
        state.mfirme_crawl_progress["active"] = False
