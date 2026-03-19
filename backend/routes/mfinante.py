"""
MFinante routes: CAPTCHA management, session management, data fetch, sync, stats.
Bilanturi routes included here since they depend on the same session.
"""
import asyncio
import logging
import re
import time
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import Response
from sqlalchemy import func

import state
import database
from constants import MFINANTE_URL

router = APIRouter()
logger = logging.getLogger(__name__)


# ─── CAPTCHA ──────────────────────────────────────────────────────────────────

@router.get("/mfinante/captcha/init")
async def init_mfinante_captcha():
    try:
        jar = aiohttp.CookieJar()
        async with aiohttp.ClientSession(cookie_jar=jar) as session:
            async with session.get(
                MFINANTE_URL,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
                },
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True
            ) as response:
                await response.text()
                jsessionid = None
                final_url = str(response.url)
                if "jsessionid=" in final_url:
                    jsessionid = final_url.split("jsessionid=")[1].split("?")[0].split(";")[0]
                cookies_dict = {}
                for cookie in jar:
                    cookies_dict[cookie.key] = cookie.value
                    if cookie.key.upper() == "JSESSIONID":
                        jsessionid = cookie.value
                if not jsessionid:
                    for key, val in response.headers.items():
                        if key.lower() == "set-cookie" and "jsessionid" in val.lower():
                            for p in val.split(";"):
                                if "jsessionid" in p.lower():
                                    jsessionid = p.split("=")[1].strip()
                                    break
                if not jsessionid:
                    raise HTTPException(status_code=500, detail="Could not obtain session from MFinante")
                state.captcha_session["jsessionid"] = jsessionid
                state.captcha_session["cookies"] = cookies_dict
                ts = int(time.time() * 1000)
                return {
                    "success": True,
                    "jsessionid": jsessionid,
                    "captcha_url": f"/api/mfinante/captcha/image?t={ts}",
                    "message": "CAPTCHA session initialized."
                }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[MFINANTE] CAPTCHA init error: {e}")
        raise HTTPException(status_code=500, detail=f"Error initializing CAPTCHA: {str(e)}")


@router.get("/mfinante/captcha/image")
async def get_mfinante_captcha_image():
    if not state.captcha_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No CAPTCHA session. Call /mfinante/captcha/init first.")
    try:
        captcha_url = f"https://mfinante.gov.ro/apps/kaptcha.jpg;jsessionid={state.captcha_session['jsessionid']}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                captcha_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                    "Referer": MFINANTE_URL,
                },
                cookies=state.captcha_session.get("cookies", {}),
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    raise HTTPException(status_code=500, detail="Could not fetch CAPTCHA image")
                image_data = await response.read()
                return Response(
                    content=image_data,
                    media_type="image/jpeg",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching CAPTCHA: {str(e)}")


@router.post("/mfinante/captcha/solve")
async def solve_mfinante_captcha(captcha_code: str, test_cui: str = "14918042"):
    if not state.captcha_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No CAPTCHA session.")
    try:
        url = f"{MFINANTE_URL};jsessionid={state.captcha_session['jsessionid']}"
        form_data = {"cod": test_cui, "captcha": captcha_code, "method.vizualizare": "VIZUALIZARE"}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, data=form_data,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": MFINANTE_URL,
                },
                cookies=state.captcha_session.get("cookies", {}),
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True
            ) as response:
                html = await response.text()
                if "Cod de validare" in html and "kaptcha" in html:
                    return {"success": False, "error": "CAPTCHA incorect. Încercați din nou.", "need_new_captcha": True}
                if "Date de identificare" in html or "Denumire" in html or "AGENTUL ECONOMIC" in html:
                    state.mfinante_session["jsessionid"] = state.captcha_session["jsessionid"]
                    state.mfinante_session["cookies"] = state.captcha_session.get("cookies", {})
                    state.mfinante_sync_progress["session_valid"] = True
                    return {
                        "success": True,
                        "message": "CAPTCHA rezolvat cu succes! Sesiunea a fost setată.",
                        "session_valid": True,
                        "jsessionid": state.captcha_session["jsessionid"][:20] + "..."
                    }
                return {"success": False, "error": "Răspuns neașteptat de la MFinante.", "need_new_captcha": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error solving CAPTCHA: {str(e)}")


# ─── Session management ───────────────────────────────────────────────────────

@router.get("/mfinante/session-status")
async def get_mfinante_session_status():
    return {
        "session_valid": state.mfinante_session.get("jsessionid") is not None,
        "jsessionid": (state.mfinante_session.get("jsessionid", "")[:20] + "...") if state.mfinante_session.get("jsessionid") else None,
        "progress": state.mfinante_sync_progress
    }


@router.post("/mfinante/set-session")
async def set_mfinante_session(jsessionid: str, cookies: dict = None):
    state.mfinante_session["jsessionid"] = jsessionid
    state.mfinante_session["cookies"] = cookies or {}
    valid = await _test_mfinante_session()
    return {
        "success": True,
        "session_set": True,
        "session_valid": valid,
        "message": "Session set. " + ("Valid!" if valid else "May be invalid, try CAPTCHA again.")
    }


async def _test_mfinante_session() -> bool:
    if not state.mfinante_session.get("jsessionid"):
        return False
    try:
        url = f"{MFINANTE_URL};jsessionid={state.mfinante_session['jsessionid']}?cod=14918042"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html"},
                cookies=state.mfinante_session.get("cookies", {}),
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True
            ) as response:
                text = await response.text()
                if "Cod de validare" in text and "kaptcha" in text:
                    return False
                return "Date de identificare" in text or "Denumire" in text
    except Exception:
        return False


# ─── Data fetch helpers ───────────────────────────────────────────────────────

async def _fetch_mfinante_data(cui: str) -> dict:
    url = f"{MFINANTE_URL};jsessionid={state.mfinante_session['jsessionid']}?cod={cui}"
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Referer": MFINANTE_URL,
            },
            cookies=state.mfinante_session.get("cookies", {}),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            html = await response.text()

    if "Cod de validare" in html and "kaptcha" in html:
        raise Exception("Session expired - CAPTCHA required")

    soup = BeautifulSoup(html, 'html.parser')
    result = {"cui": cui, "found": False, "date_identificare": {}, "date_fiscale": {}, "bilanturi_disponibili": []}

    if "AGENTUL ECONOMIC CU CODUL" not in html:
        return result
    result["found"] = True

    for row in soup.find_all('div', class_='row'):
        cols = row.find_all('div', class_='col-sm-6')
        if len(cols) >= 2:
            label = cols[0].get_text(strip=True).lower()
            value = cols[1].get_text(strip=True)
            di = result["date_identificare"]
            df = result["date_fiscale"]
            if 'denumire' in label:
                di["denumire"] = value
            elif 'adresa' in label and 'adresa' not in di:
                di["adresa"] = value
            elif 'judetul' in label:
                di["judet"] = value
            elif 'inmatriculare' in label or 'registrul' in label:
                di["nr_reg_com"] = value
            elif 'postal' in label:
                di["cod_postal"] = value
            elif 'telefon' in label:
                di["telefon"] = value
            elif 'stare societate' in label:
                di["stare"] = value
            elif 'taxa pe valoarea' in label or 'tva' in label.lower():
                df["tva_data"] = value
                df["platitor_tva"] = value != 'NU' and 'NU' not in value
            elif 'impozit pe profit' in label:
                df["impozit_profit"] = value
            elif 'microintreprinderi' in label:
                df["micro_data"] = value
            elif 'accize' in label:
                df["accize"] = value != 'NU'
            elif 'asigurari sociale' in label and 'sanatate' not in label:
                df["cas_data"] = value

    for select in soup.find_all('select', {'name': 'an'}):
        for opt in select.find_all('option'):
            v = opt.get('value', '')
            t = opt.get_text(strip=True)
            if v and t:
                result["bilanturi_disponibili"].append({"an": t, "value": v})
    return result


def _parse_value(value_text: str):
    if not value_text or value_text in ('-', ''):
        return None
    clean = re.sub(r'[^\d.,\-]', '', value_text)
    if clean:
        try:
            return float(clean.replace('.', '').replace(',', '.'))
        except Exception:
            return None
    return None


async def _fetch_mfinante_bilant(cui: str, an_value: str) -> dict:
    url = f"{MFINANTE_URL};jsessionid={state.mfinante_session['jsessionid']}"
    data = {"cod": cui, "an": an_value, "method.bilant": "VIZUALIZARE"}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, data=data,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": f"{MFINANTE_URL}?cod={cui}",
            },
            cookies=state.mfinante_session.get("cookies", {}),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            html = await response.text()

    if "Cod de validare" in html and "kaptcha" in html:
        raise Exception("Session expired - CAPTCHA required")

    soup = BeautifulSoup(html, 'html.parser')
    bilant = {
        "an": an_value.replace("WEB_UU_AN", "") if "WEB_UU_AN" in an_value else an_value,
        "indicatori": {}
    }
    for row in soup.find_all('div', class_='row'):
        cols = row.find_all('div', class_='col-sm-6')
        if len(cols) >= 2:
            label = cols[0].get_text(strip=True).lower()
            value = _parse_value(cols[1].get_text(strip=True))
            ind = bilant["indicatori"]
            if 'cifra' in label and 'afaceri' in label and 'neta' in label:
                ind["cifra_afaceri_neta"] = value
            elif 'venituri totale' in label:
                ind["venituri_totale"] = value
            elif 'cheltuieli totale' in label:
                ind["cheltuieli_totale"] = value
            elif 'profit brut' in label:
                ind["profit_brut"] = value
            elif 'pierdere brut' in label:
                ind["pierdere_bruta"] = value
            elif 'profit net' in label:
                ind["profit_net"] = value
            elif 'pierdere net' in label:
                ind["pierdere_neta"] = value
            elif 'numar mediu' in label and 'salariati' in label:
                ind["numar_angajati"] = int(value) if value else None
            elif 'active imobilizate' in label and 'total' not in label:
                ind["active_imobilizate"] = value
            elif 'active circulante' in label and 'total' not in label:
                ind["active_circulante"] = value
            elif 'stocuri' in label:
                ind["stocuri"] = value
            elif 'creante' in label:
                ind["creante"] = value
            elif 'casa' in label and 'banci' in label:
                ind["casa_conturi_banci"] = value
            elif 'cheltuieli' in label and 'avans' in label:
                ind["cheltuieli_avans"] = value
            elif 'capitaluri proprii' in label or 'capital propriu' in label:
                ind["capitaluri_proprii"] = value
            elif 'capital subscris' in label or 'capital social' in label:
                ind["capital_subscris"] = value
            elif 'patrimoniul regiei' in label:
                ind["patrimoniul_regiei"] = value
            elif 'provizioane' in label:
                ind["provizioane"] = value
            elif 'datorii' in label and 'total' not in label:
                ind["datorii"] = value
            elif 'venituri' in label and 'avans' in label:
                ind["venituri_avans"] = value
            elif 'repartizare' in label and 'profit' in label:
                ind["repartizare_profit"] = value
    return bilant


# ─── Test & fetch endpoints ───────────────────────────────────────────────────

@router.get("/mfinante/test/{cui}")
async def test_mfinante_cui(cui: str):
    if not state.mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session. Solve CAPTCHA first.")
    try:
        return await _fetch_mfinante_data(cui)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mfinante/bilant/{cui}/{an}")
async def get_mfinante_bilant(cui: str, an: str):
    if not state.mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session set")
    try:
        an_value = f"WEB_UU_AN{an}" if not an.startswith("WEB_") else an
        return await _fetch_mfinante_bilant(cui, an_value)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mfinante/full/{cui}")
async def get_mfinante_full(cui: str):
    if not state.mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session set")
    try:
        data = await _fetch_mfinante_data(cui)
        if data["found"] and data["bilanturi_disponibili"]:
            try:
                bilant = await _fetch_mfinante_bilant(cui, data["bilanturi_disponibili"][-1]["value"])
                data["bilant_recent"] = bilant
            except Exception as e:
                data["bilant_error"] = str(e)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Sync ─────────────────────────────────────────────────────────────────────

@router.post("/mfinante/sync")
async def start_mfinante_sync(
    background_tasks: BackgroundTasks,
    limit: int = 100,
    only_without_bilant: bool = True
):
    if not state.mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session. Solve CAPTCHA first.")
    if state.mfinante_sync_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync already in progress")
    state.mfinante_sync_progress.update({
        "active": True, "session_valid": True, "total_firms": 0,
        "processed": 0, "found": 0, "errors": 0,
        "last_update": datetime.utcnow().isoformat(), "last_cui": None
    })
    background_tasks.add_task(_run_mfinante_sync, limit, only_without_bilant)
    return {"message": "MFinante sync started", "status": "running"}


async def _run_mfinante_sync(limit: int, only_without_bilant: bool):
    db = database.SessionLocal()
    Firma = database.Firma
    Bilant = database.Bilant
    try:
        query = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '')
        if only_without_bilant:
            query = query.filter(
                (Firma.mf_cifra_afaceri.is_(None)) | (Firma.mf_last_sync.is_(None))
            )
        firms = query.limit(limit).all()
        state.mfinante_sync_progress["total_firms"] = len(firms)

        for firma in firms:
            if not state.mfinante_sync_progress["active"]:
                break
            state.mfinante_sync_progress["last_cui"] = firma.cui
            try:
                data = await _fetch_mfinante_data(firma.cui)
                if data.get("found"):
                    di = data.get("date_identificare", {})
                    df = data.get("date_fiscale", {})
                    firma.mf_denumire = di.get("denumire")
                    firma.mf_adresa = di.get("adresa")
                    firma.mf_judet = di.get("judet")
                    firma.mf_cod_postal = di.get("cod_postal")
                    firma.mf_telefon = di.get("telefon")
                    firma.mf_nr_reg_com = di.get("nr_reg_com")
                    firma.mf_stare = di.get("stare")
                    firma.mf_platitor_tva = df.get("platitor_tva")
                    firma.mf_tva_data = df.get("tva_data")
                    firma.mf_impozit_profit = df.get("impozit_profit")
                    firma.mf_impozit_micro = df.get("micro_data")
                    firma.mf_accize = df.get("accize")
                    firma.mf_cas_data = df.get("cas_data")
                    ani = [b["an"] for b in data.get("bilanturi_disponibili", [])]
                    firma.mf_ani_disponibili = ",".join(ani) if ani else None
                    latest_bilant = None
                    for bilant_info in data.get("bilanturi_disponibili", []):
                        try:
                            await asyncio.sleep(0.5)
                            bilant_data = await _fetch_mfinante_bilant(firma.cui, bilant_info["value"])
                            indicatori = bilant_data.get("indicatori", {})
                            an = bilant_data.get("an", bilant_info["an"])
                            if indicatori:
                                existing = db.query(Bilant).filter(
                                    Bilant.firma_id == firma.id, Bilant.an == an
                                ).first()
                                fields = {
                                    "cifra_afaceri_neta": indicatori.get("cifra_afaceri_neta"),
                                    "venituri_totale": indicatori.get("venituri_totale"),
                                    "cheltuieli_totale": indicatori.get("cheltuieli_totale"),
                                    "profit_brut": indicatori.get("profit_brut"),
                                    "pierdere_bruta": indicatori.get("pierdere_bruta"),
                                    "profit_net": indicatori.get("profit_net"),
                                    "pierdere_neta": indicatori.get("pierdere_neta"),
                                    "numar_angajati": indicatori.get("numar_angajati"),
                                    "active_imobilizate": indicatori.get("active_imobilizate"),
                                    "active_circulante": indicatori.get("active_circulante"),
                                    "stocuri": indicatori.get("stocuri"),
                                    "creante": indicatori.get("creante"),
                                    "casa_conturi_banci": indicatori.get("casa_conturi_banci"),
                                    "cheltuieli_avans": indicatori.get("cheltuieli_avans"),
                                    "capitaluri_proprii": indicatori.get("capitaluri_proprii"),
                                    "capital_subscris": indicatori.get("capital_subscris"),
                                    "patrimoniul_regiei": indicatori.get("patrimoniul_regiei"),
                                    "provizioane": indicatori.get("provizioane"),
                                    "datorii": indicatori.get("datorii"),
                                    "venituri_avans": indicatori.get("venituri_avans"),
                                    "repartizare_profit": indicatori.get("repartizare_profit"),
                                    "raw_data": indicatori
                                }
                                if existing:
                                    for k, v in fields.items():
                                        setattr(existing, k, v)
                                    existing.updated_at = datetime.utcnow()
                                else:
                                    db.add(Bilant(firma_id=firma.id, an=an, **fields))
                                if latest_bilant is None or an > latest_bilant["an"]:
                                    latest_bilant = {"an": an, "indicatori": indicatori}
                        except Exception as e:
                            logger.warning(f"[MFINANTE] Bilant error for CUI {firma.cui}: {e}")
                    if latest_bilant:
                        ind = latest_bilant["indicatori"]
                        firma.mf_an_bilant = latest_bilant["an"]
                        firma.mf_cifra_afaceri = ind.get("cifra_afaceri_neta")
                        firma.mf_venituri_totale = ind.get("venituri_totale")
                        firma.mf_cheltuieli_totale = ind.get("cheltuieli_totale")
                        firma.mf_profit_brut = ind.get("profit_brut")
                        firma.mf_pierdere_bruta = ind.get("pierdere_bruta")
                        firma.mf_profit_net = ind.get("profit_net")
                        firma.mf_pierdere_neta = ind.get("pierdere_neta")
                        firma.mf_numar_angajati = ind.get("numar_angajati")
                        firma.mf_active_imobilizate = ind.get("active_imobilizate")
                        firma.mf_active_circulante = ind.get("active_circulante")
                        firma.mf_capitaluri_proprii = ind.get("capitaluri_proprii")
                        firma.mf_datorii = ind.get("datorii")
                    firma.mf_last_sync = datetime.utcnow()
                    firma.mf_sync_status = "found"
                    state.mfinante_sync_progress["found"] += 1
                else:
                    firma.mf_sync_status = "not_found"
                db.commit()
            except Exception as e:
                error_msg = str(e)
                if "Session expired" in error_msg or "CAPTCHA" in error_msg:
                    state.mfinante_sync_progress["session_valid"] = False
                    break
                firma.mf_sync_status = "error"
                db.commit()
                state.mfinante_sync_progress["errors"] += 1
            state.mfinante_sync_progress["processed"] += 1
            state.mfinante_sync_progress["last_update"] = datetime.utcnow().isoformat()
            await asyncio.sleep(2)
    except Exception as e:
        logger.error(f"[MFINANTE] Sync error: {e}")
    finally:
        db.close()
        state.mfinante_sync_progress["active"] = False


@router.post("/mfinante/sync-stop")
async def stop_mfinante_sync():
    state.mfinante_sync_progress["active"] = False
    return {"message": "MFinante sync stop requested"}


@router.get("/mfinante/stats")
async def get_mfinante_stats():
    db = database.get_db_session()
    if db is None:
        return {
            "total_firme": 0, "synced_mfinante": 0, "not_synced": 0,
            "with_cifra_afaceri": 0, "total_bilanturi_istorice": 0,
            "session_status": {
                "has_session": state.mfinante_session.get("jsessionid") is not None,
                "session_valid": state.mfinante_sync_progress.get("session_valid", False)
            },
            "db_available": False
        }
    try:
        Firma = database.Firma
        Bilant = database.Bilant
        return {
            "total_firme": db.query(Firma).filter(Firma.cui.isnot(None)).count(),
            "synced_mfinante": db.query(Firma).filter(Firma.mf_last_sync.isnot(None)).count(),
            "not_synced": db.query(Firma).filter(Firma.cui.isnot(None)).count() - db.query(Firma).filter(Firma.mf_last_sync.isnot(None)).count(),
            "with_cifra_afaceri": db.query(Firma).filter(Firma.mf_cifra_afaceri.isnot(None)).count(),
            "total_bilanturi_istorice": db.query(Bilant).count(),
            "session_status": {
                "has_session": state.mfinante_session.get("jsessionid") is not None,
                "session_valid": state.mfinante_sync_progress.get("session_valid", False)
            },
            "db_available": True
        }
    finally:
        db.close()


# ─── Bilanturi ────────────────────────────────────────────────────────────────

@router.get("/bilanturi/firma/{firma_id}")
async def get_bilanturi_firma(firma_id: int):
    db = database.SessionLocal()
    Bilant = database.Bilant
    try:
        bilanturi = db.query(Bilant).filter(Bilant.firma_id == firma_id).order_by(Bilant.an.desc()).all()
        return [
            {
                "id": b.id, "an": b.an,
                "cifra_afaceri_neta": b.cifra_afaceri_neta, "venituri_totale": b.venituri_totale,
                "cheltuieli_totale": b.cheltuieli_totale, "profit_brut": b.profit_brut,
                "pierdere_bruta": b.pierdere_bruta, "profit_net": b.profit_net,
                "pierdere_neta": b.pierdere_neta, "numar_angajati": b.numar_angajati,
                "active_imobilizate": b.active_imobilizate, "active_circulante": b.active_circulante,
                "stocuri": b.stocuri, "creante": b.creante, "casa_conturi_banci": b.casa_conturi_banci,
                "capitaluri_proprii": b.capitaluri_proprii, "capital_subscris": b.capital_subscris,
                "datorii": b.datorii, "created_at": b.created_at.isoformat() if b.created_at else None
            }
            for b in bilanturi
        ]
    finally:
        db.close()


@router.get("/bilanturi/cui/{cui}")
async def get_bilanturi_by_cui(cui: str):
    db = database.SessionLocal()
    Firma = database.Firma
    Bilant = database.Bilant
    try:
        firma = db.query(Firma).filter(Firma.cui == cui).first()
        if not firma:
            raise HTTPException(status_code=404, detail=f"Firma cu CUI {cui} nu a fost găsită")
        bilanturi = db.query(Bilant).filter(Bilant.firma_id == firma.id).order_by(Bilant.an.desc()).all()
        return {
            "firma": {"id": firma.id, "cui": firma.cui, "denumire": firma.denumire,
                      "mf_denumire": firma.mf_denumire, "mf_stare": firma.mf_stare,
                      "mf_ani_disponibili": firma.mf_ani_disponibili},
            "bilanturi": [
                {"id": b.id, "an": b.an, "cifra_afaceri_neta": b.cifra_afaceri_neta,
                 "venituri_totale": b.venituri_totale, "profit_net": b.profit_net,
                 "pierdere_neta": b.pierdere_neta, "numar_angajati": b.numar_angajati,
                 "capitaluri_proprii": b.capitaluri_proprii, "datorii": b.datorii}
                for b in bilanturi
            ]
        }
    finally:
        db.close()


@router.get("/bilanturi/stats")
async def get_bilanturi_stats():
    db = database.SessionLocal()
    Bilant = database.Bilant
    try:
        total = db.query(Bilant).count()
        firme_cu_bilanturi = db.query(Bilant.firma_id).distinct().count()
        by_year = db.query(Bilant.an, func.count(Bilant.id).label('count')).group_by(Bilant.an).order_by(Bilant.an.desc()).all()
        return {
            "total_bilanturi": total,
            "firme_cu_bilanturi": firme_cu_bilanturi,
            "by_year": [{"an": r.an, "count": r.count} for r in by_year]
        }
    finally:
        db.close()
