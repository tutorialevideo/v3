"""
MFinante routes: CAPTCHA management, session management, data fetch, sync, stats.
Bilanturi routes included here since they depend on the same session.
"""
import asyncio
import base64
import io
import logging
import os
import re
import time
import uuid
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import Response

import state
from constants import MFINANTE_URL

load_dotenv()

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
        raise HTTPException(status_code=400, detail="No CAPTCHA session. Call /mfinante/captcha/init first.")
    try:
        url = f"{MFINANTE_URL};jsessionid={state.captcha_session['jsessionid']}"
        form_data = {"cod": test_cui, "captcha": captcha_code, "method.vizualizare": "VIZUALIZARE"}
        
        jar = aiohttp.CookieJar()
        async with aiohttp.ClientSession(cookie_jar=jar) as session:
            async with session.post(
                url, data=form_data,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": MFINANTE_URL,
                    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
                },
                cookies=state.captcha_session.get("cookies", {}),
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True
            ) as response:
                html = await response.text()
                final_url = str(response.url)
                
                # Get updated session from cookies/URL after redirect
                new_jsessionid = state.captcha_session["jsessionid"]
                new_cookies = dict(state.captcha_session.get("cookies", {}))
                
                # Update cookies from response
                for cookie in jar:
                    new_cookies[cookie.key] = cookie.value
                    if cookie.key.upper() == "JSESSIONID":
                        new_jsessionid = cookie.value
                
                # Check jsessionid in final URL
                if "jsessionid=" in final_url:
                    new_jsessionid = final_url.split("jsessionid=")[1].split("?")[0].split(";")[0].split("&")[0]
                
                # WRONG CAPTCHA: page shows captcha form again
                is_captcha_page = ("kaptcha" in html.lower() or 
                                   "captcha" in html.lower() or
                                   "Cod de validare" in html)
                
                if is_captcha_page:
                    return {"success": False, "error": "CAPTCHA incorect. Încercați din nou.", "need_new_captcha": True}
                
                # SUCCESS: any non-captcha response means the session is valid
                # MFinante may show company data, error for wrong CUI, or redirect — all are OK
                state.captcha_session["jsessionid"] = new_jsessionid
                state.captcha_session["cookies"] = new_cookies
                state.mfinante_session["jsessionid"] = new_jsessionid
                state.mfinante_session["cookies"] = new_cookies
                state.mfinante_sync_progress["session_valid"] = True
                # Persist session to MongoDB so it survives backend restarts
                await _save_session_to_db()
                
                # Detect company data in response for confirmation message
                has_company_data = any(kw in html for kw in [
                    "Date de identificare", "Denumire", "AGENTUL ECONOMIC",
                    "codul fiscal", "CUI", "TVA", "infocodfiscal"
                ])
                
                msg = "CAPTCHA rezolvat! Sesiunea MFinante este activă."
                if has_company_data:
                    msg = "CAPTCHA rezolvat cu succes! Date companie găsite, sesiunea este validă."
                
                return {
                    "success": True,
                    "message": msg,
                    "session_valid": True,
                    "jsessionid": new_jsessionid[:20] + "..."
                }
    except Exception as e:
        logger.error(f"[MFINANTE] CAPTCHA solve error: {e}")
        raise HTTPException(status_code=500, detail=f"Error solving CAPTCHA: {str(e)}")


# ─── Session management ───────────────────────────────────────────────────────

async def _load_session_from_db():
    """Load persisted MFinante session from MongoDB on startup."""
    try:
        saved = await state.mongo_db.mfinante_session.find_one({}, {"_id": 0})
        if saved and saved.get("jsessionid"):
            state.mfinante_session["jsessionid"] = saved["jsessionid"]
            state.mfinante_session["cookies"] = saved.get("cookies", {})
            logger.info("[MFINANTE] Session loaded from MongoDB")
    except Exception as e:
        logger.warning(f"[MFINANTE] Could not load session from MongoDB: {e}")


async def _save_session_to_db():
    """Persist MFinante session to MongoDB."""
    try:
        await state.mongo_db.mfinante_session.replace_one(
            {},
            {"jsessionid": state.mfinante_session["jsessionid"],
             "cookies": state.mfinante_session.get("cookies", {})},
            upsert=True
        )
    except Exception as e:
        logger.warning(f"[MFINANTE] Could not save session to MongoDB: {e}")


@router.post("/mfinante/captcha/auto-solve")
async def auto_solve_captcha(test_cui: str = "14918042", max_attempts: int = 15):
    """
    Automatically solve MFinante CAPTCHA using local Tesseract OCR.
    No external API needed. Fast and free.
    Falls back to Gemini if Tesseract is unavailable.
    """
    # Check available OCR method
    ocr_method = "none"
    try:
        import pytesseract
        pytesseract.get_tesseract_version()
        ocr_method = "tesseract"
    except Exception:
        gemini_key = os.environ.get("GEMINI_API_KEY")
        if gemini_key:
            ocr_method = "gemini"
        else:
            raise HTTPException(status_code=500, detail="Niciun OCR disponibil. Instalează Tesseract sau configurează GEMINI_API_KEY.")

    logger.info(f"[CAPTCHA] Auto-solve using {ocr_method}")

    for attempt in range(1, max_attempts + 1):
        logger.info(f"[CAPTCHA] Auto-solve attempt {attempt}/{max_attempts}")

        try:
            jar = aiohttp.CookieJar(unsafe=True)
            headers_browser = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
            }

            async with aiohttp.ClientSession(cookie_jar=jar, headers=headers_browser) as session:
                # Step 1: Init session
                async with session.get(
                    MFINANTE_URL,
                    timeout=aiohttp.ClientTimeout(total=30),
                    allow_redirects=True
                ) as response:
                    await response.read()
                    jsessionid = None
                    final_url = str(response.url)
                    if "jsessionid=" in final_url:
                        jsessionid = final_url.split("jsessionid=")[1].split("?")[0].split(";")[0]
                    for cookie in jar:
                        if cookie.key.upper() == "JSESSIONID":
                            jsessionid = cookie.value

                if not jsessionid:
                    logger.warning(f"[CAPTCHA] Attempt {attempt}: No jsessionid")
                    continue

                await asyncio.sleep(1)

                # Step 2: Fetch CAPTCHA image (same session)
                captcha_url = f"https://mfinante.gov.ro/apps/kaptcha.jpg;jsessionid={jsessionid}"
                async with session.get(
                    captcha_url,
                    headers={**headers_browser, "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                             "Referer": MFINANTE_URL},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status != 200:
                        logger.warning(f"[CAPTCHA] Attempt {attempt}: Image HTTP {response.status}")
                        continue
                    image_bytes = await response.read()
                    if len(image_bytes) < 500:
                        logger.warning(f"[CAPTCHA] Attempt {attempt}: Image too small ({len(image_bytes)} bytes)")
                        continue

                image_b64 = base64.b64encode(image_bytes).decode("utf-8")
                image_mime = "image/jpeg"
                logger.info(f"[CAPTCHA] Attempt {attempt}: Got image ({len(image_bytes)} bytes)")

                # ── Read CAPTCHA text ──────────────────────────────────────────
                captcha_text = ""

                if ocr_method == "tesseract":
                    try:
                        import pytesseract
                        from PIL import Image, ImageEnhance, ImageFilter
                        import numpy as np
                        img_pil = Image.open(io.BytesIO(image_bytes)).convert('L')
                        # Scale 4x + contrast + sharpen
                        scaled = img_pil.resize((img_pil.width * 4, img_pil.height * 4), Image.LANCZOS)
                        sharp = ImageEnhance.Contrast(scaled).enhance(4.0)
                        sharp = sharp.filter(ImageFilter.SHARPEN).filter(ImageFilter.SHARPEN)
                        # Binary threshold
                        arr = np.array(sharp)
                        binary = Image.fromarray((arr > arr.mean()).astype('uint8') * 255)

                        cfg = '--psm 8 --oem 3 -c tessedit_char_whitelist=abcdefghijklmnopqrstuvwxyz0123456789'
                        # Try sharp first, then binary
                        for proc in [sharp, binary]:
                            text = pytesseract.image_to_string(proc, config=cfg).strip().replace(' ', '').replace('\n', '').lower()
                            if len(text) >= 2:
                                captcha_text = text
                                break
                        logger.info(f"[CAPTCHA] Attempt {attempt}: Tesseract read '{captcha_text}'")
                    except Exception as e:
                        logger.warning(f"[CAPTCHA] Tesseract error: {e}")

                elif ocr_method == "gemini":
                    gemini_key = os.environ.get("GEMINI_API_KEY", "")
                    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}"
                    payload = {
                        "contents": [{"parts": [
                            {"text": "What text is written in this image? Reply with only the characters, nothing else."},
                            {"inline_data": {"mime_type": image_mime, "data": image_b64}}
                        ]}],
                        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 50}
                    }
                    async with aiohttp.ClientSession() as gs:
                        async with gs.post(gemini_url, json=payload, headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=30)) as gr:
                            if gr.status == 200:
                                d = await gr.json()
                                parts = d.get("candidates", [{}])[0].get("content", {}).get("parts", [])
                                captcha_text = parts[0].get("text", "").strip().replace(" ", "").replace("\n", "") if parts else ""
                                logger.info(f"[CAPTCHA] Attempt {attempt}: Gemini read '{captcha_text}'")

                if not captcha_text or len(captcha_text) < 1:
                    logger.warning(f"[CAPTCHA] Attempt {attempt}: Empty response from Gemini")
                    continue

                # Step 4: Submit CAPTCHA (same session)
                await asyncio.sleep(0.5)
                url = f"{MFINANTE_URL};jsessionid={jsessionid}"
                form_data = {"cod": test_cui, "captcha": captcha_text, "method.vizualizare": "VIZUALIZARE"}
                async with session.post(
                    url, data=form_data,
                    headers={**headers_browser,
                             "Content-Type": "application/x-www-form-urlencoded",
                             "Referer": f"{MFINANTE_URL}?cod={test_cui}"},
                    timeout=aiohttp.ClientTimeout(total=30),
                    allow_redirects=True
                ) as response:
                    html = await response.text()

            is_captcha_page = "kaptcha" in html.lower() or "Cod de validare" in html

            if not is_captcha_page:
                cookies_dict = {c.key: c.value for c in jar}
                state.captcha_session["jsessionid"] = jsessionid
                state.captcha_session["cookies"] = cookies_dict
                state.mfinante_session["jsessionid"] = jsessionid
                state.mfinante_session["cookies"] = cookies_dict
                state.mfinante_sync_progress["session_valid"] = True
                await _save_session_to_db()
                logger.info(f"[CAPTCHA] Auto-solved on attempt {attempt}! Code: '{captcha_text}'")
                return {
                    "success": True,
                    "message": f"CAPTCHA rezolvat automat la încercarea {attempt}!",
                    "captcha_code": captcha_text,
                    "attempts": attempt,
                    "session_valid": True
                }
            else:
                logger.info(f"[CAPTCHA] Attempt {attempt}: Wrong code '{captcha_text}', retrying...")

        except Exception as e:
            logger.warning(f"[CAPTCHA] Attempt {attempt}: Error: {str(e)[:80]}")

        await asyncio.sleep(2)

    raise HTTPException(
        status_code=400,
        detail=f"Nu s-a putut rezolva CAPTCHA-ul automat după {max_attempts} încercări. Încearcă manual."
    )


@router.get("/mfinante/captcha/auto-status")
async def get_captcha_auto_status():
    gemini_key = bool(os.environ.get("GEMINI_API_KEY"))
    if not state.mfinante_session.get("jsessionid"):
        await _load_session_from_db()
    valid = state.mfinante_session.get("jsessionid") is not None
    return {
        "session_valid": valid,
        "auto_solve_available": gemini_key,
        "provider": "gemini-2.5-flash" if gemini_key else None,
        "message": "Sesiune activă" if valid else "Sesiune expirată — rulați auto-solve"
    }

@router.get("/mfinante/captcha/auto-status")
async def get_captcha_auto_status():
    gemini_key = bool(os.environ.get("GEMINI_API_KEY"))
    emergent_key = bool(os.environ.get("EMERGENT_LLM_KEY"))
    if not state.mfinante_session.get("jsessionid"):
        await _load_session_from_db()
    valid = state.mfinante_session.get("jsessionid") is not None
    return {
        "session_valid": valid,
        "auto_solve_available": gemini_key or emergent_key,
        "provider": "gemini" if gemini_key else ("openai" if emergent_key else None),
        "message": "Sesiune activă" if valid else "Sesiune expirată — rulați auto-solve"
    }
async def get_mfinante_session_status():
    # Try to load from DB if not in memory
    if not state.mfinante_session.get("jsessionid"):
        await _load_session_from_db()
    return {
        "session_valid": state.mfinante_session.get("jsessionid") is not None,
        "jsessionid": (state.mfinante_session.get("jsessionid", "")[:20] + "...") if state.mfinante_session.get("jsessionid") else None,
        "progress": state.mfinante_sync_progress
    }


@router.get("/mfinante/session-status")
async def get_mfinante_session_status():
    if not state.mfinante_session.get("jsessionid"):
        await _load_session_from_db()
    return {
        "session_valid": state.mfinante_session.get("jsessionid") is not None,
        "jsessionid": (state.mfinante_session.get("jsessionid", "")[:20] + "...") if state.mfinante_session.get("jsessionid") else None,
        "progress": state.mfinante_sync_progress
    }


@router.get("/mfinante/sync-progress")
async def get_mfinante_sync_progress():
    return {
        "session_valid": state.mfinante_session.get("jsessionid") is not None,
        "progress": state.mfinante_sync_progress
    }


@router.post("/mfinante/set-session")
async def set_mfinante_session(jsessionid: str, cookies: dict = None):
    state.mfinante_session["jsessionid"] = jsessionid
    state.mfinante_session["cookies"] = cookies or {}
    await _save_session_to_db()
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

def _clean_mf_text(text: str) -> str:
    """Clean MFinante HTML text: remove \r\n\t, collapse whitespace."""
    if not text:
        return text
    cleaned = re.sub(r'[\r\n\t]+', ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()


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
            value = _clean_mf_text(cols[1].get_text(separator=' ', strip=True))
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
            t = _clean_mf_text(opt.get_text(strip=True))
            if v and t:
                result["bilanturi_disponibili"].append({"an": t, "value": v})
    return result


def _parse_value(value_text: str):
    if not value_text or value_text.strip() in ('-', '', 'lei', '-lei'):
        return None
    clean = re.sub(r'[^\d.,\-]', '', value_text.strip())
    if clean in ('', '-', '0', '0.0'):
        return None
    if clean:
        try:
            result = float(clean.replace('.', '').replace(',', '.'))
            return result if result != 0.0 else None
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
    an_clean = an_value.replace("WEB_UU_AN", "") if "WEB_UU_AN" in an_value else an_value
    bilant = {
        "an": an_clean,
        "indicatori": {},
        "raw_labels": [],
        # Debug: collect first 2000 chars of HTML to understand structure
        "debug_html_snippet": html[:2000] if not bilant_has_data(soup) else None
    }
    
    # Try multiple HTML structures MFinante might use
    # Strategy 1: div.row > div.col-sm-6 pairs (Bootstrap grid)
    _parse_bilant_rows(soup, bilant, 'div', 'row', 'div', 'col-sm-6')
    
    # Strategy 2: table rows (some MFinante pages use tables)
    if not bilant["indicatori"]:
        _parse_bilant_tables(soup, bilant)
    
    # Strategy 3: any pair of elements with label-value pattern
    if not bilant["indicatori"]:
        _parse_bilant_generic(soup, bilant)
    
    # Remove debug snippet if data was found  
    if bilant["indicatori"]:
        bilant.pop("debug_html_snippet", None)
    
    return bilant


def bilant_has_data(soup) -> bool:
    """Check if soup has any row-col structure"""
    for row in soup.find_all('div', class_='row'):
        if row.find_all('div', class_='col-sm-6'):
            return True
    return bool(soup.find_all('tr'))


def _apply_bilant_label(label: str, value, ind: dict):
    """Map a label string to the correct indicator key — based on actual MFinante table structure."""
    if not label or value is None:
        return
    l = label.lower().strip()

    # ── Cifra de afaceri ─────────────────────────────────────────────────────
    if 'cifra' in l and 'afaceri' in l:
        ind.setdefault("cifra_afaceri_neta", value)
    # ── Venituri / Cheltuieli totale ──────────────────────────────────────────
    elif 'venituri' in l and 'total' in l:
        ind.setdefault("venituri_totale", value)
    elif 'cheltuieli' in l and 'total' in l:
        ind.setdefault("cheltuieli_totale", value)
    # ── Profit / Pierdere brut(ă) ─────────────────────────────────────────────
    # Handles: "Profitul sau pierderea brut(a)" header (empty), then "-Profit" / "-Pierdere" subrows
    elif 'profit' in l and 'brut' in l:
        ind.setdefault("profit_brut", value)
    elif 'pierdere' in l and 'brut' in l:
        ind.setdefault("pierdere_bruta", value)
    elif l.rstrip(':').strip() in ('-profit', '- profit', 'profit'):
        # "-Profit" subrow under "Profitul sau pierderea brut(a)"
        if 'profit_brut' not in ind:
            ind["profit_brut"] = value
        else:
            ind.setdefault("profit_net", value)
    elif l.rstrip(':').strip() in ('-pierdere', '- pierdere', 'pierdere'):
        if 'pierdere_bruta' not in ind:
            ind["pierdere_bruta"] = value
        else:
            ind.setdefault("pierdere_neta", value)
    # ── Profit / Pierdere net(ă) ──────────────────────────────────────────────
    elif 'profit' in l and ('net' in l or 'neta' in l):
        ind.setdefault("profit_net", value)
    elif 'pierdere' in l and ('net' in l or 'neta' in l):
        ind.setdefault("pierdere_neta", value)
    # ── Angajați ─────────────────────────────────────────────────────────────
    elif any(kw in l for kw in ['salariat', 'angajat', 'personal', 'numar mediu']):
        ind.setdefault("numar_angajati", int(value) if value else None)
    # ── Active ────────────────────────────────────────────────────────────────
    elif 'active' in l and 'imobilizat' in l:
        ind.setdefault("active_imobilizate", value)
    elif 'active' in l and 'circulant' in l:
        ind.setdefault("active_circulante", value)
    elif 'stocuri' in l:
        ind.setdefault("stocuri", value)
    elif 'creant' in l:
        ind.setdefault("creante", value)
    elif ('casa' in l or 'disponibil' in l) and ('banci' in l or 'numerar' in l or 'trezorerie' in l):
        ind.setdefault("casa_conturi_banci", value)
    elif 'cheltuieli' in l and 'avans' in l:
        ind.setdefault("cheltuieli_avans", value)
    # ── Capitaluri ────────────────────────────────────────────────────────────
    # "CAPITALURI - TOTAL, din care:" is the main capital total
    elif 'capitaluri' in l and 'total' in l:
        ind.setdefault("capitaluri_proprii", value)
    elif 'capital' in l and ('propri' in l or 'net' in l):
        ind.setdefault("capitaluri_proprii", value)
    elif 'capital' in l and ('subscris' in l or 'social' in l or 'varsat' in l):
        ind.setdefault("capital_subscris", value)
    elif 'patrimoniul' in l and 'regiei' in l:
        ind.setdefault("patrimoniul_regiei", value)
    # ── Pasive ────────────────────────────────────────────────────────────────
    elif 'provizioane' in l:
        ind.setdefault("provizioane", value)
    elif 'datorii' in l:
        ind.setdefault("datorii", value)
    elif 'venituri' in l and 'avans' in l:
        ind.setdefault("venituri_avans", value)
    elif 'repartizar' in l and 'profit' in l:
        ind.setdefault("repartizare_profit", value)


def _parse_bilant_rows(soup, bilant: dict, row_tag: str, row_cls: str, col_tag: str, col_cls: str):
    """Parse Bootstrap grid rows."""
    ind = bilant["indicatori"]
    for row in soup.find_all(row_tag, class_=row_cls):
        cols = row.find_all(col_tag, class_=col_cls)
        if len(cols) >= 2:
            label_raw = cols[0].get_text(separator=' ', strip=True)
            label = _clean_mf_text(label_raw)
            value_text = _clean_mf_text(cols[1].get_text(separator=' ', strip=True))
            value = _parse_value(value_text)
            if label and len(label) > 2:
                bilant["raw_labels"].append({"label": label[:80], "value": value_text[:30]})
            _apply_bilant_label(label, value, ind)


def _parse_bilant_tables(soup, bilant: dict):
    """Parse HTML tables for financial data."""
    ind = bilant["indicatori"]
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                label_raw = cells[0].get_text(separator=' ', strip=True)
                label = _clean_mf_text(label_raw)
                # Find numeric value in remaining cells
                for cell in cells[1:]:
                    value_text = _clean_mf_text(cell.get_text(separator=' ', strip=True))
                    value = _parse_value(value_text)
                    if value is not None:
                        if label and len(label) > 2:
                            bilant["raw_labels"].append({"label": label[:80], "value": value_text[:30]})
                        _apply_bilant_label(label, value, ind)
                        break


def _parse_bilant_generic(soup, bilant: dict):
    """Generic: collect ALL label-value pairs for debug."""
    # Find all text that looks like financial labels
    all_text = soup.get_text(separator='\n')
    bilant["raw_labels"].append({"label": "HTML_TEXT_SAMPLE", "value": all_text[:500]})


# ─── Test & fetch endpoints ───────────────────────────────────────────────────

@router.get("/mfinante/test/{cui}")
async def test_mfinante_cui(cui: str):
    if not state.mfinante_session.get("jsessionid"):
        await _load_session_from_db()
    if not state.mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session. Solve CAPTCHA first.")
    try:
        return await _fetch_mfinante_data(cui)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mfinante/bilant/{cui}/{an}")
async def get_mfinante_bilant(cui: str, an: str):
    if not state.mfinante_session.get("jsessionid"):
        await _load_session_from_db()
    if not state.mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session set")
    try:
        an_value = f"WEB_UU_AN{an}" if not an.startswith("WEB_") else an
        return await _fetch_mfinante_bilant(cui, an_value)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mfinante/bilant-debug/{cui}/{an}")
async def debug_mfinante_bilant_html(cui: str, an: str):
    """Debug endpoint: returns raw HTML from MFinante bilant page for structure analysis."""
    if not state.mfinante_session.get("jsessionid"):
        await _load_session_from_db()
    if not state.mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session set")
    try:
        an_value = f"WEB_UU_AN{an}" if not an.startswith("WEB_") else an
        url = f"{MFINANTE_URL};jsessionid={state.mfinante_session['jsessionid']}"
        data_form = {"cod": cui, "an": an_value, "method.bilant": "VIZUALIZARE"}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, data=data_form,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": f"{MFINANTE_URL}?cod={cui}",
                },
                cookies=state.mfinante_session.get("cookies", {}),
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                html = await response.text()
        soup = BeautifulSoup(html, 'html.parser')
        # Collect all structural info
        rows_with_cols = []
        for row in soup.find_all('div', class_='row'):
            cols = row.find_all('div', class_='col-sm-6')
            if cols:
                rows_with_cols.append([_clean_mf_text(c.get_text(separator=' ', strip=True))[:100] for c in cols])
        tables_text = []
        for table in soup.find_all('table'):
            for tr in table.find_all('tr')[:20]:
                cells = [_clean_mf_text(td.get_text(separator=' ', strip=True))[:80] for td in tr.find_all(['td','th'])]
                if cells:
                    tables_text.append(cells)
        return {
            "html_length": len(html),
            "is_captcha_page": "kaptcha" in html.lower(),
            "rows_with_cols_count": len(rows_with_cols),
            "rows_sample": rows_with_cols[:20],
            "tables_count": len(soup.find_all('table')),
            "tables_sample": tables_text[:20],
            "page_text_sample": soup.get_text(separator='\n')[:1000]
        }
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
    """Sync MFinante data — ONLY for ANAF-confirmed ACTIVE firms."""
    if not state.mfinante_session.get("jsessionid"):
        await _load_session_from_db()
    if not state.mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session. Solve CAPTCHA first.")
    if state.mfinante_sync_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync already in progress")
    state.mfinante_sync_progress.update({
        "active": True, "session_valid": True, "total_firms": 0,
        "processed": 0, "found": 0, "not_found": 0, "skipped": 0, "errors": 0,
        "last_update": datetime.utcnow().isoformat(), "last_cui": None, "logs": []
    })
    # only_anaf_active is always True — we never sync inactive/radiated firms
    background_tasks.add_task(_run_mfinante_sync, limit, only_without_bilant, True)
    return {"message": "MFinante sync started (doar firme active ANAF)", "status": "running"}


@router.get("/mfinante/sync-logs")
async def get_mfinante_sync_logs():
    """Get current MFinante sync logs and progress."""
    return {
        "active": state.mfinante_sync_progress["active"],
        "processed": state.mfinante_sync_progress["processed"],
        "total_firms": state.mfinante_sync_progress["total_firms"],
        "found": state.mfinante_sync_progress["found"],
        "not_found": state.mfinante_sync_progress.get("not_found", 0),
        "skipped": state.mfinante_sync_progress.get("skipped", 0),
        "errors": state.mfinante_sync_progress["errors"],
        "last_cui": state.mfinante_sync_progress["last_cui"],
        "session_valid": state.mfinante_sync_progress["session_valid"],
        "logs": state.mfinante_sync_progress["logs"][-80:]
    }


async def _run_mfinante_sync(limit: int, only_without_bilant: bool, only_anaf_active: bool = True):
    """MFinante sync — MongoDB backend."""
    import mongo_db as mdb
    from pymongo import UpdateOne

    # Build MongoDB query
    query = {"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}}
    if only_anaf_active:
        query["anaf_sync_status"] = "found"
        query["anaf_stare"] = {"$regex": "^INREGISTRAT"}
        query["anaf_inactiv"] = {"$ne": True}
        state.add_mfinante_log("Filtru: doar firme INREGISTRAT + activ fiscal")
    else:
        state.add_mfinante_log("Filtru: toate firmele cu CUI")

    if only_without_bilant:
        query["$and"] = query.get("$and", []) + [
            {"$or": [{"mf_cifra_afaceri": None}, {"mf_last_sync": None}]}
        ]
        state.add_mfinante_log("Filtru suplimentar: fara bilant salvat")

    firms = await mdb.firme_col.find(query, {"_id": 0}).sort("id", 1).to_list(None) if not limit else await mdb.firme_col.find(query, {"_id": 0}).sort("id", 1).limit(limit).to_list(limit)
    state.mfinante_sync_progress["total_firms"] = len(firms)
    state.add_mfinante_log(f"Total firme de procesat: {len(firms)}")

    try:
        for firma in firms:
            if not state.mfinante_sync_progress["active"]:
                state.add_mfinante_log("Oprire solicitata de utilizator.")
                break

            firma_id = firma["id"]
            cui = firma.get("cui", "")
            state.mfinante_sync_progress["last_cui"] = cui
            denumire_short = (firma.get("denumire") or firma.get("anaf_denumire") or cui)[:45]
            idx = state.mfinante_sync_progress["processed"] + 1

            try:
                data = await _fetch_mfinante_data(cui)

                if data.get("found"):
                    di = data.get("date_identificare", {})
                    df_tax = data.get("date_fiscale", {})
                    ani = [b["an"] for b in data.get("bilanturi_disponibili", [])]

                    firm_update = {
                        "mf_denumire": di.get("denumire"),
                        "mf_adresa": di.get("adresa"),
                        "mf_judet": di.get("judet"),
                        "mf_nr_reg_com": di.get("nr_reg_com"),
                        "mf_stare": di.get("stare"),
                        "mf_platitor_tva": df_tax.get("platitor_tva"),
                        "mf_tva_data": df_tax.get("tva_data"),
                        "mf_impozit_profit": df_tax.get("impozit_profit"),
                        "mf_impozit_micro": df_tax.get("micro_data"),
                        "mf_accize": df_tax.get("accize"),
                        "mf_cas_data": df_tax.get("cas_data"),
                        "mf_ani_disponibili": ",".join(ani) if ani else None,
                        "mf_sync_status": "found",
                    }

                    ani_str = ", ".join(ani) if ani else "niciun an"
                    state.add_mfinante_log(f"[{idx}/{len(firms)}] {cui} | {denumire_short} | ani: {ani_str}")

                    latest_bilant = None
                    bilanturi_salvate = 0
                    for bilant_info in data.get("bilanturi_disponibili", []):
                        try:
                            await asyncio.sleep(0.5)
                            bilant_data = await _fetch_mfinante_bilant(cui, bilant_info["value"])
                            indicatori = bilant_data.get("indicatori", {})
                            an = bilant_data.get("an", bilant_info["an"])
                            if indicatori:
                                bilant_doc = {
                                    "firma_id": firma_id, "an": an,
                                    **{k: v for k, v in indicatori.items()},
                                    "updated_at": datetime.utcnow()
                                }
                                existing = await mdb.get_bilant(firma_id, an)
                                if existing:
                                    await mdb.bilanturi_col.update_one(
                                        {"firma_id": firma_id, "an": an},
                                        {"$set": bilant_doc}
                                    )
                                else:
                                    bilant_id = await mdb.next_id("bilanturi")
                                    bilant_doc["id"] = bilant_id
                                    bilant_doc["created_at"] = datetime.utcnow()
                                    await mdb.bilanturi_col.insert_one(bilant_doc)
                                bilanturi_salvate += 1
                                if latest_bilant is None or an > latest_bilant["an"]:
                                    latest_bilant = {"an": an, "indicatori": indicatori}
                        except Exception as e:
                            state.add_mfinante_log(f"  !! Bilant {bilant_info['an']}: {str(e)[:50]}")

                    if bilanturi_salvate > 0:
                        ca = latest_bilant["indicatori"].get("cifra_afaceri_neta") if latest_bilant else None
                        ca_str = f" | CA: {ca:,.0f} RON" if ca else ""
                        state.add_mfinante_log(f"  -> {bilanturi_salvate} bilanturi salvate{ca_str}")

                    if latest_bilant:
                        ind = latest_bilant["indicatori"]
                        firm_update.update({
                            "mf_an_bilant": latest_bilant["an"],
                            "mf_cifra_afaceri": ind.get("cifra_afaceri_neta"),
                            "mf_venituri_totale": ind.get("venituri_totale"),
                            "mf_cheltuieli_totale": ind.get("cheltuieli_totale"),
                            "mf_profit_brut": ind.get("profit_brut"),
                            "mf_profit_net": ind.get("profit_net"),
                            "mf_numar_angajati": ind.get("numar_angajati"),
                            "mf_active_imobilizate": ind.get("active_imobilizate"),
                            "mf_active_circulante": ind.get("active_circulante"),
                            "mf_capitaluri_proprii": ind.get("capitaluri_proprii"),
                            "mf_datorii": ind.get("datorii"),
                        })

                    firm_update["mf_last_sync"] = datetime.utcnow()
                    await mdb.firme_col.update_one({"id": firma_id}, {"$set": firm_update})
                    state.mfinante_sync_progress["found"] += 1
                else:
                    await mdb.firme_col.update_one({"id": firma_id}, {"$set": {"mf_sync_status": "not_found"}})
                    state.mfinante_sync_progress["not_found"] = state.mfinante_sync_progress.get("not_found", 0) + 1
                    state.add_mfinante_log(f"[{idx}/{len(firms)}] {cui} | {denumire_short} | negasit")

            except Exception as e:
                error_msg = str(e)
                if "Session expired" in error_msg or "CAPTCHA" in error_msg:
                    state.mfinante_sync_progress["session_valid"] = False
                    state.add_mfinante_log("!! Sesiune expirata! Rezolvati CAPTCHA din nou.")
                    break
                await mdb.firme_col.update_one({"id": firma_id}, {"$set": {"mf_sync_status": "error"}})
                state.mfinante_sync_progress["errors"] += 1
                state.add_mfinante_log(f"[{idx}/{len(firms)}] {cui} | Eroare: {error_msg[:50]}")

            state.mfinante_sync_progress["processed"] += 1
            state.mfinante_sync_progress["last_update"] = datetime.utcnow().isoformat()
            await asyncio.sleep(2)

        state.add_mfinante_log(
            f"Sync finalizat: {state.mfinante_sync_progress['found']} gasite, "
            f"{state.mfinante_sync_progress.get('not_found', 0)} negasite, "
            f"{state.mfinante_sync_progress['errors']} erori"
        )

    except Exception as e:
        state.add_mfinante_log(f"Eroare generala: {str(e)[:60]}")
        logger.error(f"[MFINANTE] Sync error: {e}")
    finally:
        state.mfinante_sync_progress["active"] = False


@router.post("/mfinante/sync-stop")
async def stop_mfinante_sync():
    state.mfinante_sync_progress["active"] = False
    return {"message": "MFinante sync stop requested"}


@router.get("/mfinante/stats")
async def get_mfinante_stats():
    import mongo_db as mdb
    base = {"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}}
    active_q = {**base, "anaf_sync_status": "found",
                "anaf_stare": {"$regex": "^INREGISTRAT"},
                "anaf_inactiv": {"$ne": True}}
    total = await mdb.firme_col.count_documents(base)
    synced = await mdb.firme_col.count_documents({**base, "mf_last_sync": {"$ne": None}})
    with_ca = await mdb.firme_col.count_documents({**base, "mf_cifra_afaceri": {"$ne": None}})
    bilanturi = await mdb.bilanturi_col.count_documents({})
    active_anaf = await mdb.firme_col.count_documents(active_q)
    active_fara_bilant = await mdb.firme_col.count_documents({**active_q, "mf_last_sync": None})
    return {
        "total_firme": total,
        "synced_mfinante": synced,
        "not_synced": total - synced,
        "active_anaf_eligible": active_anaf,
        "active_fara_bilant": active_fara_bilant,
        "with_cifra_afaceri": with_ca,
        "total_bilanturi_istorice": bilanturi,
        "session_status": {
            "has_session": state.mfinante_session.get("jsessionid") is not None,
            "session_valid": state.mfinante_sync_progress.get("session_valid", False)
        },
        "db_available": True
    }


# ─── Bilanturi — MongoDB ──────────────────────────────────────────────────────

@router.get("/bilanturi/firma/{firma_id}")
async def get_bilanturi_firma(firma_id: int):
    import mongo_db as mdb
    bilanturi = await mdb.bilanturi_col.find(
        {"firma_id": firma_id}, {"_id": 0}
    ).sort("an", -1).to_list(None)
    return bilanturi


@router.get("/bilanturi/cui/{cui}")
async def get_bilanturi_by_cui(cui: str):
    import mongo_db as mdb
    firma = await mdb.get_firma_by_cui(cui)
    if not firma:
        raise HTTPException(status_code=404, detail=f"Firma cu CUI {cui} nu a fost găsită")
    bilanturi = await mdb.bilanturi_col.find(
        {"firma_id": firma["id"]}, {"_id": 0}
    ).sort("an", -1).to_list(None)
    return {
        "firma": {"id": firma["id"], "cui": firma.get("cui"),
                  "denumire": firma.get("denumire"),
                  "mf_denumire": firma.get("mf_denumire"),
                  "mf_stare": firma.get("mf_stare"),
                  "mf_ani_disponibili": firma.get("mf_ani_disponibili")},
        "bilanturi": bilanturi
    }


@router.get("/bilanturi/stats")
async def get_bilanturi_stats():
    import mongo_db as mdb
    from pymongo import DESCENDING
    total = await mdb.bilanturi_col.count_documents({})
    firme_cu = len(await mdb.bilanturi_col.distinct("firma_id"))
    pipeline = [{"$group": {"_id": "$an", "count": {"$sum": 1}}},
                {"$sort": {"_id": -1}}]
    by_year = []
    async for doc in mdb.bilanturi_col.aggregate(pipeline):
        by_year.append({"an": doc["_id"], "count": doc["count"]})
    return {"total_bilanturi": total, "firme_cu_bilanturi": firme_cu, "by_year": by_year}
