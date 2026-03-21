"""
Firma/Dosar/DBFinal routes — MongoDB backend.
All PostgreSQL/SQLAlchemy replaced with Motor async MongoDB.
"""
import csv
import io
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

import mongo_db as mdb
import state
from constants import DOWNLOADS_DIR
from helpers import normalize_company_name, is_company
from schemas import FirmaCreate, FirmaUpdate

router = APIRouter()
logger = logging.getLogger(__name__)


# ─── CSV export ───────────────────────────────────────────────────────────────

@router.get("/db/firme/export")
async def export_firme_csv():
    firme = await mdb.firme_col.find({}, {"_id": 0, "id": 1, "cui": 1, "denumire": 1}).sort("denumire", 1).to_list(None)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['id', 'cui', 'denumire', 'dosare_count'])
    for firma in firme:
        cnt = await mdb.count_dosare_for_firma(firma['id'])
        writer.writerow([firma['id'], firma.get('cui', ''), firma.get('denumire', ''), cnt])
    output.seek(0)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"firme_export_{timestamp}.csv"
    filepath = DOWNLOADS_DIR / filename
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        f.write(output.getvalue())
    return FileResponse(filepath, filename=filename, media_type='text/csv')


# ─── Firme list & CRUD ────────────────────────────────────────────────────────

@router.get("/db/firme")
async def get_firme(skip: int = 0, limit: int = 100, search: str = None, judet: str = None):
    query = {}
    if search:
        s = search.strip()
        if s.isdigit():
            query["cui"] = {"$regex": s}
        else:
            query["denumire_normalized"] = {"$regex": normalize_company_name(s), "$options": "i"}
    if judet:
        query["judet"] = {"$regex": judet, "$options": "i"}

    total = await mdb.firme_col.count_documents(query)
    cursor = mdb.firme_col.find(query, {"_id": 0}).sort("id", -1).skip(skip).limit(limit)
    firme = await cursor.to_list(limit)

    result = []
    for f in firme:
        cnt = await mdb.dosare_col.count_documents({"firma_id": f["id"]})
        result.append({**f, "dosare_count": cnt})

    return {"total": total, "firme": result}


@router.get("/db/firme/{firma_id}")
async def get_firma(firma_id: int):
    firma = await mdb.get_firma_by_id(firma_id)
    if not firma:
        raise HTTPException(status_code=404, detail="Firma not found")
    dosare = await mdb.dosare_col.find({"firma_id": firma_id}, {"_id": 0}).to_list(None)
    return {**firma, "dosare_count": len(dosare), "dosare": dosare}


@router.put("/db/firme/{firma_id}")
async def update_firma(firma_id: int, update: FirmaUpdate):
    update_data = {}
    if update.cui is not None:
        update_data["cui"] = update.cui if update.cui else None
    if update.denumire is not None:
        update_data["denumire"] = update.denumire
        update_data["denumire_normalized"] = normalize_company_name(update.denumire)
    if update_data:
        update_data["updated_at"] = datetime.utcnow()
        await mdb.firme_col.update_one({"id": firma_id}, {"$set": update_data})
    firma = await mdb.get_firma_by_id(firma_id)
    if not firma:
        raise HTTPException(status_code=404, detail="Firma not found")
    return {"id": firma["id"], "cui": firma.get("cui"), "denumire": firma.get("denumire")}


@router.get("/db/dosare/{dosar_id}")
async def get_dosar(dosar_id: int):
    dosar = await mdb.dosare_col.find_one({"id": dosar_id}, {"_id": 0})
    if not dosar:
        raise HTTPException(status_code=404, detail="Dosar not found")
    firma = await mdb.get_firma_by_id(dosar.get("firma_id"))
    timeline = await mdb.timeline_col.find({"dosar_id": dosar_id}, {"_id": 0}).sort("data", 1).to_list(None)
    return {**dosar, "firma": firma, "timeline": timeline}


# ─── DB Stats & reconnect ─────────────────────────────────────────────────────

@router.get("/db/stats")
async def get_db_stats():
    try:
        firme_total = await mdb.firme_col.count_documents({})
        firme_with_cui = await mdb.firme_col.count_documents({"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}})
        dosare_total = await mdb.dosare_col.count_documents({})
        timeline_total = await mdb.timeline_col.count_documents({})
        return {
            "firme_total": firme_total,
            "firme_with_cui": firme_with_cui,
            "firme_without_cui": firme_total - firme_with_cui,
            "dosare_total": dosare_total,
            "timeline_events": timeline_total,
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


@router.post("/db/reconnect")
async def reconnect_database():
    try:
        await mdb.client.admin.command('ping')
        await mdb.create_indexes()
        return {"success": True, "message": "MongoDB connection OK", "postgres_available": True}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MongoDB error: {str(e)}")


@router.get("/db/status")
async def get_db_status():
    try:
        await mdb.client.admin.command('ping')
        return {"postgres_available": True, "mongodb": True}
    except Exception:
        return {"postgres_available": False, "mongodb": False}


@router.get("/db/import-progress")
async def get_import_progress():
    return state.import_progress


# ─── CSV import ───────────────────────────────────────────────────────────────

@router.post("/db/import-cui")
async def import_cui_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    has_header: bool = False,
    only_companies: bool = True
):
    """Upload CSV and process in background — no timeout issues for large files."""
    if state.import_progress.get("active"):
        raise HTTPException(status_code=400, detail="Import deja în curs")

    content = await file.read()
    filename = file.filename

    # Start background processing immediately — return to client
    state.import_progress.update({
        "active": True, "filename": filename,
        "total_rows": 0, "processed": 0,
        "created_new": 0, "updated": 0,
        "skipped_not_company": 0, "skipped_no_cui": 0,
        "last_update": datetime.utcnow().isoformat()
    })

    background_tasks.add_task(_process_import_csv, content, filename, has_header, only_companies)

    return {
        "message": f"Import pornit pentru {filename} ({len(content):,} bytes). Urmărește progresul.",
        "status": "running",
        "poll_url": "/api/db/import-progress"
    }


async def _process_import_csv(content: bytes, filename: str, has_header: bool, only_companies: bool):
    try:
        decoded = content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            decoded = content.decode('latin-1')
        except UnicodeDecodeError:
            decoded = content.decode('cp1252')

    lines = [line for line in decoded.strip().split('\n') if line.strip()]
    if not lines:
        state.import_progress["active"] = False
        return {"error": "File is empty"}

    first_line = lines[0]
    delimiter = '^'
    if '\t' in first_line:
        delimiter = '\t'
    elif ';' in first_line:
        delimiter = ';'
    elif ',' in first_line and '^' not in first_line:
        delimiter = ','

    start_row = 1 if has_header else 0
    state.import_progress.update({
        "active": True, "filename": filename,
        "total_rows": len(lines) - start_row, "processed": 0,
        "created_new": 0, "updated": 0, "skipped_not_company": 0,
        "skipped_no_cui": 0, "last_update": datetime.utcnow().isoformat()
    })

    results = {
        "total_rows": 0, "processed": 0, "skipped_not_company": 0,
        "created_new": 0, "already_exists": 0, "updated": 0,
        "skipped_no_cui": 0, "sample_created": [], "delimiter_detected": delimiter
    }

    def get_col(cols, idx, default=None):
        if idx < len(cols):
            v = cols[idx].strip() if cols[idx] else None
            return v if v else default
        return default

    batch = []
    existing_cuis = set()

    # Pre-load existing CUIs
    async for doc in mdb.firme_col.find({}, {"cui": 1, "_id": 0}):
        if doc.get("cui"):
            existing_cuis.add(doc["cui"])

    for line in lines[start_row:]:
        if not line.strip():
            continue
        results["total_rows"] += 1
        cols = line.split(delimiter)
        denumire = get_col(cols, 0)
        cui = get_col(cols, 1)
        cod_inregistrare = get_col(cols, 2)
        data_inregistrare = get_col(cols, 3)
        cod_onrc = get_col(cols, 4)
        forma_juridica = get_col(cols, 5)
        tara = get_col(cols, 6)
        judet = get_col(cols, 7)
        localitate = get_col(cols, 8)
        strada = get_col(cols, 9)
        numar = get_col(cols, 10)
        bloc = get_col(cols, 11)
        scara = get_col(cols, 12)
        etaj = get_col(cols, 13)
        apartament = get_col(cols, 14)
        cod_postal = get_col(cols, 15)
        detalii_adresa = ' | '.join(c.strip() for c in cols[16:] if c.strip()) if len(cols) > 16 else None

        if not denumire:
            continue
        if only_companies:
            excluded_forms = {'PF', 'PFA', 'II', 'IF', 'AF', 'CA'}
            if forma_juridica and forma_juridica.upper() in excluded_forms:
                results["skipped_not_company"] += 1
                continue
            if not is_company(denumire) and forma_juridica not in {'SRL', 'SA', 'SNC', 'SCS', 'RA'}:
                results["skipped_not_company"] += 1
                continue
        if cui == 'CUI' or denumire == 'DENUMIRE':
            continue
        if not cui or cui == '0' or len(cui) < 2:
            results["skipped_no_cui"] += 1
            continue

        results["processed"] += 1
        denumire_normalized = normalize_company_name(denumire)
        doc = {
            "cui": cui, "denumire": denumire, "denumire_normalized": denumire_normalized,
            "cod_inregistrare": cod_inregistrare, "data_inregistrare": data_inregistrare,
            "cod_onrc": cod_onrc, "forma_juridica": forma_juridica, "tara": tara,
            "judet": judet, "localitate": localitate, "strada": strada, "numar": numar,
            "bloc": bloc, "scara": scara, "etaj": etaj, "apartament": apartament,
            "cod_postal": cod_postal, "detalii_adresa": detalii_adresa,
            "updated_at": datetime.utcnow()
        }

        if cui in existing_cuis:
            batch.append({"filter": {"cui": cui}, "update": {"$set": doc}, "upsert": False, "type": "update"})
            results["updated"] += 1
        else:
            firma_id = await mdb.next_id("firme")
            doc.update({"id": firma_id, "created_at": datetime.utcnow()})
            batch.append({"filter": None, "doc": doc, "type": "insert"})
            existing_cuis.add(cui)
            results["created_new"] += 1
            if len(results["sample_created"]) < 5:
                results["sample_created"].append({"denumire": denumire[:50], "cui": cui})

        if len(batch) >= 2000:
            from pymongo import InsertOne, UpdateOne
            ops = []
            for b in batch:
                if b["type"] == "insert":
                    ops.append(InsertOne(b["doc"]))
                else:
                    ops.append(UpdateOne(b["filter"], b["update"]))
            if ops:
                await mdb.firme_col.bulk_write(ops, ordered=False)
            batch = []
            state.import_progress.update({
                "processed": results["processed"], "created_new": results["created_new"],
                "updated": results["updated"]
            })

    if batch:
        from pymongo import InsertOne, UpdateOne
        ops = []
        for b in batch:
            if b["type"] == "insert":
                ops.append(InsertOne(b["doc"]))
            else:
                ops.append(UpdateOne(b["filter"], b["update"]))
        if ops:
            await mdb.firme_col.bulk_write(ops, ordered=False)

    state.import_progress["active"] = False
    return results


# ─── Firma profile ────────────────────────────────────────────────────────────

@router.get("/db/firma/{firma_id}")
async def get_firma_profile(firma_id: int):
    firma = await mdb.get_firma_by_id(firma_id)
    if not firma:
        raise HTTPException(status_code=404, detail=f"Firma cu ID {firma_id} nu a fost găsită")

    bilanturi = await mdb.bilanturi_col.find(
        {"firma_id": firma_id}, {"_id": 0}
    ).sort("an", -1).to_list(None)

    dosare_total = await mdb.dosare_col.count_documents({"firma_id": firma_id})
    dosare_list = await mdb.dosare_col.find(
        {"firma_id": firma_id}, {"_id": 0}
    ).sort("data_dosar", -1).limit(20).to_list(20)

    return {
        "id": firma.get("id"),
        "basic_info": {
            "denumire": firma.get("denumire"),
            "cui": firma.get("cui"),
            "cod_inregistrare": firma.get("cod_inregistrare"),
            "data_inregistrare": firma.get("data_inregistrare"),
            "cod_onrc": firma.get("cod_onrc"),
            "forma_juridica": firma.get("forma_juridica"),
        },
        "adresa": {
            "judet": firma.get("judet"), "localitate": firma.get("localitate"),
            "strada": firma.get("strada"), "numar": firma.get("numar"),
            "bloc": firma.get("bloc"), "scara": firma.get("scara"),
            "etaj": firma.get("etaj"), "apartament": firma.get("apartament"),
            "cod_postal": firma.get("cod_postal"), "tara": firma.get("tara"),
        },
        "anaf_data": {k: v for k, v in firma.items() if k.startswith("anaf_")},
        "mfinante_data": {k: v for k, v in firma.items() if k.startswith("mf_")},
        "bilanturi_history": bilanturi,
        "dosare_summary": {
            "total": dosare_total,
            "recente": [{"numar_dosar": d.get("numar_dosar"), "institutie": d.get("institutie"),
                         "obiect": d.get("obiect"), "stadiu": d.get("stadiu"),
                         "data_dosar": d.get("data_dosar").strftime("%d.%m.%Y") if d.get("data_dosar") else None}
                        for d in dosare_list]
        },
        "metadata": {
            "created_at": firma.get("created_at").isoformat() if firma.get("created_at") else None,
            "updated_at": firma.get("updated_at").isoformat() if firma.get("updated_at") else None,
        }
    }


@router.get("/db/firma-by-cui/{cui}")
async def get_firma_by_cui(cui: str):
    firma = await mdb.get_firma_by_cui(cui)
    if not firma:
        raise HTTPException(status_code=404, detail=f"Firma cu CUI {cui} nu a fost găsită")
    return await get_firma_profile(firma["id"])


# ─── DB Final ─────────────────────────────────────────────────────────────────

@router.get("/dbfinal/stats")
async def get_dbfinal_stats():
    base = {"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}}

    pipeline = [
        {"$match": base},
        {"$group": {
            "_id": None,
            "total_cu_cui": {"$sum": 1},
            "sincronizate_anaf": {"$sum": {"$cond": [{"$eq": ["$anaf_sync_status", "found"]}, 1, 0]}},
            "sincronizate_mfinante": {"$sum": {"$cond": [{"$ne": [{"$ifNull": ["$mf_last_sync", None]}, None]}, 1, 0]}},
            "cu_date_bilant": {"$sum": {"$cond": [{"$ne": [{"$ifNull": ["$mf_cifra_afaceri", None]}, None]}, 1, 0]}},
            "active": {"$sum": {"$cond": [{"$regexMatch": {"input": {"$ifNull": ["$anaf_stare", ""]}, "regex": "^INREGISTRAT"}}, 1, 0]}},
            "active_fiscal": {"$sum": {"$cond": [
                {"$and": [
                    {"$regexMatch": {"input": {"$ifNull": ["$anaf_stare", ""]}, "regex": "^INREGISTRAT"}},
                    {"$ne": ["$anaf_inactiv", True]}
                ]}, 1, 0
            ]}},
            "active_dar_inactiv_fiscal": {"$sum": {"$cond": [
                {"$and": [
                    {"$regexMatch": {"input": {"$ifNull": ["$anaf_stare", ""]}, "regex": "^INREGISTRAT"}},
                    {"$eq": ["$anaf_inactiv", True]}
                ]}, 1, 0
            ]}},
            "radiate": {"$sum": {"$cond": [{"$regexMatch": {"input": {"$ifNull": ["$anaf_stare", ""]}, "regex": "^RADIERE"}}, 1, 0]}},
            "suspendate": {"$sum": {"$cond": [{"$regexMatch": {"input": {"$ifNull": ["$anaf_stare", ""]}, "regex": "^SUSPENDARE"}}, 1, 0]}},
            "transfer": {"$sum": {"$cond": [{"$regexMatch": {"input": {"$ifNull": ["$anaf_stare", ""]}, "regex": "^TRANSFER"}}, 1, 0]}},
            "dizolvare": {"$sum": {"$cond": [{"$regexMatch": {"input": {"$ifNull": ["$anaf_stare", ""]}, "regex": "^DIZOLVARE"}}, 1, 0]}},
            "reluare": {"$sum": {"$cond": [{"$regexMatch": {"input": {"$ifNull": ["$anaf_stare", ""]}, "regex": "^RELUARE"}}, 1, 0]}},
            "inactiv_anaf": {"$sum": {"$cond": [{"$eq": ["$anaf_inactiv", True]}, 1, 0]}},
            "platitori_tva": {"$sum": {"$cond": [{"$eq": ["$anaf_platitor_tva", True]}, 1, 0]}},
            "tva_incasare": {"$sum": {"$cond": [{"$eq": ["$anaf_tva_incasare", True]}, 1, 0]}},
            "split_tva": {"$sum": {"$cond": [{"$eq": ["$anaf_split_tva", True]}, 1, 0]}},
            "e_factura": {"$sum": {"$cond": [{"$eq": ["$anaf_e_factura", True]}, 1, 0]}},
            "nesincronizate": {"$sum": {"$cond": [
                {"$or": [
                    {"$eq": [{"$ifNull": ["$anaf_sync_status", None]}, None]},
                    {"$eq": ["$anaf_sync_status", ""]}
                ]}, 1, 0
            ]}},
        }}
    ]
    result = await mdb.firme_col.aggregate(pipeline).to_list(1)
    if result:
        r = result[0]
        r.pop("_id", None)
        r["db_available"] = True
        return r
    return {
        "total_cu_cui": 0, "sincronizate_anaf": 0, "sincronizate_mfinante": 0,
        "cu_date_bilant": 0, "active": 0, "active_fiscal": 0, "active_dar_inactiv_fiscal": 0,
        "radiate": 0, "suspendate": 0, "transfer": 0, "dizolvare": 0, "reluare": 0,
        "inactiv_anaf": 0, "platitori_tva": 0, "tva_incasare": 0, "split_tva": 0,
        "e_factura": 0, "nesincronizate": 0, "db_available": True
    }


@router.get("/dbfinal/firme")
async def get_dbfinal_firme(skip: int = 0, limit: int = 100, search: str = None,
                             judet: str = None, doar_active: bool = False, doar_cu_bilant: bool = False):
    query = {"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}}
    if search:
        query["$or"] = [
            {"denumire": {"$regex": search, "$options": "i"}},
            {"cui": {"$regex": search, "$options": "i"}}
        ]
    if judet:
        query["judet"] = {"$regex": judet, "$options": "i"}
    if doar_active:
        query["anaf_stare"] = {"$regex": "ACTIV", "$options": "i"}
        query["$nor"] = [{"anaf_stare": {"$regex": "INACTIV", "$options": "i"}},
                         {"anaf_stare": {"$regex": "RADIERE", "$options": "i"}}]
    if doar_cu_bilant:
        query["mf_cifra_afaceri"] = {"$ne": None}

    total = await mdb.firme_col.count_documents(query)
    firme = await mdb.firme_col.find(query, {"_id": 0}).sort("denumire", 1).skip(skip).limit(limit).to_list(limit)

    return {
        "firme": [{"id": f.get("id"), "denumire": f.get("denumire"), "cui": f.get("cui"),
                   "judet": f.get("judet"), "localitate": f.get("localitate"),
                   "stare": f.get("anaf_stare") or f.get("mf_stare"),
                   "cifra_afaceri": f.get("mf_cifra_afaceri"), "profit": f.get("mf_profit_net"),
                   "angajati": f.get("mf_numar_angajati"), "an_bilant": f.get("mf_an_bilant"),
                   "platitor_tva": f.get("anaf_platitor_tva"), "anaf_sync": f.get("anaf_last_sync") is not None,
                   "mf_sync": f.get("mf_last_sync") is not None} for f in firme],
        "total": total, "skip": skip, "limit": limit, "db_available": True
    }


@router.get("/dbfinal/export")
async def export_dbfinal_csv(search: str = None, judet: str = None,
                              doar_active: bool = False, doar_cu_bilant: bool = False):
    query = {"cui": {"$ne": None, "$exists": True, "$not": {"$in": [None, ""]}}}
    if search:
        query["$or"] = [{"denumire": {"$regex": search, "$options": "i"}},
                        {"cui": {"$regex": search, "$options": "i"}}]
    if judet:
        query["judet"] = {"$regex": judet, "$options": "i"}
    if doar_active:
        query["anaf_stare"] = {"$regex": "ACTIV", "$options": "i"}
    if doar_cu_bilant:
        query["mf_cifra_afaceri"] = {"$ne": None}

    firme = await mdb.firme_col.find(query, {"_id": 0}).sort("denumire", 1).to_list(None)
    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['CUI', 'Denumire', 'Forma Juridica', 'Judet', 'Localitate', 'Stare ANAF',
                     'Nr Reg Com', 'Cod CAEN', 'Platitor TVA', 'e-Factura', 'An Bilant',
                     'Cifra Afaceri', 'Profit Net', 'Nr Angajati', 'Capitaluri', 'Datorii'])
    for f in firme:
        writer.writerow([
            f.get('cui', ''), f.get('denumire', ''), f.get('forma_juridica', ''),
            f.get('judet', ''), f.get('localitate', ''), f.get('anaf_stare', ''),
            f.get('anaf_nr_reg_com', ''), f.get('anaf_cod_caen', ''),
            'DA' if f.get('anaf_platitor_tva') else 'NU',
            'DA' if f.get('anaf_e_factura') else 'NU',
            f.get('mf_an_bilant', ''),
            round(f['mf_cifra_afaceri']) if f.get('mf_cifra_afaceri') else '',
            round(f['mf_profit_net']) if f.get('mf_profit_net') else '',
            f.get('mf_numar_angajati', ''),
            round(f['mf_capitaluri_proprii']) if f.get('mf_capitaluri_proprii') else '',
            round(f['mf_datorii']) if f.get('mf_datorii') else '',
        ])
    output.seek(0)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dbfinal_export_{timestamp}.csv"
    filepath = DOWNLOADS_DIR / filename
    with open(filepath, 'w', encoding='utf-8-sig', newline='') as f_out:
        f_out.write(output.getvalue())
    return FileResponse(filepath, filename=filename, media_type='text/csv')


# ─── Debug ────────────────────────────────────────────────────────────────────

@router.get("/db/debug-connection")
async def debug_connection():
    try:
        await mdb.client.admin.command('ping')
        firme_count = await mdb.firme_col.count_documents({})
        return {"connected": True, "type": "MongoDB", "firme_count": firme_count}
    except Exception as e:
        return {"connected": False, "error": str(e)}
