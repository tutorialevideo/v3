"""
Firma/Dosar/DBFinal routes: list, get, update, profile, CSV import, dbfinal stats & list.
"""
import csv
import io
import logging
from datetime import datetime

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import func

import database
import state
from constants import DOWNLOADS_DIR
from helpers import normalize_company_name, is_company
from schemas import FirmaCreate, FirmaUpdate

router = APIRouter()
logger = logging.getLogger(__name__)


# ─── CSV export ───────────────────────────────────────────────────────────────

@router.get("/db/firme/export")
async def export_firme_csv():
    db = database.SessionLocal()
    Firma = database.Firma
    Dosar = database.Dosar
    try:
        firme = db.query(Firma).order_by(Firma.denumire).all()
        # Single aggregated query instead of N+1
        dosare_counts = dict(
            db.query(Dosar.firma_id, func.count(Dosar.id))
            .group_by(Dosar.firma_id).all()
        )
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['id', 'cui', 'denumire', 'dosare_count'])
        for firma in firme:
            writer.writerow([firma.id, firma.cui or '', firma.denumire, dosare_counts.get(firma.id, 0)])
        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"firme_export_{timestamp}.csv"
        filepath = DOWNLOADS_DIR / filename
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            f.write(output.getvalue())
        return FileResponse(filepath, filename=filename, media_type='text/csv',
                            headers={"Content-Disposition": f"attachment; filename={filename}"})
    finally:
        db.close()


# ─── Firme list & CRUD ────────────────────────────────────────────────────────

@router.get("/db/firme")
async def get_firme(skip: int = 0, limit: int = 100, search: str = None, judet: str = None):
    db = database.get_db_session()
    if db is None:
        return {"firme": [], "total": 0, "db_available": False}
    Firma = database.Firma
    Dosar = database.Dosar
    try:
        query = db.query(Firma)
        if search:
            s = search.strip()
            if s.isdigit():
                query = query.filter(Firma.cui.contains(s))
            else:
                query = query.filter(Firma.denumire_normalized.contains(normalize_company_name(s)))
        if judet:
            query = query.filter(Firma.judet.ilike(f"%{judet}%"))
        total = query.count()
        firme = query.order_by(Firma.id.desc()).offset(skip).limit(limit).all()
        # Single query for all dosare counts (no N+1)
        firma_ids = [f.id for f in firme]
        dosare_counts = {}
        if firma_ids:
            dosare_counts = dict(
                db.query(Dosar.firma_id, func.count(Dosar.id))
                .filter(Dosar.firma_id.in_(firma_ids))
                .group_by(Dosar.firma_id).all()
            )
        result = []
        for f in firme:
            result.append({
                "id": f.id, "cui": f.cui, "denumire": f.denumire,
                "cod_inregistrare": f.cod_inregistrare, "data_inregistrare": f.data_inregistrare,
                "forma_juridica": f.forma_juridica, "judet": f.judet, "localitate": f.localitate,
                "strada": f.strada, "numar": f.numar,
                "dosare_count": dosare_counts.get(f.id, 0),
                "created_at": f.created_at.isoformat() if f.created_at else None
            })
        return {"total": total, "firme": result}
    finally:
        db.close()


@router.get("/db/firme/{firma_id}")
async def get_firma(firma_id: int):
    db = database.SessionLocal()
    Firma = database.Firma
    Dosar = database.Dosar
    try:
        firma = db.query(Firma).filter(Firma.id == firma_id).first()
        if not firma:
            raise HTTPException(status_code=404, detail="Firma not found")
        dosare = db.query(Dosar).filter(Dosar.firma_id == firma_id).all()
        return {
            "id": firma.id, "cui": firma.cui, "denumire": firma.denumire,
            "cod_inregistrare": firma.cod_inregistrare, "data_inregistrare": firma.data_inregistrare,
            "cod_onrc": firma.cod_onrc, "forma_juridica": firma.forma_juridica,
            "tara": firma.tara, "judet": firma.judet, "localitate": firma.localitate,
            "strada": firma.strada, "numar": firma.numar, "bloc": firma.bloc,
            "scara": firma.scara, "etaj": firma.etaj, "apartament": firma.apartament,
            "cod_postal": firma.cod_postal, "detalii_adresa": firma.detalii_adresa,
            "created_at": firma.created_at.isoformat() if firma.created_at else None,
            "updated_at": firma.updated_at.isoformat() if firma.updated_at else None,
            "dosare_count": len(dosare),
            "dosare": [{
                "id": d.id, "numar_dosar": d.numar_dosar, "institutie": d.institutie,
                "obiect": d.obiect, "stadiu": d.stadiu,
                "data_dosar": d.data_dosar.isoformat() if d.data_dosar else None
            } for d in dosare]
        }
    finally:
        db.close()


@router.put("/db/firme/{firma_id}")
async def update_firma(firma_id: int, update: FirmaUpdate):
    db = database.SessionLocal()
    Firma = database.Firma
    try:
        firma = db.query(Firma).filter(Firma.id == firma_id).first()
        if not firma:
            raise HTTPException(status_code=404, detail="Firma not found")
        if update.cui is not None:
            firma.cui = update.cui if update.cui else None
        if update.denumire is not None:
            firma.denumire = update.denumire
            firma.denumire_normalized = normalize_company_name(update.denumire)
        firma.updated_at = datetime.utcnow()
        db.commit()
        return {"id": firma.id, "cui": firma.cui, "denumire": firma.denumire}
    finally:
        db.close()


@router.get("/db/dosare/{dosar_id}")
async def get_dosar(dosar_id: int):
    db = database.SessionLocal()
    Dosar = database.Dosar
    Firma = database.Firma
    TimelineEvent = database.TimelineEvent
    try:
        dosar = db.query(Dosar).filter(Dosar.id == dosar_id).first()
        if not dosar:
            raise HTTPException(status_code=404, detail="Dosar not found")
        timeline = db.query(TimelineEvent).filter(TimelineEvent.dosar_id == dosar_id).order_by(TimelineEvent.data).all()
        firma = db.query(Firma).filter(Firma.id == dosar.firma_id).first()
        return {
            "id": dosar.id, "numar_dosar": dosar.numar_dosar,
            "firma": {"id": firma.id, "cui": firma.cui, "denumire": firma.denumire} if firma else None,
            "institutie": dosar.institutie, "obiect": dosar.obiect, "stadiu": dosar.stadiu,
            "categorie": dosar.categorie, "materie": dosar.materie,
            "data_dosar": dosar.data_dosar.isoformat() if dosar.data_dosar else None,
            "timeline": [{"id": t.id, "tip": t.tip,
                          "data": t.data.isoformat() if t.data else None,
                          "descriere": t.descriere, "detalii": t.detalii} for t in timeline]
        }
    finally:
        db.close()


# ─── DB Stats & reconnect ─────────────────────────────────────────────────────

@router.get("/db/stats")
async def get_db_stats():
    db = database.get_db_session()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available. Try /api/db/reconnect")
    Firma = database.Firma
    Dosar = database.Dosar
    TimelineEvent = database.TimelineEvent
    try:
        firme_count = db.query(Firma).count()
        firme_with_cui = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '').count()
        dosare_count = db.query(Dosar).count()
        timeline_count = db.query(TimelineEvent).count()
        return {
            "firme_total": firme_count, "firme_with_cui": firme_with_cui,
            "firme_without_cui": firme_count - firme_with_cui,
            "dosare_total": dosare_count, "timeline_events": timeline_count
        }
    finally:
        db.close()


@router.post("/db/reconnect")
async def reconnect_database():
    success = database.init_postgres_connection(max_retries=3, retry_delay=2)
    if success:
        try:
            database.Base.metadata.create_all(bind=database.engine)
            database._migrate_schema()
        except Exception as e:
            logger.warning(f"Table/schema migration after reconnect: {e}")
        return {"success": True, "message": "Database connection established successfully", "postgres_available": True}
    raise HTTPException(status_code=503, detail="Could not connect to PostgreSQL.")


@router.get("/db/status")
async def get_db_status():
    return {
        "postgres_available": database.postgres_available,
        "session_local_exists": database.SessionLocal is not None,
        "engine_exists": database.engine is not None
    }


@router.get("/db/import-progress")
async def get_import_progress():
    return state.import_progress


# ─── CSV import ───────────────────────────────────────────────────────────────

@router.post("/db/import-cui")
async def import_cui_csv(file: UploadFile = File(...), has_header: bool = False, only_companies: bool = True):
    content = await file.read()
    try:
        decoded = content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            decoded = content.decode('latin-1')
        except UnicodeDecodeError:
            decoded = content.decode('cp1252')

    lines = [line for line in decoded.strip().split('\n') if line.strip()]
    if not lines:
        raise HTTPException(status_code=400, detail="File is empty")

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
        "active": True, "filename": file.filename,
        "total_rows": len(lines) - start_row, "processed": 0,
        "created_new": 0, "updated": 0, "skipped_not_company": 0,
        "skipped_no_cui": 0, "last_update": datetime.utcnow().isoformat()
    })

    db = database.SessionLocal()
    Firma = database.Firma
    results = {
        "total_rows": 0, "processed": 0, "skipped_not_company": 0,
        "created_new": 0, "already_exists": 0, "updated": 0,
        "skipped_no_cui": 0, "sample_created": [], "delimiter_detected": delimiter
    }
    existing_by_cui = {f.cui: f for f in db.query(Firma).all() if f.cui}

    def get_col(cols, idx, default=None):
        if idx < len(cols):
            v = cols[idx].strip() if cols[idx] else None
            return v if v else default
        return default

    try:
        batch_count = 0
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
                excluded_forms = {'PF', 'PFA', 'II', 'IF', 'AF', 'CA', 'FORMA_JURIDICA'}
                if forma_juridica and forma_juridica.upper() in excluded_forms:
                    results["skipped_not_company"] += 1
                    continue
                if not is_company(denumire) and forma_juridica not in {'SRL', 'SA', 'SNC', 'SCS', 'SCA', 'RA', 'GIE', 'SC', 'OCR', 'OCC', 'OCM', 'OC1', 'OC2'}:
                    results["skipped_not_company"] += 1
                    continue
            if cui == 'CUI' or denumire == 'DENUMIRE':
                continue
            if not cui or cui == '0' or len(cui) < 2:
                results["skipped_no_cui"] += 1
                continue

            results["processed"] += 1
            denumire_normalized = normalize_company_name(denumire)
            existing_firma = existing_by_cui.get(cui)

            if existing_firma:
                existing_firma.denumire = denumire
                existing_firma.denumire_normalized = denumire_normalized
                existing_firma.cod_inregistrare = cod_inregistrare
                existing_firma.data_inregistrare = data_inregistrare
                existing_firma.cod_onrc = cod_onrc
                existing_firma.forma_juridica = forma_juridica
                existing_firma.tara = tara
                existing_firma.judet = judet
                existing_firma.localitate = localitate
                existing_firma.strada = strada
                existing_firma.numar = numar
                existing_firma.bloc = bloc
                existing_firma.scara = scara
                existing_firma.etaj = etaj
                existing_firma.apartament = apartament
                existing_firma.cod_postal = cod_postal
                existing_firma.detalii_adresa = detalii_adresa
                existing_firma.updated_at = datetime.utcnow()
                results["updated"] += 1
            else:
                new_firma = Firma(
                    cui=cui, denumire=denumire, denumire_normalized=denumire_normalized,
                    cod_inregistrare=cod_inregistrare, data_inregistrare=data_inregistrare,
                    cod_onrc=cod_onrc, forma_juridica=forma_juridica, tara=tara, judet=judet,
                    localitate=localitate, strada=strada, numar=numar, bloc=bloc, scara=scara,
                    etaj=etaj, apartament=apartament, cod_postal=cod_postal, detalii_adresa=detalii_adresa
                )
                db.add(new_firma)
                existing_by_cui[cui] = new_firma
                results["created_new"] += 1
                if len(results["sample_created"]) < 5:
                    results["sample_created"].append({"denumire": denumire[:50], "cui": cui, "forma_juridica": forma_juridica, "judet": judet})

            batch_count += 1
            if batch_count >= 5000:
                db.commit()
                batch_count = 0
                state.import_progress.update({"processed": results["total_rows"], "created_new": results["created_new"], "updated": results["updated"]})

        db.commit()
        state.import_progress.update({"processed": results["total_rows"], "created_new": results["created_new"], "updated": results["updated"]})
    except Exception as e:
        db.rollback()
        state.import_progress["active"] = False
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    finally:
        db.close()
        state.import_progress["active"] = False
    return results


# ─── Firma profile ────────────────────────────────────────────────────────────

@router.get("/db/firma/{firma_id}")
async def get_firma_profile(firma_id: int):
    db = database.get_db_session()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    Firma = database.Firma
    Dosar = database.Dosar
    Bilant = database.Bilant
    try:
        firma = db.query(Firma).filter(Firma.id == firma_id).first()
        if not firma:
            raise HTTPException(status_code=404, detail=f"Firma cu ID {firma_id} nu a fost găsită")
        bilanturi = db.query(Bilant).filter(Bilant.firma_id == firma_id).order_by(Bilant.an.desc()).all()
        dosare_count = db.query(Dosar).filter(Dosar.firma_id == firma_id).count()
        dosare_list = db.query(Dosar).filter(Dosar.firma_id == firma_id).order_by(Dosar.data_dosar.desc()).limit(20).all()
        return {
            "id": firma.id,
            "basic_info": {
                "denumire": firma.denumire, "cui": firma.cui,
                "cod_inregistrare": firma.cod_inregistrare, "data_inregistrare": firma.data_inregistrare,
                "cod_onrc": firma.cod_onrc, "forma_juridica": firma.forma_juridica,
            },
            "adresa": {
                "judet": firma.judet, "localitate": firma.localitate, "strada": firma.strada,
                "numar": firma.numar, "bloc": firma.bloc, "scara": firma.scara,
                "etaj": firma.etaj, "apartament": firma.apartament,
                "cod_postal": firma.cod_postal, "tara": firma.tara, "detalii_adresa": firma.detalii_adresa,
            },
            "anaf_data": {
                "anaf_denumire": firma.anaf_denumire, "anaf_adresa": firma.anaf_adresa,
                "anaf_stare": firma.anaf_stare, "anaf_nr_reg_com": firma.anaf_nr_reg_com,
                "anaf_telefon": firma.anaf_telefon, "anaf_fax": firma.anaf_fax,
                "anaf_cod_postal": firma.anaf_cod_postal, "anaf_data_inregistrare": firma.anaf_data_inregistrare,
                "anaf_cod_caen": firma.anaf_cod_caen, "anaf_forma_juridica": firma.anaf_forma_juridica,
                "anaf_forma_organizare": firma.anaf_forma_organizare, "anaf_forma_proprietate": firma.anaf_forma_proprietate,
                "anaf_organ_fiscal": firma.anaf_organ_fiscal, "anaf_platitor_tva": firma.anaf_platitor_tva,
                "anaf_tva_incasare": firma.anaf_tva_incasare, "anaf_split_tva": firma.anaf_split_tva,
                "anaf_inactiv": firma.anaf_inactiv, "anaf_e_factura": firma.anaf_e_factura,
                "anaf_sediu_judet": firma.anaf_sediu_judet, "anaf_sediu_localitate": firma.anaf_sediu_localitate,
                "anaf_sediu_strada": firma.anaf_sediu_strada, "anaf_sediu_numar": firma.anaf_sediu_numar,
                "anaf_last_sync": firma.anaf_last_sync.isoformat() if firma.anaf_last_sync else None,
                "anaf_sync_status": firma.anaf_sync_status,
            },
            "mfinante_data": {
                "mf_denumire": firma.mf_denumire, "mf_judet": firma.mf_judet,
                "mf_nr_reg_com": firma.mf_nr_reg_com, "mf_stare": firma.mf_stare,
                "mf_platitor_tva": firma.mf_platitor_tva, "mf_impozit_profit": firma.mf_impozit_profit,
                "mf_impozit_micro": firma.mf_impozit_micro, "mf_an_bilant": firma.mf_an_bilant,
                "mf_cifra_afaceri": firma.mf_cifra_afaceri, "mf_venituri_totale": firma.mf_venituri_totale,
                "mf_cheltuieli_totale": firma.mf_cheltuieli_totale, "mf_profit_brut": firma.mf_profit_brut,
                "mf_pierdere_bruta": firma.mf_pierdere_bruta, "mf_profit_net": firma.mf_profit_net,
                "mf_pierdere_neta": firma.mf_pierdere_neta, "mf_numar_angajati": firma.mf_numar_angajati,
                "mf_active_imobilizate": firma.mf_active_imobilizate, "mf_active_circulante": firma.mf_active_circulante,
                "mf_capitaluri_proprii": firma.mf_capitaluri_proprii, "mf_datorii": firma.mf_datorii,
                "mf_ani_disponibili": firma.mf_ani_disponibili,
                "mf_last_sync": firma.mf_last_sync.isoformat() if firma.mf_last_sync else None,
                "mf_sync_status": firma.mf_sync_status,
            },
            "bilanturi_history": [
                {"an": b.an, "cifra_afaceri_neta": b.cifra_afaceri_neta, "venituri_totale": b.venituri_totale,
                 "cheltuieli_totale": b.cheltuieli_totale, "profit_brut": b.profit_brut, "pierdere_bruta": b.pierdere_bruta,
                 "profit_net": b.profit_net, "pierdere_neta": b.pierdere_neta, "numar_angajati": b.numar_angajati,
                 "active_imobilizate": b.active_imobilizate, "active_circulante": b.active_circulante,
                 "capitaluri_proprii": b.capitaluri_proprii, "datorii": b.datorii}
                for b in bilanturi
            ],
            "dosare_summary": {
                "total": dosare_count,
                "recente": [
                    {"numar_dosar": d.numar_dosar, "institutie": d.institutie, "obiect": d.obiect,
                     "stadiu": d.stadiu, "data_dosar": d.data_dosar.strftime("%d.%m.%Y") if d.data_dosar else None}
                    for d in dosare_list
                ]
            },
            "metadata": {
                "created_at": firma.created_at.isoformat() if firma.created_at else None,
                "updated_at": firma.updated_at.isoformat() if firma.updated_at else None,
            }
        }
    finally:
        db.close()


@router.get("/db/firma-by-cui/{cui}")
async def get_firma_by_cui(cui: str):
    db = database.get_db_session()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    Firma = database.Firma
    try:
        firma = db.query(Firma).filter(Firma.cui == cui).first()
        if not firma:
            raise HTTPException(status_code=404, detail=f"Firma cu CUI {cui} nu a fost găsită")
        firma_id = firma.id
    finally:
        db.close()
    return await get_firma_profile(firma_id)


# ─── DB Final ─────────────────────────────────────────────────────────────────

@router.get("/dbfinal/stats")
async def get_dbfinal_stats():
    db = database.get_db_session()
    if db is None:
        return {"total_cu_cui": 0, "sincronizate_anaf": 0, "sincronizate_mfinante": 0,
                "cu_date_bilant": 0, "active": 0, "db_available": False}
    Firma = database.Firma
    try:
        base = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '')
        return {
            "total_cu_cui": base.count(),
            "sincronizate_anaf": base.filter(Firma.anaf_last_sync.isnot(None)).count(),
            "sincronizate_mfinante": base.filter(Firma.mf_last_sync.isnot(None)).count(),
            "cu_date_bilant": base.filter(Firma.mf_cifra_afaceri.isnot(None)).count(),
            "active": base.filter(Firma.anaf_stare.ilike('%ACTIV%'), ~Firma.anaf_stare.ilike('%INACTIV%'), ~Firma.anaf_stare.ilike('%RADIERE%')).count(),
            "db_available": True
        }
    finally:
        db.close()


@router.get("/dbfinal/firme")
async def get_dbfinal_firme(skip: int = 0, limit: int = 100, search: str = None,
                             judet: str = None, doar_active: bool = False, doar_cu_bilant: bool = False):
    db = database.get_db_session()
    if db is None:
        return {"firme": [], "total": 0, "skip": skip, "limit": limit, "db_available": False}
    Firma = database.Firma
    try:
        query = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '')
        if search:
            query = query.filter((Firma.denumire.ilike(f"%{search}%")) | (Firma.cui.ilike(f"%{search}%")))
        if judet:
            query = query.filter(Firma.judet.ilike(f"%{judet}%"))
        if doar_active:
            query = query.filter(Firma.anaf_stare.ilike('%ACTIV%'), ~Firma.anaf_stare.ilike('%INACTIV%'), ~Firma.anaf_stare.ilike('%RADIERE%'))
        if doar_cu_bilant:
            query = query.filter(Firma.mf_cifra_afaceri.isnot(None))
        total = query.count()
        firme = query.order_by(Firma.denumire).offset(skip).limit(limit).all()
        return {
            "firme": [
                {"id": f.id, "denumire": f.denumire, "cui": f.cui, "judet": f.judet,
                 "localitate": f.localitate, "stare": f.anaf_stare or f.mf_stare,
                 "cifra_afaceri": f.mf_cifra_afaceri, "profit": f.mf_profit_net,
                 "angajati": f.mf_numar_angajati, "an_bilant": f.mf_an_bilant,
                 "platitor_tva": f.anaf_platitor_tva, "anaf_sync": f.anaf_last_sync is not None,
                 "mf_sync": f.mf_last_sync is not None}
                for f in firme
            ],
            "total": total, "skip": skip, "limit": limit, "db_available": True
        }
    finally:
        db.close()


@router.get("/dbfinal/export")
async def export_dbfinal_csv(
    search: str = None,
    judet: str = None,
    doar_active: bool = False,
    doar_cu_bilant: bool = False,
    format: str = "csv"
):
    """Export all DB Final firms (with CUI) as CSV with full financial data."""
    db = database.get_db_session()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")
    Firma = database.Firma
    try:
        query = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '')
        if search:
            query = query.filter((Firma.denumire.ilike(f"%{search}%")) | (Firma.cui.ilike(f"%{search}%")))
        if judet:
            query = query.filter(Firma.judet.ilike(f"%{judet}%"))
        if doar_active:
            query = query.filter(Firma.anaf_stare.ilike('%ACTIV%'), ~Firma.anaf_stare.ilike('%INACTIV%'), ~Firma.anaf_stare.ilike('%RADIERE%'))
        if doar_cu_bilant:
            query = query.filter(Firma.mf_cifra_afaceri.isnot(None))
        firme = query.order_by(Firma.denumire).all()

        output = io.StringIO()
        writer = csv.writer(output, delimiter=';')
        writer.writerow([
            'CUI', 'Denumire', 'Forma Juridica', 'Judet', 'Localitate', 'Strada', 'Nr',
            'Stare ANAF', 'Nr Reg Com', 'Cod CAEN',
            'Platitor TVA', 'e-Factura', 'Inactiv',
            'An Bilant', 'Cifra Afaceri (RON)', 'Venituri Totale (RON)',
            'Profit Brut (RON)', 'Profit Net (RON)', 'Pierdere Neta (RON)',
            'Nr Angajati', 'Active Imobilizate (RON)', 'Active Circulante (RON)',
            'Capitaluri Proprii (RON)', 'Datorii Totale (RON)',
            'Ani Disponibili MF', 'Sync ANAF', 'Sync MFinante'
        ])
        for f in firme:
            writer.writerow([
                f.cui or '', f.denumire or '', f.forma_juridica or '',
                f.judet or '', f.localitate or '', f.strada or '', f.numar or '',
                f.anaf_stare or f.mf_stare or '', f.anaf_nr_reg_com or '', f.anaf_cod_caen or '',
                'DA' if f.anaf_platitor_tva else 'NU',
                'DA' if f.anaf_e_factura else 'NU',
                'DA' if f.anaf_inactiv else 'NU',
                f.mf_an_bilant or '',
                round(f.mf_cifra_afaceri) if f.mf_cifra_afaceri else '',
                round(f.mf_venituri_totale) if f.mf_venituri_totale else '',
                round(f.mf_profit_brut) if f.mf_profit_brut else '',
                round(f.mf_profit_net) if f.mf_profit_net else '',
                round(f.mf_pierdere_neta) if f.mf_pierdere_neta else '',
                f.mf_numar_angajati or '',
                round(f.mf_active_imobilizate) if f.mf_active_imobilizate else '',
                round(f.mf_active_circulante) if f.mf_active_circulante else '',
                round(f.mf_capitaluri_proprii) if f.mf_capitaluri_proprii else '',
                round(f.mf_datorii) if f.mf_datorii else '',
                f.mf_ani_disponibili or '',
                'DA' if f.anaf_last_sync else 'NU',
                'DA' if f.mf_last_sync else 'NU',
            ])

        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dbfinal_export_{timestamp}.csv"
        filepath = DOWNLOADS_DIR / filename
        with open(filepath, 'w', encoding='utf-8-sig', newline='') as file:
            file.write(output.getvalue())

        return FileResponse(
            filepath, filename=filename, media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    finally:
        db.close()
