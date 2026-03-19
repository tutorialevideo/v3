"""
Database diagnostics routes: overview, duplicates, cleanup, optimize, migrate schema, indexes.
"""
import logging

from fastapi import APIRouter, HTTPException

import database

router = APIRouter()
logger = logging.getLogger(__name__)

# Note: diagnostics use the 'database' async connection (databases library) for raw SQL.


@router.get("/diagnostics/overview")
async def get_diagnostics_overview():
    try:
        def safe_cnt(res):
            return int(res["cnt"]) if res else 0

        try:
            firme_cnt = safe_cnt(await database.database.fetch_one("SELECT COUNT(*) as cnt FROM firme"))
        except Exception:
            firme_cnt = 0
        try:
            dosare_cnt = safe_cnt(await database.database.fetch_one("SELECT COUNT(*) as cnt FROM dosare"))
        except Exception:
            dosare_cnt = 0
        try:
            timeline_cnt = safe_cnt(await database.database.fetch_one("SELECT COUNT(*) as cnt FROM timeline_events"))
        except Exception:
            timeline_cnt = 0

        try:
            table_sizes = await database.database.fetch_all("""
                SELECT relname as table_name,
                       pg_size_pretty(pg_total_relation_size(relid)) as total_size,
                       pg_total_relation_size(relid) as size_bytes
                FROM pg_catalog.pg_statio_user_tables
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(relid) DESC
            """)
            table_sizes_list = [{"table": r["table_name"], "size": r["total_size"], "bytes": int(r["size_bytes"])} for r in table_sizes]
        except Exception:
            table_sizes_list = []

        try:
            dup_denumiri = safe_cnt(await database.database.fetch_one("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT denumire_normalized FROM firme GROUP BY denumire_normalized HAVING COUNT(*) > 1
                ) as dupes
            """))
        except Exception:
            dup_denumiri = 0

        try:
            dup_cui = safe_cnt(await database.database.fetch_one("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT cui FROM firme WHERE cui IS NOT NULL AND cui != '' GROUP BY cui HAVING COUNT(*) > 1
                ) as dupes
            """))
        except Exception:
            dup_cui = 0

        try:
            no_cui_cnt = safe_cnt(await database.database.fetch_one("SELECT COUNT(*) as cnt FROM firme WHERE cui IS NULL OR cui = ''"))
        except Exception:
            no_cui_cnt = 0

        try:
            orphaned_cnt = safe_cnt(await database.database.fetch_one("SELECT COUNT(*) as cnt FROM dosare WHERE firma_id NOT IN (SELECT id FROM firme)"))
        except Exception:
            orphaned_cnt = 0

        return {
            "counts": {"firme": firme_cnt, "dosare": dosare_cnt, "timeline_events": timeline_cnt},
            "table_sizes": table_sizes_list,
            "issues": {
                "duplicate_denumiri": dup_denumiri, "duplicate_cui": dup_cui,
                "firme_without_cui": no_cui_cnt, "orphaned_dosare": orphaned_cnt
            }
        }
    except Exception as e:
        logger.error(f"Diagnostics overview error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diagnostics/duplicates/denumire")
async def get_duplicate_denumiri(limit: int = 50):
    try:
        duplicates = await database.database.fetch_all(f"""
            SELECT denumire_normalized, COUNT(*) as count,
                   array_agg(id ORDER BY id) as ids,
                   array_agg(denumire ORDER BY id) as denumiri,
                   array_agg(COALESCE(cui, '') ORDER BY id) as cui_list
            FROM firme GROUP BY denumire_normalized HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC LIMIT {limit}
        """)
        return [{"denumire_normalized": r["denumire_normalized"], "count": r["count"],
                 "ids": list(r["ids"]) if r["ids"] else [],
                 "denumiri": list(r["denumiri"]) if r["denumiri"] else [],
                 "cui_list": list(r["cui_list"]) if r["cui_list"] else []}
                for r in duplicates]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diagnostics/duplicates/cui")
async def get_duplicate_cui(limit: int = 50):
    try:
        duplicates = await database.database.fetch_all(f"""
            SELECT cui, COUNT(*) as count,
                   array_agg(id ORDER BY id) as ids,
                   array_agg(denumire ORDER BY id) as denumiri
            FROM firme WHERE cui IS NOT NULL AND cui != ''
            GROUP BY cui HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC LIMIT {limit}
        """)
        return [{"cui": r["cui"], "count": r["count"],
                 "ids": list(r["ids"]) if r["ids"] else [],
                 "denumiri": list(r["denumiri"]) if r["denumiri"] else []}
                for r in duplicates]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/cleanup/duplicates-denumire")
async def cleanup_duplicate_denumiri():
    try:
        count_query = await database.database.fetch_one("""
            SELECT COUNT(*) as cnt FROM firme f1
            WHERE EXISTS (SELECT 1 FROM firme f2 WHERE f2.denumire_normalized = f1.denumire_normalized
                AND ((f2.cui IS NOT NULL AND f2.cui != '' AND (f1.cui IS NULL OR f1.cui = ''))
                    OR (f2.id < f1.id AND (f2.cui IS NOT NULL OR f1.cui IS NULL))))
        """)
        await database.database.execute("""
            DELETE FROM firme WHERE id IN (
                SELECT f1.id FROM firme f1 WHERE EXISTS (
                    SELECT 1 FROM firme f2 WHERE f2.denumire_normalized = f1.denumire_normalized
                    AND f2.id < f1.id AND ((f2.cui IS NOT NULL AND f2.cui != '') OR (f1.cui IS NULL OR f1.cui = ''))
                )
            )
        """)
        await database.database.execute("VACUUM ANALYZE firme")
        deleted = int(count_query["cnt"]) if count_query else 0
        return {"success": True, "deleted_count": deleted, "message": f"Deleted {deleted} duplicate entries"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/cleanup/duplicates-cui")
async def cleanup_duplicate_cui():
    try:
        count_query = await database.database.fetch_one("""
            SELECT COUNT(*) as cnt FROM firme f1 WHERE cui IS NOT NULL AND cui != ''
            AND EXISTS (SELECT 1 FROM firme f2 WHERE f2.cui = f1.cui AND f2.id < f1.id)
        """)
        await database.database.execute("""
            DELETE FROM firme WHERE id IN (
                SELECT f1.id FROM firme f1 WHERE f1.cui IS NOT NULL AND f1.cui != ''
                AND EXISTS (SELECT 1 FROM firme f2 WHERE f2.cui = f1.cui AND f2.id < f1.id)
            )
        """)
        await database.database.execute("VACUUM ANALYZE firme")
        deleted = int(count_query["cnt"]) if count_query else 0
        return {"success": True, "deleted_count": deleted, "message": f"Deleted {deleted} duplicate CUI entries"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/cleanup/orphaned-dosare")
async def cleanup_orphaned_dosare():
    try:
        count_query = await database.database.fetch_one("SELECT COUNT(*) as cnt FROM dosare WHERE firma_id NOT IN (SELECT id FROM firme)")
        await database.database.execute("DELETE FROM timeline_events WHERE dosar_id IN (SELECT id FROM dosare WHERE firma_id NOT IN (SELECT id FROM firme))")
        await database.database.execute("DELETE FROM dosare WHERE firma_id NOT IN (SELECT id FROM firme)")
        await database.database.execute("VACUUM ANALYZE dosare")
        deleted = int(count_query["cnt"]) if count_query else 0
        return {"success": True, "deleted_count": deleted, "message": f"Deleted {deleted} orphaned dosare"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/optimize")
async def optimize_database():
    for table in ["firme", "dosare", "timeline_events"]:
        try:
            await database.database.execute(f"VACUUM ANALYZE {table}")
        except Exception:
            pass
    return {"success": True, "message": "Database optimized successfully"}


@router.post("/diagnostics/migrate-schema")
async def migrate_database_schema():
    try:
        new_columns = [
            ("cod_inregistrare", "VARCHAR(50)"), ("data_inregistrare", "VARCHAR(20)"),
            ("cod_onrc", "VARCHAR(100)"), ("forma_juridica", "VARCHAR(50)"),
            ("tara", "VARCHAR(100)"), ("judet", "VARCHAR(100)"), ("localitate", "VARCHAR(200)"),
            ("strada", "VARCHAR(300)"), ("numar", "VARCHAR(50)"), ("bloc", "VARCHAR(50)"),
            ("scara", "VARCHAR(20)"), ("etaj", "VARCHAR(20)"), ("apartament", "VARCHAR(20)"),
            ("cod_postal", "VARCHAR(20)"), ("detalii_adresa", "TEXT"),
            ("anaf_denumire", "VARCHAR(500)"), ("anaf_adresa", "TEXT"), ("anaf_nr_reg_com", "VARCHAR(50)"),
            ("anaf_telefon", "VARCHAR(50)"), ("anaf_fax", "VARCHAR(50)"), ("anaf_cod_postal", "VARCHAR(20)"),
            ("anaf_stare", "VARCHAR(200)"), ("anaf_data_inregistrare", "VARCHAR(20)"), ("anaf_cod_caen", "VARCHAR(20)"),
            ("anaf_forma_juridica", "VARCHAR(100)"), ("anaf_forma_organizare", "VARCHAR(100)"),
            ("anaf_forma_proprietate", "VARCHAR(200)"), ("anaf_organ_fiscal", "VARCHAR(200)"),
            ("anaf_platitor_tva", "BOOLEAN"), ("anaf_tva_incasare", "BOOLEAN"), ("anaf_split_tva", "BOOLEAN"),
            ("anaf_inactiv", "BOOLEAN"), ("anaf_e_factura", "BOOLEAN"),
            ("anaf_sediu_judet", "VARCHAR(100)"), ("anaf_sediu_localitate", "VARCHAR(200)"),
            ("anaf_sediu_strada", "VARCHAR(300)"), ("anaf_sediu_numar", "VARCHAR(50)"),
            ("anaf_sediu_cod_postal", "VARCHAR(20)"), ("anaf_last_sync", "TIMESTAMP"), ("anaf_sync_status", "VARCHAR(50)"),
            ("mf_denumire", "VARCHAR(500)"), ("mf_adresa", "TEXT"), ("mf_judet", "VARCHAR(100)"),
            ("mf_cod_postal", "VARCHAR(20)"), ("mf_telefon", "VARCHAR(50)"), ("mf_nr_reg_com", "VARCHAR(50)"),
            ("mf_stare", "VARCHAR(200)"), ("mf_platitor_tva", "BOOLEAN"), ("mf_tva_data", "VARCHAR(200)"),
            ("mf_impozit_profit", "VARCHAR(200)"), ("mf_impozit_micro", "VARCHAR(200)"),
            ("mf_accize", "BOOLEAN"), ("mf_cas_data", "VARCHAR(200)"), ("mf_an_bilant", "VARCHAR(10)"),
            ("mf_cifra_afaceri", "FLOAT"), ("mf_venituri_totale", "FLOAT"), ("mf_cheltuieli_totale", "FLOAT"),
            ("mf_profit_brut", "FLOAT"), ("mf_pierdere_bruta", "FLOAT"), ("mf_profit_net", "FLOAT"),
            ("mf_pierdere_neta", "FLOAT"), ("mf_numar_angajati", "INTEGER"),
            ("mf_active_imobilizate", "FLOAT"), ("mf_active_circulante", "FLOAT"),
            ("mf_capitaluri_proprii", "FLOAT"), ("mf_datorii", "FLOAT"),
            ("mf_ani_disponibili", "VARCHAR(200)"), ("mf_last_sync", "TIMESTAMP"), ("mf_sync_status", "VARCHAR(50)")
        ]
        added = []
        for col_name, col_type in new_columns:
            try:
                await database.database.execute(f"ALTER TABLE firme ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                added.append(col_name)
            except Exception:
                pass
        # Add siruta column for locality matching
        try:
            await database.database.execute("ALTER TABLE firme ADD COLUMN IF NOT EXISTS siruta BIGINT")
            await database.database.execute("CREATE INDEX IF NOT EXISTS idx_firme_siruta ON firme(siruta)")
            added.append("siruta")
        except Exception:
            pass
        return {"success": True, "columns_added": added, "message": f"Schema migration complete. Added {len(added)} columns."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diagnostics/indexes")
async def get_database_indexes():
    try:
        indexes = await database.database.fetch_all("""
            SELECT indexname, tablename, indexdef FROM pg_indexes
            WHERE schemaname = 'public' ORDER BY tablename, indexname
        """)
        return [{"name": r["indexname"], "table": r["tablename"], "definition": r["indexdef"]} for r in indexes]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/create-indexes")
async def create_performance_indexes():
    created = []
    index_queries = [
        ("idx_firme_denumire_normalized", "CREATE INDEX IF NOT EXISTS idx_firme_denumire_normalized ON firme(denumire_normalized)"),
        ("idx_firme_cui", "CREATE INDEX IF NOT EXISTS idx_firme_cui ON firme(cui) WHERE cui IS NOT NULL AND cui != ''"),
        ("idx_dosare_firma_id", "CREATE INDEX IF NOT EXISTS idx_dosare_firma_id ON dosare(firma_id)"),
        ("idx_dosare_numar", "CREATE INDEX IF NOT EXISTS idx_dosare_numar ON dosare(numar_dosar)"),
        ("idx_timeline_dosar_id", "CREATE INDEX IF NOT EXISTS idx_timeline_dosar_id ON timeline_events(dosar_id)"),
    ]
    for name, query in index_queries:
        try:
            await database.database.execute(query)
            created.append(name)
        except Exception:
            pass
    for table in ["firme", "dosare", "timeline_events"]:
        try:
            await database.database.execute(f"ANALYZE {table}")
        except Exception:
            pass
    return {"success": True, "created_indexes": created, "message": f"Created {len(created)} indexes"}
