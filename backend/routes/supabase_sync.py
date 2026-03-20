"""
Supabase Sync — transfers processed data from local PostgreSQL to Supabase.
Logic: local Docker = processing power, Supabase = clean production read-only view.

What gets synced:
- firme: only ANAF-active firms with complete data
- bilanturi: financial statements for synced firms
- dosare: court cases linked to synced firms (optional)
"""
import asyncio
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import database
import state

router = APIRouter()
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")

# ─── Supabase sync state ──────────────────────────────────────────────────────
supabase_sync_progress = {
    "active": False,
    "phase": "",
    "total": 0,
    "processed": 0,
    "inserted": 0,
    "updated": 0,
    "errors": 0,
    "last_update": None,
    "logs": []
}


def add_sync_log(message: str):
    from datetime import datetime
    ts = datetime.now().strftime("%H:%M:%S")
    supabase_sync_progress["logs"].append(f"[{ts}] {message}")
    if len(supabase_sync_progress["logs"]) > 200:
        supabase_sync_progress["logs"] = supabase_sync_progress["logs"][-200:]


def _resolve_ipv4(hostname: str) -> str:
    """Force IPv4 resolution — avoids Docker IPv6 issues."""
    import socket
    try:
        results = socket.getaddrinfo(hostname, None, socket.AF_INET)
        if results:
            return results[0][4][0]
    except Exception:
        pass
    return hostname


def get_supabase_engine():
    """Create SQLAlchemy engine for Supabase with forced IPv4."""
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_URL nu este configurat în .env")

    # Parse hostname from URL
    # Format: postgresql://user:pass@host:port/db
    import re
    url_clean = re.sub(r'\?.*$', '', SUPABASE_URL.strip())
    match = re.search(r'@([^:/]+)', url_clean)
    hostname = match.group(1) if match else None

    connect_args = {
        "connect_timeout": 15,
        "sslmode": "require",
        "gssencmode": "disable",
    }

    # Force IPv4: resolve hostname and pass as hostaddr
    # psycopg2 uses 'host' for SSL cert validation, 'hostaddr' for actual TCP connection
    if hostname:
        ipv4 = _resolve_ipv4(hostname)
        if ipv4 != hostname:  # resolved successfully
            connect_args["hostaddr"] = ipv4
            logger.info(f"[SUPABASE] Forced IPv4: {hostname} → {ipv4}")

    return create_engine(
        url_clean,
        pool_size=3,
        max_overflow=5,
        pool_pre_ping=True,
        pool_timeout=20,
        connect_args=connect_args,
    )


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("/supabase/status")
async def get_supabase_status():
    """Check Supabase connection and sync status."""
    supabase_ok = False
    supabase_counts = {}
    error = None
    try:
        eng = get_supabase_engine()
        with eng.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM firme"))
            firme_count = result.scalar()
            result2 = conn.execute(text("SELECT COUNT(*) FROM bilanturi"))
            bilant_count = result2.scalar()
            supabase_counts = {"firme": firme_count, "bilanturi": bilant_count}
            supabase_ok = True
        eng.dispose()
    except Exception as e:
        error = str(e)[:100]

    local_counts = {}
    if database.SessionLocal:
        db = database.SessionLocal()
        try:
            Firma = database.Firma
            Bilant = database.Bilant
            local_counts = {
                "firme_total": db.query(Firma).count(),
                "firme_active": db.query(Firma).filter(
                    Firma.anaf_sync_status == 'found',
                    Firma.anaf_stare.ilike('%ACTIV%'),
                    ~Firma.anaf_stare.ilike('%INACTIV%'),
                    ~Firma.anaf_stare.ilike('%RADIERE%')
                ).count(),
                "bilanturi": db.query(Bilant).count(),
            }
        finally:
            db.close()

    return {
        "supabase_connected": supabase_ok,
        "supabase_error": error,
        "supabase_counts": supabase_counts,
        "local_counts": local_counts,
        "sync_active": supabase_sync_progress["active"],
        "progress": supabase_sync_progress,
        "logs": supabase_sync_progress["logs"][-60:]
    }


@router.get("/supabase/sync-progress")
async def get_sync_progress():
    return {**supabase_sync_progress, "logs": supabase_sync_progress["logs"][-80:]}


@router.post("/supabase/sync-stop")
async def stop_sync():
    supabase_sync_progress["active"] = False
    return {"message": "Stop requested"}


@router.post("/supabase/sync")
async def start_supabase_sync(
    background_tasks: BackgroundTasks,
    sync_firme: bool = True,
    sync_bilanturi: bool = True,
    sync_dosare: bool = False,
    only_active: bool = True,
    clean_first: bool = False,
    batch_size: int = 500,
    judet: str = None,
):
    """
    Sync data from local PostgreSQL to Supabase.
    
    only_active: sync only ANAF-confirmed active firms (recommended)
    clean_first: DELETE all data in Supabase before sync (full refresh)
    sync_dosare: also sync court cases (can be large)
    """
    if not SUPABASE_URL:
        raise HTTPException(status_code=400, detail="SUPABASE_URL nu este configurat în .env")
    if database.SessionLocal is None:
        raise HTTPException(status_code=503, detail="Local DB not available")
    if supabase_sync_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync deja în progres")

    supabase_sync_progress.update({
        "active": True, "phase": "Inițializare",
        "total": 0, "processed": 0, "inserted": 0, "updated": 0,
        "errors": 0, "last_update": datetime.utcnow().isoformat(), "logs": []
    })

    background_tasks.add_task(
        _run_sync, sync_firme, sync_bilanturi, sync_dosare,
        only_active, clean_first, batch_size, judet
    )
    return {"message": "Sync Supabase pornit", "status": "running"}


async def _run_sync(sync_firme, sync_bilanturi, sync_dosare, only_active, clean_first, batch_size, judet):
    """Main sync runner."""
    try:
        add_sync_log("Conectare la Supabase...")
        try:
            sb_engine = get_supabase_engine()
            with sb_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            add_sync_log("✅ Conectat la Supabase")
        except Exception as e:
            add_sync_log(f"❌ Eroare conectare Supabase: {str(e)[:80]}")
            supabase_sync_progress["active"] = False
            return

        # Ensure tables exist in Supabase
        add_sync_log("Verificare/creare tabele în Supabase...")
        try:
            database.Base.metadata.create_all(bind=sb_engine)
            add_sync_log("✅ Tabele Supabase OK")
        except Exception as e:
            add_sync_log(f"⚠️ Tabele: {str(e)[:60]}")

        if clean_first:
            add_sync_log("🗑️ Ștergere date vechi din Supabase...")
            with sb_engine.connect() as conn:
                if sync_dosare:
                    conn.execute(text("TRUNCATE TABLE timeline_events, dosare, bilanturi, firme RESTART IDENTITY CASCADE"))
                else:
                    conn.execute(text("TRUNCATE TABLE bilanturi, firme RESTART IDENTITY CASCADE"))
                conn.commit()
            add_sync_log("✅ Supabase curățat")

        # ── Sync firme ─────────────────────────────────────────────────────────
        if sync_firme and supabase_sync_progress["active"]:
            await _sync_firme(sb_engine, only_active, batch_size, judet)

        # ── Sync bilanturi ─────────────────────────────────────────────────────
        if sync_bilanturi and supabase_sync_progress["active"]:
            await _sync_bilanturi(sb_engine, batch_size)

        # ── Sync dosare ────────────────────────────────────────────────────────
        if sync_dosare and supabase_sync_progress["active"]:
            await _sync_dosare(sb_engine, batch_size)

        sb_engine.dispose()
        add_sync_log("─" * 40)
        add_sync_log(
            f"✅ Sync complet: {supabase_sync_progress['inserted']:,} inserate, "
            f"{supabase_sync_progress['updated']:,} actualizate, "
            f"{supabase_sync_progress['errors']} erori"
        )

    except Exception as e:
        add_sync_log(f"❌ Eroare generală: {str(e)[:80]}")
        logger.error(f"[SUPABASE SYNC] Error: {e}")
    finally:
        supabase_sync_progress["active"] = False


async def _sync_firme(sb_engine, only_active: bool, batch_size: int, judet: Optional[str]):
    """Sync firme from local to Supabase."""
    supabase_sync_progress["phase"] = "Firme"
    Firma = database.Firma
    db = database.SessionLocal()

    try:
        query = db.query(Firma)
        if only_active:
            query = query.filter(
                Firma.anaf_sync_status == 'found',
                Firma.anaf_stare.ilike('%ACTIV%'),
                ~Firma.anaf_stare.ilike('%INACTIV%'),
                ~Firma.anaf_stare.ilike('%RADIERE%')
            )
        if judet:
            query = query.filter(
                (Firma.judet.ilike(f"%{judet}%")) |
                (Firma.anaf_sediu_judet.ilike(f"%{judet}%"))
            )

        total = query.count()
        supabase_sync_progress["total"] = total
        add_sync_log(f"Firme de sincronizat: {total:,} {'(doar active)' if only_active else '(toate)'}")

        offset = 0
        inserted = 0
        updated = 0

        while offset < total and supabase_sync_progress["active"]:
            batch = query.order_by(Firma.id).offset(offset).limit(batch_size).all()
            if not batch:
                break

            # Build upsert data
            rows = []
            for f in batch:
                row = {
                    col.key: getattr(f, col.key)
                    for col in Firma.__table__.columns
                    if col.key not in ('id',)
                }
                # Clean NUL bytes
                for k, v in row.items():
                    if isinstance(v, str):
                        row[k] = v.replace('\x00', '').replace('\u0000', '') or None
                row['id'] = f.id
                rows.append(row)

            if rows:
                try:
                    cols = list(rows[0].keys())
                    col_str = ', '.join(f'"{c}"' for c in cols)
                    val_str = ', '.join(f':{c}' for c in cols)
                    update_str = ', '.join(
                        f'"{c}" = EXCLUDED."{c}"'
                        for c in cols if c not in ('id', 'created_at')
                    )
                    upsert_sql = text(f"""
                        INSERT INTO firme ({col_str})
                        VALUES ({val_str})
                        ON CONFLICT (id) DO UPDATE SET {update_str}
                    """)
                    with sb_engine.begin() as conn:
                        conn.execute(upsert_sql, rows)
                    inserted += len(rows)
                    supabase_sync_progress["inserted"] = inserted
                except Exception as e:
                    supabase_sync_progress["errors"] += 1
                    add_sync_log(f"⚠️ Batch firme eroare: {str(e)[:60]}")

            offset += batch_size
            supabase_sync_progress["processed"] = offset
            supabase_sync_progress["last_update"] = datetime.utcnow().isoformat()

            if offset % (batch_size * 10) == 0:
                pct = min(offset / total * 100, 100)
                add_sync_log(f"📊 Firme: {offset:,}/{total:,} ({pct:.0f}%) | Inserate: {inserted:,}")

            await asyncio.sleep(0.05)

        add_sync_log(f"✅ Firme sincronizate: {inserted:,}")

    finally:
        db.close()


async def _sync_bilanturi(sb_engine, batch_size: int):
    """Sync bilanturi for all firms that exist in Supabase."""
    supabase_sync_progress["phase"] = "Bilanțuri"
    Bilant = database.Bilant
    db = database.SessionLocal()

    try:
        # Only sync bilanturi for firms already in Supabase
        with sb_engine.connect() as conn:
            result = conn.execute(text("SELECT id FROM firme"))
            supabase_firma_ids = set(row[0] for row in result.fetchall())

        if not supabase_firma_ids:
            add_sync_log("⚠️ Nicio firmă în Supabase — sync bilanturi skipped")
            return

        query = db.query(Bilant).filter(Bilant.firma_id.in_(supabase_firma_ids))
        total = query.count()
        add_sync_log(f"Bilanțuri de sincronizat: {total:,}")

        offset = 0
        inserted = 0

        while offset < total and supabase_sync_progress["active"]:
            batch = query.order_by(Bilant.id).offset(offset).limit(batch_size).all()
            if not batch:
                break

            rows = []
            for b in batch:
                row = {col.key: getattr(b, col.key) for col in Bilant.__table__.columns}
                for k, v in row.items():
                    if isinstance(v, str):
                        row[k] = v.replace('\x00', '') or None
                rows.append(row)

            if rows:
                try:
                    cols = list(rows[0].keys())
                    col_str = ', '.join(f'"{c}"' for c in cols)
                    val_str = ', '.join(f':{c}' for c in cols)
                    update_str = ', '.join(
                        f'"{c}" = EXCLUDED."{c}"'
                        for c in cols if c not in ('id', 'firma_id', 'an', 'created_at')
                    )
                    upsert_sql = text(f"""
                        INSERT INTO bilanturi ({col_str})
                        VALUES ({val_str})
                        ON CONFLICT (id) DO UPDATE SET {update_str}
                    """)
                    with sb_engine.begin() as conn:
                        conn.execute(upsert_sql, rows)
                    inserted += len(rows)
                    supabase_sync_progress["updated"] = inserted
                except Exception as e:
                    supabase_sync_progress["errors"] += 1
                    add_sync_log(f"⚠️ Batch bilanturi eroare: {str(e)[:60]}")

            offset += batch_size
            await asyncio.sleep(0.05)

        add_sync_log(f"✅ Bilanțuri sincronizate: {inserted:,}")

    finally:
        db.close()


async def _sync_dosare(sb_engine, batch_size: int):
    """Sync dosare for firms in Supabase."""
    supabase_sync_progress["phase"] = "Dosare"
    Dosar = database.Dosar
    db = database.SessionLocal()

    try:
        with sb_engine.connect() as conn:
            result = conn.execute(text("SELECT id FROM firme"))
            supabase_firma_ids = set(row[0] for row in result.fetchall())

        if not supabase_firma_ids:
            add_sync_log("⚠️ Nicio firmă în Supabase — sync dosare skipped")
            return

        query = db.query(Dosar).filter(Dosar.firma_id.in_(supabase_firma_ids))
        total = query.count()
        add_sync_log(f"Dosare de sincronizat: {total:,}")

        offset = 0
        inserted = 0

        while offset < total and supabase_sync_progress["active"]:
            batch = query.order_by(Dosar.id).offset(offset).limit(batch_size).all()
            if not batch:
                break

            rows = []
            for d in batch:
                row = {}
                for col in Dosar.__table__.columns:
                    val = getattr(d, col.key)
                    if isinstance(val, str):
                        val = val.replace('\x00', '') or None
                    row[col.key] = val
                rows.append(row)

            if rows:
                try:
                    cols = [c for c in rows[0].keys() if c != 'raw_data']  # skip large JSON
                    cols.append('raw_data')
                    col_str = ', '.join(f'"{c}"' for c in cols)
                    val_str = ', '.join(f':{c}' for c in cols)
                    update_str = ', '.join(
                        f'"{c}" = EXCLUDED."{c}"'
                        for c in cols if c not in ('id', 'firma_id', 'numar_dosar', 'created_at')
                    )
                    upsert_sql = text(f"""
                        INSERT INTO dosare ({col_str})
                        VALUES ({val_str})
                        ON CONFLICT (id) DO UPDATE SET {update_str}
                    """)
                    with sb_engine.begin() as conn:
                        conn.execute(upsert_sql, rows)
                    inserted += len(rows)
                except Exception as e:
                    supabase_sync_progress["errors"] += 1
                    add_sync_log(f"⚠️ Batch dosare eroare: {str(e)[:60]}")

            offset += batch_size
            if offset % (batch_size * 20) == 0:
                add_sync_log(f"📊 Dosare: {offset:,}/{total:,}")
            await asyncio.sleep(0.05)

        add_sync_log(f"✅ Dosare sincronizate: {inserted:,}")

    finally:
        db.close()
