"""
PostgreSQL database setup: SQLAlchemy models, connection management, session helper.
"""
import os
import time
import logging
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime,
    ForeignKey, JSON, Boolean, BigInteger, Float
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from databases import Database

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

logger = logging.getLogger(__name__)

POSTGRES_URL = os.environ.get('POSTGRES_URL', 'postgresql://justapp:justapp123@localhost:5432/justportal')
DATABASE_URL = POSTGRES_URL.replace('postgresql://', 'postgresql+asyncpg://')

database = Database(DATABASE_URL)
Base = declarative_base()

postgres_available = False
engine = None
SessionLocal = None


def init_postgres_connection(max_retries=5, retry_delay=3):
    global engine, SessionLocal, postgres_available
    from sqlalchemy import text
    for attempt in range(max_retries):
        try:
            engine = create_engine(
                POSTGRES_URL,
                pool_size=20,           # conexiuni permanente în pool
                max_overflow=40,        # conexiuni extra la peak
                pool_timeout=30,        # așteptare pentru o conexiune liberă
                pool_pre_ping=True,     # verifică conexiunea înainte de folosire
                pool_recycle=3600,      # reciclează conexiunile la fiecare oră
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            postgres_available = True
            logger.info(f"[DB] PostgreSQL connection successful (attempt {attempt + 1})")
            return True
        except Exception as e:
            logger.warning(f"[DB] Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    logger.warning("[DB] PostgreSQL not available after all retries. Running in limited mode.")
    engine = None
    SessionLocal = None
    return False


def get_db_session():
    global SessionLocal
    if SessionLocal is None:
        init_postgres_connection(max_retries=2, retry_delay=1)
    if SessionLocal is None:
        return None
    return SessionLocal()


# ─── SQLAlchemy Models ───────────────────────────────────────────────────────

class Firma(Base):
    __tablename__ = "firme"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    cui = Column(String(20), nullable=True, index=True)
    denumire = Column(String(500), nullable=False, index=True)
    denumire_normalized = Column(String(500), index=True)
    cod_inregistrare = Column(String(50), nullable=True)
    data_inregistrare = Column(String(20), nullable=True)
    cod_onrc = Column(String(100), nullable=True)
    forma_juridica = Column(String(50), nullable=True)
    tara = Column(String(100), nullable=True)
    judet = Column(String(100), nullable=True, index=True)
    localitate = Column(String(200), nullable=True)
    strada = Column(String(300), nullable=True)
    numar = Column(String(50), nullable=True)
    bloc = Column(String(50), nullable=True)
    scara = Column(String(20), nullable=True)
    etaj = Column(String(20), nullable=True)
    apartament = Column(String(20), nullable=True)
    cod_postal = Column(String(20), nullable=True)
    detalii_adresa = Column(Text, nullable=True)
    # ANAF
    anaf_denumire = Column(String(500), nullable=True)
    anaf_adresa = Column(Text, nullable=True)
    anaf_nr_reg_com = Column(String(50), nullable=True)
    anaf_telefon = Column(String(50), nullable=True)
    anaf_fax = Column(String(50), nullable=True)
    anaf_cod_postal = Column(String(20), nullable=True)
    anaf_stare = Column(String(200), nullable=True)
    anaf_data_inregistrare = Column(String(20), nullable=True)
    anaf_cod_caen = Column(String(20), nullable=True)
    anaf_forma_juridica = Column(String(100), nullable=True)
    anaf_forma_organizare = Column(String(100), nullable=True)
    anaf_forma_proprietate = Column(String(200), nullable=True)
    anaf_organ_fiscal = Column(String(200), nullable=True)
    anaf_platitor_tva = Column(Boolean, nullable=True)
    anaf_tva_incasare = Column(Boolean, nullable=True)
    anaf_split_tva = Column(Boolean, nullable=True)
    anaf_inactiv = Column(Boolean, nullable=True)
    anaf_e_factura = Column(Boolean, nullable=True)
    anaf_sediu_judet = Column(String(100), nullable=True)
    anaf_sediu_localitate = Column(String(200), nullable=True)
    anaf_sediu_strada = Column(String(300), nullable=True)
    anaf_sediu_numar = Column(String(50), nullable=True)
    anaf_sediu_cod_postal = Column(String(20), nullable=True)
    # Extra ANAF fields
    anaf_iban = Column(String(50), nullable=True)
    anaf_data_efactura = Column(String(20), nullable=True)      # data_inreg_Reg_RO_e_Factura
    anaf_data_inactivare = Column(String(20), nullable=True)
    anaf_data_reactivare = Column(String(20), nullable=True)
    anaf_data_radiere = Column(String(20), nullable=True)
    anaf_data_inceput_tva_inc = Column(String(20), nullable=True)
    anaf_data_sfarsit_tva_inc = Column(String(20), nullable=True)
    anaf_data_inceput_split_tva = Column(String(20), nullable=True)
    # Adresa domiciliu fiscal (poate diferi de sediu)
    anaf_df_judet = Column(String(100), nullable=True)
    anaf_df_localitate = Column(String(200), nullable=True)
    anaf_df_strada = Column(String(300), nullable=True)
    anaf_df_numar = Column(String(50), nullable=True)
    anaf_df_cod_postal = Column(String(20), nullable=True)
    anaf_last_sync = Column(DateTime, nullable=True)
    anaf_sync_status = Column(String(50), nullable=True)
    # MFinante
    mf_denumire = Column(String(500), nullable=True)
    mf_adresa = Column(Text, nullable=True)
    mf_judet = Column(String(100), nullable=True)
    mf_cod_postal = Column(String(20), nullable=True)
    mf_telefon = Column(String(50), nullable=True)
    mf_nr_reg_com = Column(String(50), nullable=True)
    mf_stare = Column(String(200), nullable=True)
    mf_platitor_tva = Column(Boolean, nullable=True)
    mf_tva_data = Column(String(200), nullable=True)
    mf_impozit_profit = Column(String(200), nullable=True)
    mf_impozit_micro = Column(String(200), nullable=True)
    mf_accize = Column(Boolean, nullable=True)
    mf_cas_data = Column(String(200), nullable=True)
    mf_an_bilant = Column(String(10), nullable=True)
    mf_cifra_afaceri = Column(Float, nullable=True)
    mf_venituri_totale = Column(Float, nullable=True)
    mf_cheltuieli_totale = Column(Float, nullable=True)
    mf_profit_brut = Column(Float, nullable=True)
    mf_pierdere_bruta = Column(Float, nullable=True)
    mf_profit_net = Column(Float, nullable=True)
    mf_pierdere_neta = Column(Float, nullable=True)
    mf_numar_angajati = Column(Integer, nullable=True)
    mf_active_imobilizate = Column(Float, nullable=True)
    mf_active_circulante = Column(Float, nullable=True)
    mf_capitaluri_proprii = Column(Float, nullable=True)
    mf_datorii = Column(Float, nullable=True)
    mf_ani_disponibili = Column(String(200), nullable=True)
    mf_last_sync = Column(DateTime, nullable=True)
    mf_sync_status = Column(String(50), nullable=True)
    # Localitati normalized (from judete-orase reference)
    siruta = Column(BigInteger, nullable=True, index=True)  # SIRUTA code for matched locality
    bilanturi = relationship("Bilant", back_populates="firma")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    dosare = relationship("Dosar", back_populates="firma")


class Dosar(Base):
    __tablename__ = "dosare"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    firma_id = Column(BigInteger, ForeignKey("firme.id"), nullable=False, index=True)
    numar_dosar = Column(String(100), index=True)
    institutie = Column(String(200))
    obiect = Column(Text)
    data_dosar = Column(DateTime, nullable=True)
    stadiu = Column(String(100))
    categorie = Column(String(200))
    materie = Column(String(200))
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    firma = relationship("Firma", back_populates="dosare")
    timeline = relationship("TimelineEvent", back_populates="dosar")


class TimelineEvent(Base):
    __tablename__ = "timeline"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dosar_id = Column(BigInteger, ForeignKey("dosare.id"), nullable=False, index=True)
    tip = Column(String(50))
    data = Column(DateTime, nullable=True)
    descriere = Column(Text)
    detalii = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    dosar = relationship("Dosar", back_populates="timeline")


class Bilant(Base):
    __tablename__ = "bilanturi"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    firma_id = Column(BigInteger, ForeignKey("firme.id"), nullable=False, index=True)
    an = Column(String(10), nullable=False, index=True)
    cifra_afaceri_neta = Column(Float, nullable=True)
    venituri_totale = Column(Float, nullable=True)
    cheltuieli_totale = Column(Float, nullable=True)
    profit_brut = Column(Float, nullable=True)
    pierdere_bruta = Column(Float, nullable=True)
    profit_net = Column(Float, nullable=True)
    pierdere_neta = Column(Float, nullable=True)
    numar_angajati = Column(Integer, nullable=True)
    active_imobilizate = Column(Float, nullable=True)
    active_circulante = Column(Float, nullable=True)
    stocuri = Column(Float, nullable=True)
    creante = Column(Float, nullable=True)
    casa_conturi_banci = Column(Float, nullable=True)
    cheltuieli_avans = Column(Float, nullable=True)
    capitaluri_proprii = Column(Float, nullable=True)
    capital_subscris = Column(Float, nullable=True)
    patrimoniul_regiei = Column(Float, nullable=True)
    provizioane = Column(Float, nullable=True)
    datorii = Column(Float, nullable=True)
    venituri_avans = Column(Float, nullable=True)
    repartizare_profit = Column(Float, nullable=True)
    raw_data = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    firma = relationship("Firma", back_populates="bilanturi")


def create_tables():
    if engine is not None:
        try:
            Base.metadata.create_all(bind=engine)
            logger.info("[DB] Tables created/verified")
            _migrate_schema()
        except Exception as e:
            logger.error(f"[DB] Could not create tables: {e}")


def _migrate_schema():
    """
    Add any new columns that are in the SQLAlchemy model but missing from the DB.
    Uses ALTER TABLE ... ADD COLUMN IF NOT EXISTS — safe to run multiple times.
    """
    if engine is None:
        return
    migrations = [
        # Localitati
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS siruta BIGINT",
        "CREATE INDEX IF NOT EXISTS idx_firme_siruta ON firme(siruta)",
        # ANAF extra columns added later
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_sediu_cod_postal VARCHAR(20)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_split_tva BOOLEAN",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_inactiv BOOLEAN",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_e_factura BOOLEAN",
        # Extra ANAF fields
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_iban VARCHAR(50)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_data_efactura VARCHAR(20)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_data_inactivare VARCHAR(20)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_data_reactivare VARCHAR(20)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_data_radiere VARCHAR(20)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_data_inceput_tva_inc VARCHAR(20)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_data_sfarsit_tva_inc VARCHAR(20)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_data_inceput_split_tva VARCHAR(20)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_df_judet VARCHAR(100)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_df_localitate VARCHAR(200)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_df_strada VARCHAR(300)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_df_numar VARCHAR(50)",
        "ALTER TABLE firme ADD COLUMN IF NOT EXISTS anaf_df_cod_postal VARCHAR(20)",
    ]
    from sqlalchemy import text
    with engine.connect() as conn:
        for sql in migrations:
            try:
                conn.execute(text(sql))
            except Exception:
                pass
        conn.commit()
    logger.info("[DB] Schema migrations applied")


# Initialize on import
init_postgres_connection()
create_tables()
