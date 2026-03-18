from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks, UploadFile, File
from fastapi.responses import FileResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey, JSON, Boolean, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from databases import Database
import os
import re
import csv
import io
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
import uuid
from datetime import datetime, timezone
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
import json

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection (for config/job runs)
mongo_url = os.environ['MONGO_URL']
mongo_client = AsyncIOMotorClient(mongo_url)
mongo_db = mongo_client[os.environ['DB_NAME']]

# PostgreSQL connection
POSTGRES_URL = os.environ.get('POSTGRES_URL', 'postgresql://justapp:justapp123@localhost:5432/justportal')
DATABASE_URL = POSTGRES_URL.replace('postgresql://', 'postgresql+asyncpg://')

database = Database(DATABASE_URL)
engine = create_engine(POSTGRES_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Create downloads directory (for backup JSON)
DOWNLOADS_DIR = ROOT_DIR / 'downloads'
DOWNLOADS_DIR.mkdir(exist_ok=True)

# SQLAlchemy Models
class Firma(Base):
    __tablename__ = "firme"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    # Date de identificare
    cui = Column(String(20), nullable=True, index=True)
    denumire = Column(String(500), nullable=False, index=True)
    denumire_normalized = Column(String(500), index=True)  # For search
    
    # Date înregistrare ONRC
    cod_inregistrare = Column(String(50), nullable=True)  # J40/123/2024
    data_inregistrare = Column(String(20), nullable=True)  # 19/12/2023
    cod_onrc = Column(String(100), nullable=True)  # ROONRC.J40/123/2024
    forma_juridica = Column(String(50), nullable=True)  # SRL, SA, PFA, etc.
    
    # Adresa sediu social
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
    
    # Alte detalii adresă
    detalii_adresa = Column(Text, nullable=True)  # Restul câmpurilor concatenate
    
    # ===== DATE ANAF =====
    # Date generale ANAF
    anaf_denumire = Column(String(500), nullable=True)
    anaf_adresa = Column(Text, nullable=True)
    anaf_nr_reg_com = Column(String(50), nullable=True)  # J17/836/2002
    anaf_telefon = Column(String(50), nullable=True)
    anaf_fax = Column(String(50), nullable=True)
    anaf_cod_postal = Column(String(20), nullable=True)
    anaf_stare = Column(String(200), nullable=True)  # RADIERE din data... / ACTIV
    anaf_data_inregistrare = Column(String(20), nullable=True)
    anaf_cod_caen = Column(String(20), nullable=True)
    anaf_forma_juridica = Column(String(100), nullable=True)
    anaf_forma_organizare = Column(String(100), nullable=True)
    anaf_forma_proprietate = Column(String(200), nullable=True)
    anaf_organ_fiscal = Column(String(200), nullable=True)
    
    # Status TVA
    anaf_platitor_tva = Column(Boolean, nullable=True)  # scpTVA
    anaf_tva_incasare = Column(Boolean, nullable=True)  # statusTvaIncasare
    anaf_split_tva = Column(Boolean, nullable=True)  # statusSplitTVA
    anaf_inactiv = Column(Boolean, nullable=True)  # statusInactivi
    anaf_e_factura = Column(Boolean, nullable=True)  # statusRO_e_Factura
    
    # Adresa sediu social ANAF
    anaf_sediu_judet = Column(String(100), nullable=True)
    anaf_sediu_localitate = Column(String(200), nullable=True)
    anaf_sediu_strada = Column(String(300), nullable=True)
    anaf_sediu_numar = Column(String(50), nullable=True)
    anaf_sediu_cod_postal = Column(String(20), nullable=True)
    
    # Metadata sincronizare
    anaf_last_sync = Column(DateTime, nullable=True)
    anaf_sync_status = Column(String(50), nullable=True)  # success, not_found, error
    
    # ===== DATE MFINANTE (date identificare) =====
    mf_denumire = Column(String(500), nullable=True)
    mf_adresa = Column(Text, nullable=True)
    mf_judet = Column(String(100), nullable=True)
    mf_cod_postal = Column(String(20), nullable=True)
    mf_telefon = Column(String(50), nullable=True)
    mf_nr_reg_com = Column(String(50), nullable=True)
    mf_stare = Column(String(200), nullable=True)
    
    # ===== DATE FISCALE MFINANTE =====
    mf_platitor_tva = Column(Boolean, nullable=True)
    mf_tva_data = Column(String(200), nullable=True)  # Când a intrat/ieșit în/din TVA
    mf_impozit_profit = Column(String(200), nullable=True)
    mf_impozit_micro = Column(String(200), nullable=True)
    mf_accize = Column(Boolean, nullable=True)
    mf_cas_data = Column(String(200), nullable=True)
    
    # ===== BILANȚ CEL MAI RECENT (pentru acces rapid) =====
    mf_an_bilant = Column(String(10), nullable=True)  # anul bilanțului cel mai recent
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
    
    # ===== METADATA MFINANTE =====
    mf_ani_disponibili = Column(String(200), nullable=True)  # Lista anilor disponibili (ex: "2023,2022,2021")
    mf_last_sync = Column(DateTime, nullable=True)
    mf_sync_status = Column(String(50), nullable=True)
    
    # Relație cu bilanțurile istorice
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
    raw_data = Column(JSON)  # Store original data
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    firma = relationship("Firma", back_populates="dosare")
    timeline = relationship("TimelineEvent", back_populates="dosar")


class TimelineEvent(Base):
    __tablename__ = "timeline"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dosar_id = Column(BigInteger, ForeignKey("dosare.id"), nullable=False, index=True)
    tip = Column(String(50))  # sedinta, hotarare, cale_atac, parte
    data = Column(DateTime, nullable=True)
    descriere = Column(Text)
    detalii = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    dosar = relationship("Dosar", back_populates="timeline")


class Bilant(Base):
    """
    Tabel pentru istoricul complet al bilanțurilor de la MFinante.
    Fiecare firmă poate avea bilanțuri pentru mai mulți ani.
    """
    __tablename__ = "bilanturi"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    firma_id = Column(BigInteger, ForeignKey("firme.id"), nullable=False, index=True)
    an = Column(String(10), nullable=False, index=True)  # Anul bilanțului (ex: "2023")
    
    # Indicatori principali
    cifra_afaceri_neta = Column(Float, nullable=True)
    venituri_totale = Column(Float, nullable=True)
    cheltuieli_totale = Column(Float, nullable=True)
    profit_brut = Column(Float, nullable=True)
    pierdere_bruta = Column(Float, nullable=True)
    profit_net = Column(Float, nullable=True)
    pierdere_neta = Column(Float, nullable=True)
    numar_angajati = Column(Integer, nullable=True)
    
    # Indicatori patrimoniali
    active_imobilizate = Column(Float, nullable=True)
    active_circulante = Column(Float, nullable=True)
    stocuri = Column(Float, nullable=True)
    creante = Column(Float, nullable=True)
    casa_conturi_banci = Column(Float, nullable=True)
    cheltuieli_avans = Column(Float, nullable=True)
    
    # Pasive
    capitaluri_proprii = Column(Float, nullable=True)
    capital_subscris = Column(Float, nullable=True)
    patrimoniul_regiei = Column(Float, nullable=True)
    provizioane = Column(Float, nullable=True)
    datorii = Column(Float, nullable=True)
    venituri_avans = Column(Float, nullable=True)
    
    # Indicatori suplimentari
    repartizare_profit = Column(Float, nullable=True)
    
    # Date raw pentru orice alte câmpuri
    raw_data = Column(JSON, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    firma = relationship("Firma", back_populates="bilanturi")
    
    # Unique constraint: o firmă poate avea un singur bilanț per an
    __table_args__ = (
        # Index compus pentru căutare rapidă
        # Index('ix_bilanturi_firma_an', 'firma_id', 'an', unique=True),
    )


# Create tables
Base.metadata.create_all(bind=engine)

# Create the main app with increased file upload limit (300MB for large CSV files)
app = FastAPI()
app.router.redirect_slashes = False

# CORS Configuration - MUST be added before routes
# Allow all origins for local development, or use CORS_ORIGINS env var
cors_origins_env = os.environ.get('CORS_ORIGINS', '')
if cors_origins_env and cors_origins_env != '*':
    cors_origins = [origin.strip() for origin in cors_origins_env.split(',')]
else:
    cors_origins = ["*"]

print(f"[CORS] Configured origins: {cors_origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True if cors_origins != ["*"] else False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Set max upload size
from starlette.requests import Request
from starlette.datastructures import UploadFile as StarletteUploadFile

# Increase max request size to 300MB
MAX_UPLOAD_SIZE = 300 * 1024 * 1024  # 300MB
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import progress tracking (global state)
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

# ANAF sync progress tracking
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
    "eta_seconds": None
}

# Scheduler
scheduler = AsyncIOScheduler()

# SOAP Configuration
SOAP_URL = "http://portalquery.just.ro/query.asmx"
SOAP_ACTION_DOSARE2 = "portalquery.just.ro/CautareDosare2"

# Pattern to match companies (SC, SRL, SA, etc.) - EXCLUDES PFA, II, IF
COMPANY_PATTERNS = [
    r'\bSRL\b', r'\bSA\b', r'\bSCS\b', r'\bSNC\b', r'\bSCA\b',
    r'\bSPRL\b', r'\bGMBH\b', r'\bLTD\b', r'\bLLC\b', r'\bINC\b',
    r'\bONG\b', r'\bASSOC\b',
    r'S\.R\.L\.', r'S\.A\.', r'S\.C\.S\.', r'S\.N\.C\.'
]
COMPANY_REGEX = re.compile('|'.join(COMPANY_PATTERNS), re.IGNORECASE)

# Patterns to EXCLUDE (individuals, not companies)
EXCLUDE_PATTERNS = [
    r'PERSOANĂ FIZICĂ', r'PERSOANA FIZICA', r'PFA\b',
    r'ÎNTREPRINDERE INDIVIDUALĂ', r'INTREPRINDERE INDIVIDUALA',
    r'\bII\b', r'\bIF\b', r'CABINET INDIVIDUAL', r'BIROU INDIVIDUAL'
]
EXCLUDE_REGEX = re.compile('|'.join(EXCLUDE_PATTERNS), re.IGNORECASE)

# Complete list of all 246 institutions
INSTITUTII = [
    "CurteaMilitaradeApelBUCURESTI", "CurteadeApelALBAIULIA", "CurteadeApelBACAU",
    "CurteadeApelBRASOV", "CurteadeApelBUCURESTI", "CurteadeApelCLUJ",
    "CurteadeApelCONSTANTA", "CurteadeApelCRAIOVA", "CurteadeApelGALATI",
    "CurteadeApelIASI", "CurteadeApelORADEA", "CurteadeApelPITESTI",
    "CurteadeApelPLOIESTI", "CurteadeApelSUCEAVA", "CurteadeApelTARGUMURES",
    "CurteadeApelTIMISOARA",
    "TribunalulALBA", "TribunalulARAD", "TribunalulARGES", "TribunalulBACAU",
    "TribunalulBIHOR", "TribunalulBISTRITANASAUD", "TribunalulBOTOSANI",
    "TribunalulBRAILA", "TribunalulBRASOV", "TribunalulBUCURESTI",
    "TribunalulBUZAU", "TribunalulCALARASI", "TribunalulCARASSEVERIN",
    "TribunalulCLUJ", "TribunalulCONSTANTA", "TribunalulCOVASNA",
    "TribunalulComercialARGES", "TribunalulComercialCLUJ", "TribunalulComercialMURES",
    "TribunalulDAMBOVITA", "TribunalulDOLJ", "TribunalulGALATI",
    "TribunalulGIURGIU", "TribunalulGORJ", "TribunalulHARGHITA",
    "TribunalulHUNEDOARA", "TribunalulIALOMITA", "TribunalulIASI",
    "TribunalulILFOV", "TribunalulMARAMURES", "TribunalulMEHEDINTI",
    "TribunalulMURES", "TribunalulMilitarBUCURESTI", "TribunalulMilitarCLUJNAPOCA",
    "TribunalulMilitarIASI", "TribunalulMilitarTIMISOARA", "TribunalulMilitarTeritorialBUCURESTI",
    "TribunalulNEAMT", "TribunalulOLT", "TribunalulPRAHOVA",
    "TribunalulSALAJ", "TribunalulSATUMARE", "TribunalulSIBIU",
    "TribunalulSUCEAVA", "TribunalulTELEORMAN", "TribunalulTIMIS",
    "TribunalulTULCEA", "TribunalulVALCEA", "TribunalulVASLUI",
    "TribunalulVRANCEA", "TribunalulpentruminoriSifamilieBRASOV",
    "JudecatoriaADJUD", "JudecatoriaAGNITA", "JudecatoriaAIUD", "JudecatoriaALBAIULIA",
    "JudecatoriaALESD", "JudecatoriaALEXANDRIA", "JudecatoriaARAD", "JudecatoriaAVRIG",
    "JudecatoriaBABADAG", "JudecatoriaBACAU", "JudecatoriaBAIADEARAMA", "JudecatoriaBAIAMARE",
    "JudecatoriaBAILESTI", "JudecatoriaBALCESTI", "JudecatoriaBALS", "JudecatoriaBARLAD",
    "JudecatoriaBECLEAN", "JudecatoriaBEIUS", "JudecatoriaBICAZ", "JudecatoriaBISTRITA",
    "JudecatoriaBLAJ", "JudecatoriaBOLINTINVALE", "JudecatoriaBOTOSANI", "JudecatoriaBOZOVICI",
    "JudecatoriaBRAD", "JudecatoriaBRAILA", "JudecatoriaBRASOV", "JudecatoriaBREZOI",
    "JudecatoriaBUFTEA", "JudecatoriaBUHUSI", "JudecatoriaBUZAU", "JudecatoriaCALAFAT",
    "JudecatoriaCALARASI", "JudecatoriaCAMPENI", "JudecatoriaCAMPINA", "JudecatoriaCAMPULUNG",
    "JudecatoriaCAMPULUNGMOLDOVENESC", "JudecatoriaCARACAL", "JudecatoriaCARANSEBES",
    "JudecatoriaCAREI", "JudecatoriaCHISINEUCRIS", "JudecatoriaCLUJNAPOCA", "JudecatoriaCONSTANTA",
    "JudecatoriaCORABIA", "JudecatoriaCORNETU", "JudecatoriaCOSTESTI", "JudecatoriaCRAIOVA",
    "JudecatoriaCURTEADEARGES", "JudecatoriaDEJ", "JudecatoriaDETA", "JudecatoriaDEVA",
    "JudecatoriaDOROHOI", "JudecatoriaDRAGASANI", "JudecatoriaDRAGOMIRESTI",
    "JudecatoriaDROBETATURNUSEVERIN", "JudecatoriaDarabani", "JudecatoriaFAGARAS",
    "JudecatoriaFAGET", "JudecatoriaFALTICENI", "JudecatoriaFAUREI", "JudecatoriaFETESTI",
    "JudecatoriaFILIASI", "JudecatoriaFOCSANI", "JudecatoriaGAESTI", "JudecatoriaGALATI",
    "JudecatoriaGHEORGHENI", "JudecatoriaGHERLA", "JudecatoriaGIURGIU", "JudecatoriaGURAHONT",
    "JudecatoriaGURAHUMORULUI", "JudecatoriaHARLAU", "JudecatoriaHARSOVA", "JudecatoriaHATEG",
    "JudecatoriaHOREZU", "JudecatoriaHUEDIN", "JudecatoriaHUNEDOARA", "JudecatoriaHUSI",
    "JudecatoriaIASI", "JudecatoriaINEU", "JudecatoriaINSURATEI", "JudecatoriaINTORSURABUZAULUI",
    "JudecatoriaJIBOU", "JudecatoriaLEHLIUGARA", "JudecatoriaLIESTI", "JudecatoriaLIPOVA",
    "JudecatoriaLUDUS", "JudecatoriaLUGOJ", "JudecatoriaMACIN", "JudecatoriaMANGALIA",
    "JudecatoriaMARGHITA", "JudecatoriaMEDGIDIA", "JudecatoriaMEDIAS", "JudecatoriaMIERCUREACIUC",
    "JudecatoriaMIZIL", "JudecatoriaMOINESTI", "JudecatoriaMOLDOVANOUA", "JudecatoriaMORENI",
    "JudecatoriaMOTRU", "JudecatoriaMURGENI", "JudecatoriaNASAUD", "JudecatoriaNEGRESTIOAS",
    "JudecatoriaNOVACI", "JudecatoriaODORHEIULSECUIESC", "JudecatoriaOLTENITA", "JudecatoriaONESTI",
    "JudecatoriaORADEA", "JudecatoriaORASTIE", "JudecatoriaORAVITA", "JudecatoriaORSOVA",
    "JudecatoriaPANCIU", "JudecatoriaPASCANI", "JudecatoriaPATARLAGELE", "JudecatoriaPETROSANI",
    "JudecatoriaPIATRANEAMT", "JudecatoriaPITESTI", "JudecatoriaPLOIESTI", "JudecatoriaPODUTURCULUI",
    "JudecatoriaPOGOANELE", "JudecatoriaPUCIOASA", "JudecatoriaRACARI", "JudecatoriaRADAUTI",
    "JudecatoriaRADUCANENI", "JudecatoriaRAMNICUSARAT", "JudecatoriaRAMNICUVALCEA",
    "JudecatoriaREGHIN", "JudecatoriaRESITA", "JudecatoriaROMAN", "JudecatoriaROSIORIDEVEDE",
    "JudecatoriaRUPEA", "JudecatoriaSALISTE", "JudecatoriaSALONTA", "JudecatoriaSANNICOLAULMARE",
    "JudecatoriaSATUMARE", "JudecatoriaSAVENI", "JudecatoriaSEBES", "JudecatoriaSECTORUL1BUCURESTI",
    "JudecatoriaSECTORUL2BUCURESTI", "JudecatoriaSECTORUL3BUCURESTI", "JudecatoriaSECTORUL4BUCURESTI",
    "JudecatoriaSECTORUL5BUCURESTI", "JudecatoriaSECTORUL6BUCURESTI", "JudecatoriaSEGARCEA",
    "JudecatoriaSFANTUGHEORGHE", "JudecatoriaSIBIU", "JudecatoriaSIGHETUMARMATIEI",
    "JudecatoriaSIGHISOARA", "JudecatoriaSIMLEULSILVANIEI", "JudecatoriaSINAIA", "JudecatoriaSLATINA",
    "JudecatoriaSLOBOZIA", "JudecatoriaSOMCUTAMARE", "JudecatoriaSTREHAIA", "JudecatoriaSUCEAVA",
    "JudecatoriaTARGOVISTE", "JudecatoriaTARGUBUJOR", "JudecatoriaTARGUCARBUNESTI",
    "JudecatoriaTARGUJIU", "JudecatoriaTARGULAPUS", "JudecatoriaTARGUMURES", "JudecatoriaTARGUNEAMT",
    "JudecatoriaTARGUSECUIESC", "JudecatoriaTARNAVENI", "JudecatoriaTECUCI", "JudecatoriaTIMISOARA",
    "JudecatoriaTOPLITA", "JudecatoriaTOPOLOVENI", "JudecatoriaTULCEA", "JudecatoriaTURDA",
    "JudecatoriaTURNUMAGURELE", "JudecatoriaURZICENI", "JudecatoriaVALENIIDEMUNTE",
    "JudecatoriaVANJUMARE", "JudecatoriaVASLUI", "JudecatoriaVATRADORNEI", "JudecatoriaVIDELE",
    "JudecatoriaVISEUDESUS", "JudecatoriaZALAU", "JudecatoriaZARNESTI", "JudecatoriaZIMNICEA"
]


# Pydantic Models
class JobConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schedule_hour: int = 2
    schedule_minute: int = 0
    search_term: str = ""
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    cron_enabled: bool = False
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class JobConfigUpdate(BaseModel):
    schedule_hour: Optional[int] = None
    schedule_minute: Optional[int] = None
    search_term: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    cron_enabled: Optional[bool] = None
    is_active: Optional[bool] = None


class JobRun(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    status: str = "running"
    total_records: int = 0
    records_downloaded: int = 0
    firme_count: int = 0
    dosare_count: int = 0
    timeline_count: int = 0
    error_message: Optional[str] = None
    files_created: List[str] = []
    triggered_by: str = "manual"


class SearchRequest(BaseModel):
    company_name: str
    institutie: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None


class FirmaCreate(BaseModel):
    cui: Optional[str] = None
    denumire: str


class FirmaUpdate(BaseModel):
    cui: Optional[str] = None
    denumire: Optional[str] = None


# Helper functions
def normalize_company_name(name: str) -> str:
    """Normalize company name for matching"""
    name = name.upper().strip()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'[^\w\s]', '', name)
    return name


def is_company(name: str) -> bool:
    """Check if the name is a real company (SRL, SA, etc.) - excludes PFA, II, IF"""
    # First check if it should be excluded (PFA, II, etc.)
    if EXCLUDE_REGEX.search(name):
        return False
    # Then check if it's a company
    return bool(COMPANY_REGEX.search(name))


def extract_companies_from_parti(parti: list) -> List[dict]:
    """Extract company names from parti list"""
    companies = []
    for parte in parti:
        nume = parte.get('nume', '')
        if nume and is_company(nume):
            companies.append({
                'denumire': nume.strip(),
                'denumire_normalized': normalize_company_name(nume),
                'calitate': parte.get('calitateParte', '')
            })
    return companies


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string to datetime"""
    if not date_str:
        return None
    try:
        # Try common formats
        for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%Y-%m-%dT%H:%M:%S']:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
    except Exception:
        pass
    return None


def build_soap_request(nume_parte: str, institutie: str, date_start: str = "", date_end: str = "") -> str:
    """Build SOAP request for CautareDosare2"""
    data_start = f"<dataStart>{date_start}</dataStart>" if date_start else "<dataStart xsi:nil=\"true\" />"
    data_stop = f"<dataStop>{date_end}</dataStop>" if date_end else "<dataStop xsi:nil=\"true\" />"
    
    return f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
               xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CautareDosare2 xmlns="portalquery.just.ro">
      <numarDosar xsi:nil="true" />
      <obiectDosar xsi:nil="true" />
      <numeParte>{nume_parte}</numeParte>
      <institutie>{institutie}</institutie>
      {data_start}
      {data_stop}
      <dataUltimaModificareStart xsi:nil="true" />
      <dataUltimaModificareStop xsi:nil="true" />
    </CautareDosare2>
  </soap:Body>
</soap:Envelope>'''


def parse_soap_response(xml_content: str) -> List[dict]:
    """Parse SOAP response"""
    try:
        root = ET.fromstring(xml_content)
        namespaces = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/', 'pq': 'portalquery.just.ro'}
        
        dosare = []
        for dosar in root.findall('.//pq:Dosar', namespaces):
            dosar_dict = {}
            for child in dosar:
                tag = child.tag.replace('{portalquery.just.ro}', '')
                dosar_dict[tag] = child.text or ""
                
                if len(child) > 0:
                    nested_items = []
                    for nested in child:
                        nested_dict = {}
                        for nc in nested:
                            ntag = nc.tag.replace('{portalquery.just.ro}', '')
                            nested_dict[ntag] = nc.text or ""
                        if nested_dict:
                            nested_items.append(nested_dict)
                    if nested_items:
                        dosar_dict[tag] = nested_items
            
            if dosar_dict:
                dosare.append(dosar_dict)
        return dosare
    except ET.ParseError as e:
        logger.error(f"XML Parse error: {e}")
        return []


async def fetch_dosare(session: aiohttp.ClientSession, nume_parte: str, institutie: str, 
                       date_start: str = "", date_end: str = "") -> List[dict]:
    """Fetch dosare from portal"""
    soap_body = build_soap_request(nume_parte, institutie, date_start, date_end)
    headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': f'"{SOAP_ACTION_DOSARE2}"'}
    
    try:
        async with session.post(SOAP_URL, data=soap_body, headers=headers, timeout=60) as response:
            if response.status == 200:
                return parse_soap_response(await response.text())
    except Exception as e:
        logger.error(f"Error fetching from {institutie}: {e}")
    return []


async def save_to_postgres(dosare: List[dict], search_term: str) -> dict:
    """Save dosare to PostgreSQL, extracting only companies"""
    db = SessionLocal()
    stats = {'firme_new': 0, 'firme_existing': 0, 'dosare_new': 0, 'timeline_new': 0}
    
    try:
        for dosar_data in dosare:
            # Extract companies from parti
            parti = dosar_data.get('parti', [])
            if not isinstance(parti, list):
                continue
                
            companies = extract_companies_from_parti(parti)
            if not companies:
                continue  # Skip if no companies found
            
            # Process each company
            for company in companies:
                # Find or create firma
                firma = db.query(Firma).filter(
                    Firma.denumire_normalized == company['denumire_normalized']
                ).first()
                
                if not firma:
                    firma = Firma(
                        denumire=company['denumire'],
                        denumire_normalized=company['denumire_normalized']
                    )
                    db.add(firma)
                    db.flush()
                    stats['firme_new'] += 1
                else:
                    stats['firme_existing'] += 1
                
                # Check if dosar already exists for this firma
                numar_dosar = dosar_data.get('numar', '')
                existing_dosar = db.query(Dosar).filter(
                    Dosar.firma_id == firma.id,
                    Dosar.numar_dosar == numar_dosar
                ).first()
                
                if existing_dosar:
                    continue  # Skip duplicate
                
                # Create dosar
                dosar = Dosar(
                    firma_id=firma.id,
                    numar_dosar=numar_dosar,
                    institutie=dosar_data.get('institutie', ''),
                    obiect=dosar_data.get('obiect', ''),
                    data_dosar=parse_date(dosar_data.get('data', '')),
                    stadiu=dosar_data.get('stadiuProcesual', ''),
                    categorie=dosar_data.get('categorieCaz', ''),
                    materie=dosar_data.get('materie', ''),
                    raw_data=dosar_data
                )
                db.add(dosar)
                db.flush()
                stats['dosare_new'] += 1
                
                # Add timeline events from sedinte
                sedinte = dosar_data.get('sedinte', [])
                if isinstance(sedinte, list):
                    for sedinta in sedinte:
                        event = TimelineEvent(
                            dosar_id=dosar.id,
                            tip='sedinta',
                            data=parse_date(sedinta.get('data', '')),
                            descriere=sedinta.get('solutie', '') or sedinta.get('complet', ''),
                            detalii=sedinta
                        )
                        db.add(event)
                        stats['timeline_new'] += 1
                
                # Add timeline events from caiAtac
                cai_atac = dosar_data.get('caiAtac', [])
                if isinstance(cai_atac, list):
                    for cale in cai_atac:
                        event = TimelineEvent(
                            dosar_id=dosar.id,
                            tip='cale_atac',
                            data=parse_date(cale.get('dataDeclarare', '')),
                            descriere=cale.get('tipCaleAtac', ''),
                            detalii=cale
                        )
                        db.add(event)
                        stats['timeline_new'] += 1
        
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error saving to PostgreSQL: {e}")
        raise
    finally:
        db.close()
    
    return stats


async def run_download_job(search_term: str, job_run_id: str, date_start: str = "", 
                           date_end: str = "", triggered_by: str = "manual"):
    """Run the download job"""
    logger.info(f"Starting download job for: {search_term}")
    
    all_dosare = []
    processed = 0
    total_stats = {'firme_new': 0, 'firme_existing': 0, 'dosare_new': 0, 'timeline_new': 0}
    
    async with aiohttp.ClientSession() as session:
        for institutie in INSTITUTII:
            dosare = await fetch_dosare(session, search_term, institutie, date_start, date_end)
            
            if dosare:
                # Save to PostgreSQL
                stats = await save_to_postgres(dosare, search_term)
                for key in total_stats:
                    total_stats[key] += stats[key]
                all_dosare.extend(dosare)
            
            processed += 1
            await mongo_db.job_runs.update_one(
                {"id": job_run_id},
                {"$set": {
                    "records_downloaded": len(all_dosare),
                    "firme_count": total_stats['firme_new'],
                    "dosare_count": total_stats['dosare_new'],
                    "timeline_count": total_stats['timeline_new'],
                    "progress_message": f"Procesare {processed}/{len(INSTITUTII)} instituții"
                }}
            )
            await asyncio.sleep(0.5)
    
    # Also save JSON backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dosare_{search_term.replace(' ', '_')}_{timestamp}.json"
    filepath = DOWNLOADS_DIR / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({
            "search_term": search_term,
            "date_start": date_start,
            "date_end": date_end,
            "stats": total_stats,
            "total_records": len(all_dosare),
            "dosare": all_dosare
        }, f, ensure_ascii=False, indent=2)
    
    # Update job run
    await mongo_db.job_runs.update_one(
        {"id": job_run_id},
        {"$set": {
            "status": "completed",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(all_dosare),
            "firme_count": total_stats['firme_new'],
            "dosare_count": total_stats['dosare_new'],
            "timeline_count": total_stats['timeline_new'],
            "files_created": [filename]
        }}
    )
    
    logger.info(f"Job completed: {total_stats}")
    return total_stats


async def scheduled_job():
    """Scheduled cron job"""
    config = await mongo_db.job_config.find_one({}, {"_id": 0})
    if not config or not config.get('search_term') or not config.get('cron_enabled'):
        return
    
    running = await mongo_db.job_runs.find_one({"status": "running"}, {"_id": 0})
    if running:
        return
    
    job_run = JobRun(triggered_by="cron")
    job_run_dict = job_run.model_dump()
    job_run_dict['started_at'] = job_run_dict['started_at'].isoformat()
    await mongo_db.job_runs.insert_one(job_run_dict)
    
    await run_download_job(config['search_term'], job_run.id, 
                           config.get('date_start', ''), config.get('date_end', ''), "cron")


def update_scheduler(hour: int, minute: int, enabled: bool):
    """Update scheduler"""
    if scheduler.get_job('daily_download'):
        scheduler.remove_job('daily_download')
    
    if enabled:
        scheduler.add_job(scheduled_job, CronTrigger(hour=hour, minute=minute),
                         id='daily_download', replace_existing=True)


# API Endpoints
@api_router.get("/")
async def root():
    return {"message": "Portal JUST Downloader API - PostgreSQL Edition"}


@api_router.get("/config")
async def get_config():
    config = await mongo_db.job_config.find_one({}, {"_id": 0})
    if not config:
        default_config = JobConfig().model_dump()
        default_config['created_at'] = default_config['created_at'].isoformat()
        default_config['updated_at'] = default_config['updated_at'].isoformat()
        await mongo_db.job_config.insert_one(default_config)
        return default_config
    return config


@api_router.put("/config")
async def update_config(update: JobConfigUpdate):
    update_data = {k: v for k, v in update.model_dump().items() if v is not None}
    update_data['updated_at'] = datetime.now(timezone.utc).isoformat()
    
    result = await mongo_db.job_config.find_one_and_update(
        {}, {"$set": update_data}, return_document=True, projection={"_id": 0}
    )
    
    if not result:
        config = JobConfig(**update_data)
        config_dict = config.model_dump()
        config_dict['created_at'] = config_dict['created_at'].isoformat()
        config_dict['updated_at'] = config_dict['updated_at'].isoformat()
        await mongo_db.job_config.insert_one(config_dict)
        result = config_dict
    
    if any(k in update_data for k in ['schedule_hour', 'schedule_minute', 'cron_enabled']):
        update_scheduler(result.get('schedule_hour', 2), result.get('schedule_minute', 0),
                        result.get('cron_enabled', False))
    
    return result


@api_router.post("/run")
async def trigger_run(background_tasks: BackgroundTasks):
    config = await mongo_db.job_config.find_one({}, {"_id": 0})
    if not config or not config.get('search_term'):
        raise HTTPException(status_code=400, detail="No search term configured")
    
    running = await mongo_db.job_runs.find_one({"status": "running"}, {"_id": 0})
    if running:
        raise HTTPException(status_code=409, detail="A job is already running")
    
    job_run = JobRun(triggered_by="manual")
    job_run_dict = job_run.model_dump()
    job_run_dict['started_at'] = job_run_dict['started_at'].isoformat()
    await mongo_db.job_runs.insert_one(job_run_dict)
    
    background_tasks.add_task(run_download_job, config['search_term'], job_run.id,
                              config.get('date_start', ''), config.get('date_end', ''), "manual")
    
    return {"message": "Job started", "job_id": job_run.id}


@api_router.get("/runs")
async def get_runs():
    return await mongo_db.job_runs.find({}, {"_id": 0}).sort("started_at", -1).to_list(50)


@api_router.get("/runs/current")
async def get_current_run():
    return await mongo_db.job_runs.find_one({"status": "running"}, {"_id": 0})


@api_router.post("/search")
async def search_dosare(request: SearchRequest):
    async with aiohttp.ClientSession() as session:
        if not request.institutie:
            all_dosare = []
            for inst in ["TribunalulBUCURESTI", "CurteadeApelBUCURESTI", "TribunalulCLUJ"]:
                dosare = await fetch_dosare(session, request.company_name, inst,
                                           request.date_start or "", request.date_end or "")
                all_dosare.extend(dosare)
                if len(all_dosare) >= 20:
                    break
            return {"total": len(all_dosare), "dosare": all_dosare[:20]}
        else:
            dosare = await fetch_dosare(session, request.company_name, request.institutie,
                                       request.date_start or "", request.date_end or "")
            return {"total": len(dosare), "dosare": dosare[:20]}


# PostgreSQL Data Endpoints
@api_router.get("/db/firme/export")
async def export_firme_csv():
    """Export all firms as CSV for editing"""
    db = SessionLocal()
    try:
        firme = db.query(Firma).order_by(Firma.denumire).all()
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['id', 'cui', 'denumire', 'dosare_count'])
        
        for firma in firme:
            dosare_count = db.query(Dosar).filter(Dosar.firma_id == firma.id).count()
            writer.writerow([firma.id, firma.cui or '', firma.denumire, dosare_count])
        
        output.seek(0)
        
        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"firme_export_{timestamp}.csv"
        filepath = DOWNLOADS_DIR / filename
        
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            f.write(output.getvalue())
        
        return FileResponse(
            filepath, 
            filename=filename, 
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    finally:
        db.close()


@api_router.get("/db/firme")
async def get_firme(skip: int = 0, limit: int = 100, search: str = None, judet: str = None):
    """Get all companies from PostgreSQL"""
    db = SessionLocal()
    try:
        query = db.query(Firma)
        if search:
            # Search by CUI or denumire
            search_term = search.strip()
            if search_term.isdigit():
                # Search by CUI
                query = query.filter(Firma.cui.contains(search_term))
            else:
                query = query.filter(Firma.denumire_normalized.contains(normalize_company_name(search_term)))
        
        if judet:
            query = query.filter(Firma.judet.ilike(f"%{judet}%"))
        
        total = query.count()
        firme = query.order_by(Firma.id.desc()).offset(skip).limit(limit).all()
        
        # Get dosare counts
        result = []
        for f in firme:
            dosare_count = db.query(Dosar).filter(Dosar.firma_id == f.id).count()
            result.append({
                "id": f.id, 
                "cui": f.cui, 
                "denumire": f.denumire,
                "cod_inregistrare": f.cod_inregistrare,
                "data_inregistrare": f.data_inregistrare,
                "forma_juridica": f.forma_juridica,
                "judet": f.judet,
                "localitate": f.localitate,
                "strada": f.strada,
                "numar": f.numar,
                "dosare_count": dosare_count,
                "created_at": f.created_at.isoformat() if f.created_at else None
            })
        
        return {
            "total": total,
            "firme": result
        }
    finally:
        db.close()


@api_router.get("/db/firme/{firma_id}")
async def get_firma(firma_id: int):
    """Get firma details with dosare"""
    db = SessionLocal()
    try:
        firma = db.query(Firma).filter(Firma.id == firma_id).first()
        if not firma:
            raise HTTPException(status_code=404, detail="Firma not found")
        
        dosare = db.query(Dosar).filter(Dosar.firma_id == firma_id).all()
        return {
            "id": firma.id,
            "cui": firma.cui,
            "denumire": firma.denumire,
            "cod_inregistrare": firma.cod_inregistrare,
            "data_inregistrare": firma.data_inregistrare,
            "cod_onrc": firma.cod_onrc,
            "forma_juridica": firma.forma_juridica,
            "tara": firma.tara,
            "judet": firma.judet,
            "localitate": firma.localitate,
            "strada": firma.strada,
            "numar": firma.numar,
            "bloc": firma.bloc,
            "scara": firma.scara,
            "etaj": firma.etaj,
            "apartament": firma.apartament,
            "cod_postal": firma.cod_postal,
            "detalii_adresa": firma.detalii_adresa,
            "created_at": firma.created_at.isoformat() if firma.created_at else None,
            "updated_at": firma.updated_at.isoformat() if firma.updated_at else None,
            "dosare_count": len(dosare),
            "dosare": [{
                "id": d.id,
                "numar_dosar": d.numar_dosar,
                "institutie": d.institutie,
                "obiect": d.obiect,
                "stadiu": d.stadiu,
                "data_dosar": d.data_dosar.isoformat() if d.data_dosar else None
            } for d in dosare]
        }
    finally:
        db.close()


@api_router.put("/db/firme/{firma_id}")
async def update_firma(firma_id: int, update: FirmaUpdate):
    """Update firma CUI"""
    db = SessionLocal()
    try:
        firma = db.query(Firma).filter(Firma.id == firma_id).first()
        if not firma:
            raise HTTPException(status_code=404, detail="Firma not found")
        
        if update.cui is not None:
            firma.cui = update.cui
        if update.denumire is not None:
            firma.denumire = update.denumire
            firma.denumire_normalized = normalize_company_name(update.denumire)
        
        firma.updated_at = datetime.utcnow()
        db.commit()
        
        return {"id": firma.id, "cui": firma.cui, "denumire": firma.denumire}
    finally:
        db.close()


@api_router.get("/db/dosare/{dosar_id}")
async def get_dosar(dosar_id: int):
    """Get dosar with timeline"""
    db = SessionLocal()
    try:
        dosar = db.query(Dosar).filter(Dosar.id == dosar_id).first()
        if not dosar:
            raise HTTPException(status_code=404, detail="Dosar not found")
        
        timeline = db.query(TimelineEvent).filter(TimelineEvent.dosar_id == dosar_id).order_by(TimelineEvent.data).all()
        firma = db.query(Firma).filter(Firma.id == dosar.firma_id).first()
        
        return {
            "id": dosar.id,
            "numar_dosar": dosar.numar_dosar,
            "firma": {"id": firma.id, "cui": firma.cui, "denumire": firma.denumire} if firma else None,
            "institutie": dosar.institutie,
            "obiect": dosar.obiect,
            "stadiu": dosar.stadiu,
            "categorie": dosar.categorie,
            "materie": dosar.materie,
            "data_dosar": dosar.data_dosar.isoformat() if dosar.data_dosar else None,
            "timeline": [{
                "id": t.id,
                "tip": t.tip,
                "data": t.data.isoformat() if t.data else None,
                "descriere": t.descriere,
                "detalii": t.detalii
            } for t in timeline]
        }
    finally:
        db.close()


@api_router.get("/db/stats")
async def get_db_stats():
    """Get PostgreSQL database statistics"""
    db = SessionLocal()
    try:
        firme_count = db.query(Firma).count()
        firme_with_cui = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '').count()
        dosare_count = db.query(Dosar).count()
        timeline_count = db.query(TimelineEvent).count()
        
        return {
            "firme_total": firme_count,
            "firme_with_cui": firme_with_cui,
            "firme_without_cui": firme_count - firme_with_cui,
            "dosare_total": dosare_count,
            "timeline_events": timeline_count
        }
    finally:
        db.close()


@api_router.get("/db/import-progress")
async def get_import_progress():
    """Get current import progress"""
    return import_progress


@api_router.post("/db/import-cui")
async def import_cui_csv(
    file: UploadFile = File(...),
    has_header: bool = False,
    only_companies: bool = True
):
    """
    Import companies from ONRC file directly into database.
    Saves all columns from the ONRC file.
    
    ONRC file structure (delimiter: ^):
    0: DENUMIRE (company name)
    1: CUI (tax ID)
    2: COD_INREGISTRARE (registration code like J40/123/2024)
    3: DATA_INREGISTRARE (registration date)
    4: COD_ONRC (ROONRC identifier)
    5: FORMA_JURIDICA (legal form: SRL, SA, PFA, etc.)
    6: TARA (country)
    7: JUDET (county)
    8: LOCALITATE (city)
    9: STRADA (street)
    10: NUMAR (number)
    11: BLOC (building)
    12: SCARA (entrance)
    13: ETAJ (floor)
    14: APARTAMENT (apartment)
    15: COD_POSTAL (postal code)
    16+: Additional details
    """
    
    # Read content
    content = await file.read()
    try:
        decoded = content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            decoded = content.decode('latin-1')
        except UnicodeDecodeError:
            decoded = content.decode('cp1252')
    
    # Detect delimiter
    lines = [l for l in decoded.strip().split('\n') if l.strip()]
    if not lines:
        raise HTTPException(status_code=400, detail="File is empty")
    
    first_line = lines[0]
    delimiter = '^'
    if '^' in first_line:
        delimiter = '^'
    elif '\t' in first_line:
        delimiter = '\t'
    elif ';' in first_line:
        delimiter = ';'
    elif ',' in first_line:
        delimiter = ','
    
    logger.info(f"Detected delimiter: '{delimiter}', total lines: {len(lines)}")
    
    start_row = 1 if has_header else 0
    
    # Initialize progress tracking
    global import_progress
    import_progress = {
        "active": True,
        "filename": file.filename,
        "total_rows": len(lines) - start_row,
        "processed": 0,
        "created_new": 0,
        "updated": 0,
        "skipped_not_company": 0,
        "skipped_no_cui": 0,
        "last_update": datetime.utcnow().isoformat()
    }
    
    db = SessionLocal()
    results = {
        "total_rows": 0,
        "processed": 0,
        "skipped_not_company": 0,
        "created_new": 0,
        "already_exists": 0,
        "updated": 0,
        "skipped_no_cui": 0,
        "sample_created": [],
        "delimiter_detected": delimiter
    }
    
    # Build lookup of existing firms by CUI
    existing_by_cui = {}
    all_firme = db.query(Firma).all()
    for firma in all_firme:
        if firma.cui:
            existing_by_cui[firma.cui] = firma
    
    logger.info(f"Existing firms in DB: {len(all_firme)}")
    
    def get_col(cols, idx, default=None):
        """Safely get column value"""
        if idx < len(cols):
            val = cols[idx].strip() if cols[idx] else None
            return val if val else default
        return default
    
    def update_progress():
        """Update global progress for polling"""
        global import_progress
        import_progress["processed"] = results["total_rows"]
        import_progress["created_new"] = results["created_new"]
        import_progress["updated"] = results["updated"]
        import_progress["skipped_not_company"] = results["skipped_not_company"]
        import_progress["skipped_no_cui"] = results["skipped_no_cui"]
        import_progress["last_update"] = datetime.utcnow().isoformat()
    
    try:
        batch_count = 0
        
        for i, line in enumerate(lines[start_row:], start=start_row):
            if not line.strip():
                continue
                
            results["total_rows"] += 1
            
            cols = line.split(delimiter)
            
            # Extract all columns
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
            
            # Concatenate remaining columns as additional details
            detalii_adresa = None
            if len(cols) > 16:
                extra = [c.strip() for c in cols[16:] if c.strip()]
                if extra:
                    detalii_adresa = ' | '.join(extra)
            
            if not denumire:
                continue
            
            # Filter: only companies (SRL, SA, etc.) if requested
            # Use FORMA_JURIDICA column for accurate filtering
            if only_companies:
                # Exclude PF, PFA, II, IF, AF based on forma_juridica column
                excluded_forms = {'PF', 'PFA', 'II', 'IF', 'AF', 'CA', 'FORMA_JURIDICA'}  # FORMA_JURIDICA is header
                if forma_juridica and forma_juridica.upper() in excluded_forms:
                    results["skipped_not_company"] += 1
                    continue
                # Also check the name for safety
                if not is_company(denumire) and forma_juridica not in {'SRL', 'SA', 'SNC', 'SCS', 'SCA', 'RA', 'GIE', 'SC', 'OCR', 'OCC', 'OCM', 'OC1', 'OC2'}:
                    results["skipped_not_company"] += 1
                    continue
            
            # Skip header row
            if cui == 'CUI' or denumire == 'DENUMIRE':
                continue
            
            # Skip if no valid CUI
            if not cui or cui == '0' or len(cui) < 2:
                results["skipped_no_cui"] += 1
                continue
            
            results["processed"] += 1
            
            denumire_normalized = normalize_company_name(denumire)
            
            # Check if firm already exists by CUI
            existing_firma = existing_by_cui.get(cui)
            
            if existing_firma:
                # Update existing firm with new data
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
                # Create new firm with all data
                new_firma = Firma(
                    cui=cui,
                    denumire=denumire,
                    denumire_normalized=denumire_normalized,
                    cod_inregistrare=cod_inregistrare,
                    data_inregistrare=data_inregistrare,
                    cod_onrc=cod_onrc,
                    forma_juridica=forma_juridica,
                    tara=tara,
                    judet=judet,
                    localitate=localitate,
                    strada=strada,
                    numar=numar,
                    bloc=bloc,
                    scara=scara,
                    etaj=etaj,
                    apartament=apartament,
                    cod_postal=cod_postal,
                    detalii_adresa=detalii_adresa
                )
                db.add(new_firma)
                
                # Update lookup dictionary
                existing_by_cui[cui] = new_firma
                
                results["created_new"] += 1
                
                if len(results["sample_created"]) < 5:
                    results["sample_created"].append({
                        "denumire": denumire[:50] if denumire else "",
                        "cui": cui,
                        "forma_juridica": forma_juridica,
                        "judet": judet,
                        "localitate": localitate
                    })
            
            batch_count += 1
            
            # Commit and log every 5000 rows
            if batch_count >= 5000:
                db.commit()
                batch_count = 0
                update_progress()
                logger.info(f"[PROGRESS] {results['total_rows']:,} rânduri | {results['created_new']:,} create | {results['updated']:,} actualizate | {results['skipped_not_company']:,} PFA/II sărite")
        
        db.commit()
        update_progress()
        logger.info(f"[COMPLETE] Import finalizat: {results['total_rows']:,} rânduri, {results['created_new']:,} firme create, {results['updated']:,} actualizate")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing file: {e}")
        import_progress["active"] = False
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    finally:
        db.close()
        import_progress["active"] = False
    
    return results


@api_router.get("/files")
async def list_files():
    files = []
    for f in DOWNLOADS_DIR.iterdir():
        if f.is_file() and f.suffix == '.json':
            stat = f.stat()
            files.append({"name": f.name, "size": stat.st_size,
                         "created": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()})
    return sorted(files, key=lambda x: x['created'], reverse=True)


@api_router.get("/files/{filename}")
async def download_file(filename: str):
    filepath = DOWNLOADS_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(filepath, filename=filename, media_type='application/json')


@api_router.delete("/files/{filename}")
async def delete_file(filename: str):
    filepath = DOWNLOADS_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="File not found")
    filepath.unlink()
    return {"message": "File deleted"}


@api_router.get("/stats")
async def get_stats():
    total_runs = await mongo_db.job_runs.count_documents({})
    completed_runs = await mongo_db.job_runs.count_documents({"status": "completed"})
    failed_runs = await mongo_db.job_runs.count_documents({"status": "failed"})
    config = await mongo_db.job_config.find_one({}, {"_id": 0})
    last_run = await mongo_db.job_runs.find_one({}, {"_id": 0}, sort=[("started_at", -1)])
    
    total_files = sum(1 for f in DOWNLOADS_DIR.iterdir() if f.is_file() and f.suffix == '.json')
    total_size = sum(f.stat().st_size for f in DOWNLOADS_DIR.iterdir() if f.is_file() and f.suffix == '.json')
    
    job = scheduler.get_job('daily_download')
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    
    # Get PostgreSQL stats
    db = SessionLocal()
    try:
        firme_count = db.query(Firma).count()
        dosare_count = db.query(Dosar).count()
    finally:
        db.close()
    
    return {
        "total_runs": total_runs,
        "completed_runs": completed_runs,
        "failed_runs": failed_runs,
        "total_files": total_files,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "cron_enabled": config.get('cron_enabled', False) if config else False,
        "next_scheduled_run": next_run,
        "last_run": last_run,
        "db_firme": firme_count,
        "db_dosare": dosare_count
    }


@api_router.get("/cron/status")
async def get_cron_status():
    job = scheduler.get_job('daily_download')
    config = await mongo_db.job_config.find_one({}, {"_id": 0})
    return {
        "enabled": config.get('cron_enabled', False) if config else False,
        "schedule_hour": config.get('schedule_hour', 2) if config else 2,
        "schedule_minute": config.get('schedule_minute', 0) if config else 0,
        "next_run": job.next_run_time.isoformat() if job and job.next_run_time else None,
        "job_active": job is not None
    }


@api_router.get("/institutions")
async def get_institutions():
    return INSTITUTII


# ============================================
# DATABASE DIAGNOSTICS ENDPOINTS
# ============================================

@api_router.get("/diagnostics/overview")
async def get_diagnostics_overview():
    """Get complete database diagnostics overview"""
    try:
        # Table counts - with fallback for missing tables
        try:
            firme_count = await database.fetch_one("SELECT COUNT(*) as cnt FROM firme")
            firme_cnt = int(firme_count["cnt"]) if firme_count else 0
        except Exception:
            firme_cnt = 0
            
        try:
            dosare_count = await database.fetch_one("SELECT COUNT(*) as cnt FROM dosare")
            dosare_cnt = int(dosare_count["cnt"]) if dosare_count else 0
        except Exception:
            dosare_cnt = 0
            
        try:
            timeline_count = await database.fetch_one("SELECT COUNT(*) as cnt FROM timeline_events")
            timeline_cnt = int(timeline_count["cnt"]) if timeline_count else 0
        except Exception:
            timeline_cnt = 0
        
        # Table sizes (approximate) - with fallback
        try:
            table_sizes = await database.fetch_all("""
                SELECT 
                    relname as table_name,
                    pg_size_pretty(pg_total_relation_size(relid)) as total_size,
                    pg_total_relation_size(relid) as size_bytes
                FROM pg_catalog.pg_statio_user_tables
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(relid) DESC
            """)
            table_sizes_list = [
                {"table": r["table_name"], "size": r["total_size"], "bytes": int(r["size_bytes"])}
                for r in table_sizes
            ]
        except Exception as e:
            logger.warning(f"Could not fetch table sizes: {e}")
            table_sizes_list = []
        
        # Duplicate counts - with fallback
        try:
            denumire_dupes = await database.fetch_one("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT denumire_normalized 
                    FROM firme 
                    GROUP BY denumire_normalized 
                    HAVING COUNT(*) > 1
                ) as dupes
            """)
            dup_denumiri = int(denumire_dupes["cnt"]) if denumire_dupes else 0
        except Exception:
            dup_denumiri = 0
        
        try:
            cui_dupes = await database.fetch_one("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT cui 
                    FROM firme 
                    WHERE cui IS NOT NULL AND cui != ''
                    GROUP BY cui 
                    HAVING COUNT(*) > 1
                ) as dupes
            """)
            dup_cui = int(cui_dupes["cnt"]) if cui_dupes else 0
        except Exception:
            dup_cui = 0
        
        # Firme without CUI
        try:
            no_cui = await database.fetch_one("""
                SELECT COUNT(*) as cnt FROM firme WHERE cui IS NULL OR cui = ''
            """)
            no_cui_cnt = int(no_cui["cnt"]) if no_cui else 0
        except Exception:
            no_cui_cnt = 0
        
        # Orphaned dosare (no firma)
        try:
            orphaned_dosare = await database.fetch_one("""
                SELECT COUNT(*) as cnt FROM dosare 
                WHERE firma_id NOT IN (SELECT id FROM firme)
            """)
            orphaned_cnt = int(orphaned_dosare["cnt"]) if orphaned_dosare else 0
        except Exception:
            orphaned_cnt = 0
        
        return {
            "counts": {
                "firme": firme_cnt,
                "dosare": dosare_cnt,
                "timeline_events": timeline_cnt
            },
            "table_sizes": table_sizes_list,
            "issues": {
                "duplicate_denumiri": dup_denumiri,
                "duplicate_cui": dup_cui,
                "firme_without_cui": no_cui_cnt,
                "orphaned_dosare": orphaned_cnt
            }
        }
    except Exception as e:
        logger.error(f"Error in diagnostics overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/diagnostics/duplicates/denumire")
async def get_duplicate_denumiri(limit: int = 50):
    """Get list of duplicate company names"""
    try:
        duplicates = await database.fetch_all(f"""
            SELECT 
                denumire_normalized,
                COUNT(*) as count,
                array_agg(id ORDER BY id) as ids,
                array_agg(denumire ORDER BY id) as denumiri,
                array_agg(COALESCE(cui, '') ORDER BY id) as cui_list
            FROM firme 
            GROUP BY denumire_normalized 
            HAVING COUNT(*) > 1 
            ORDER BY COUNT(*) DESC 
            LIMIT {limit}
        """)
        
        return [
            {
                "denumire_normalized": r["denumire_normalized"],
                "count": r["count"],
                "ids": list(r["ids"]) if r["ids"] else [],
                "denumiri": list(r["denumiri"]) if r["denumiri"] else [],
                "cui_list": list(r["cui_list"]) if r["cui_list"] else []
            }
            for r in duplicates
        ]
    except Exception as e:
        logger.error(f"Error fetching duplicate denumiri: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/diagnostics/duplicates/cui")
async def get_duplicate_cui(limit: int = 50):
    """Get list of duplicate CUIs"""
    try:
        duplicates = await database.fetch_all(f"""
            SELECT 
                cui,
                COUNT(*) as count,
                array_agg(id ORDER BY id) as ids,
                array_agg(denumire ORDER BY id) as denumiri
            FROM firme 
            WHERE cui IS NOT NULL AND cui != ''
            GROUP BY cui 
            HAVING COUNT(*) > 1 
            ORDER BY COUNT(*) DESC 
            LIMIT {limit}
        """)
        
        return [
            {
                "cui": r["cui"],
                "count": r["count"],
                "ids": list(r["ids"]) if r["ids"] else [],
                "denumiri": list(r["denumiri"]) if r["denumiri"] else []
            }
            for r in duplicates
        ]
    except Exception as e:
        logger.error(f"Error fetching duplicate CUIs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/diagnostics/cleanup/duplicates-denumire")
async def cleanup_duplicate_denumiri():
    """Remove duplicate companies by denumire_normalized, keeping the one with CUI or lowest ID"""
    try:
        # First, count how many will be deleted
        count_query = await database.fetch_one("""
            SELECT COUNT(*) as cnt FROM firme f1
            WHERE EXISTS (
                SELECT 1 FROM firme f2
                WHERE f2.denumire_normalized = f1.denumire_normalized
                AND (
                    (f2.cui IS NOT NULL AND f2.cui != '' AND (f1.cui IS NULL OR f1.cui = ''))
                    OR (f2.id < f1.id AND (f2.cui IS NOT NULL OR f1.cui IS NULL))
                )
            )
        """)
        
        # Delete duplicates, keeping the one with CUI or lowest ID
        result = await database.execute("""
            DELETE FROM firme 
            WHERE id IN (
                SELECT f1.id FROM firme f1
                WHERE EXISTS (
                    SELECT 1 FROM firme f2
                    WHERE f2.denumire_normalized = f1.denumire_normalized
                    AND f2.id < f1.id
                    AND (
                        (f2.cui IS NOT NULL AND f2.cui != '')
                        OR (f1.cui IS NULL OR f1.cui = '')
                    )
                )
            )
        """)
        
        # Vacuum analyze
        await database.execute("VACUUM ANALYZE firme")
        
        return {
            "success": True,
            "deleted_count": count_query["cnt"] if count_query else 0,
            "message": f"Deleted {count_query['cnt'] if count_query else 0} duplicate entries"
        }
    except Exception as e:
        logger.error(f"Error cleaning duplicates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/diagnostics/cleanup/duplicates-cui")
async def cleanup_duplicate_cui():
    """Remove duplicate companies by CUI, keeping the one with lowest ID"""
    try:
        count_query = await database.fetch_one("""
            SELECT COUNT(*) as cnt FROM firme f1
            WHERE cui IS NOT NULL AND cui != ''
            AND EXISTS (
                SELECT 1 FROM firme f2
                WHERE f2.cui = f1.cui
                AND f2.id < f1.id
            )
        """)
        
        result = await database.execute("""
            DELETE FROM firme 
            WHERE id IN (
                SELECT f1.id FROM firme f1
                WHERE f1.cui IS NOT NULL AND f1.cui != ''
                AND EXISTS (
                    SELECT 1 FROM firme f2
                    WHERE f2.cui = f1.cui
                    AND f2.id < f1.id
                )
            )
        """)
        
        await database.execute("VACUUM ANALYZE firme")
        
        return {
            "success": True,
            "deleted_count": count_query["cnt"] if count_query else 0,
            "message": f"Deleted {count_query['cnt'] if count_query else 0} duplicate CUI entries"
        }
    except Exception as e:
        logger.error(f"Error cleaning CUI duplicates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/diagnostics/cleanup/orphaned-dosare")
async def cleanup_orphaned_dosare():
    """Remove dosare that have no associated firma"""
    try:
        count_query = await database.fetch_one("""
            SELECT COUNT(*) as cnt FROM dosare 
            WHERE firma_id NOT IN (SELECT id FROM firme)
        """)
        
        # Delete orphaned timeline events first
        await database.execute("""
            DELETE FROM timeline_events 
            WHERE dosar_id IN (
                SELECT id FROM dosare 
                WHERE firma_id NOT IN (SELECT id FROM firme)
            )
        """)
        
        # Delete orphaned dosare
        await database.execute("""
            DELETE FROM dosare 
            WHERE firma_id NOT IN (SELECT id FROM firme)
        """)
        
        await database.execute("VACUUM ANALYZE dosare")
        await database.execute("VACUUM ANALYZE timeline_events")
        
        return {
            "success": True,
            "deleted_count": count_query["cnt"] if count_query else 0,
            "message": f"Deleted {count_query['cnt'] if count_query else 0} orphaned dosare"
        }
    except Exception as e:
        logger.error(f"Error cleaning orphaned dosare: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/diagnostics/optimize")
async def optimize_database():
    """Run VACUUM ANALYZE on all tables to optimize performance"""
    try:
        try:
            await database.execute("VACUUM ANALYZE firme")
        except Exception:
            pass
        try:
            await database.execute("VACUUM ANALYZE dosare")
        except Exception:
            pass
        try:
            await database.execute("VACUUM ANALYZE timeline_events")
        except Exception:
            pass
        
        return {
            "success": True,
            "message": "Database optimized successfully"
        }
    except Exception as e:
        logger.error(f"Error optimizing database: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/diagnostics/migrate-schema")
async def migrate_database_schema():
    """Add new columns to firme table if they don't exist"""
    try:
        new_columns = [
            # ONRC columns
            ("cod_inregistrare", "VARCHAR(50)"),
            ("data_inregistrare", "VARCHAR(20)"),
            ("cod_onrc", "VARCHAR(100)"),
            ("forma_juridica", "VARCHAR(50)"),
            ("tara", "VARCHAR(100)"),
            ("judet", "VARCHAR(100)"),
            ("localitate", "VARCHAR(200)"),
            ("strada", "VARCHAR(300)"),
            ("numar", "VARCHAR(50)"),
            ("bloc", "VARCHAR(50)"),
            ("scara", "VARCHAR(20)"),
            ("etaj", "VARCHAR(20)"),
            ("apartament", "VARCHAR(20)"),
            ("cod_postal", "VARCHAR(20)"),
            ("detalii_adresa", "TEXT"),
            # ANAF columns
            ("anaf_denumire", "VARCHAR(500)"),
            ("anaf_adresa", "TEXT"),
            ("anaf_nr_reg_com", "VARCHAR(50)"),
            ("anaf_telefon", "VARCHAR(50)"),
            ("anaf_fax", "VARCHAR(50)"),
            ("anaf_cod_postal", "VARCHAR(20)"),
            ("anaf_stare", "VARCHAR(200)"),
            ("anaf_data_inregistrare", "VARCHAR(20)"),
            ("anaf_cod_caen", "VARCHAR(20)"),
            ("anaf_forma_juridica", "VARCHAR(100)"),
            ("anaf_forma_organizare", "VARCHAR(100)"),
            ("anaf_forma_proprietate", "VARCHAR(200)"),
            ("anaf_organ_fiscal", "VARCHAR(200)"),
            ("anaf_platitor_tva", "BOOLEAN"),
            ("anaf_tva_incasare", "BOOLEAN"),
            ("anaf_split_tva", "BOOLEAN"),
            ("anaf_inactiv", "BOOLEAN"),
            ("anaf_e_factura", "BOOLEAN"),
            ("anaf_sediu_judet", "VARCHAR(100)"),
            ("anaf_sediu_localitate", "VARCHAR(200)"),
            ("anaf_sediu_strada", "VARCHAR(300)"),
            ("anaf_sediu_numar", "VARCHAR(50)"),
            ("anaf_sediu_cod_postal", "VARCHAR(20)"),
            ("anaf_last_sync", "TIMESTAMP"),
            ("anaf_sync_status", "VARCHAR(50)"),
            # MFinante columns - Date identificare
            ("mf_denumire", "VARCHAR(500)"),
            ("mf_adresa", "TEXT"),
            ("mf_judet", "VARCHAR(100)"),
            ("mf_cod_postal", "VARCHAR(20)"),
            ("mf_telefon", "VARCHAR(50)"),
            ("mf_nr_reg_com", "VARCHAR(50)"),
            ("mf_stare", "VARCHAR(200)"),
            # MFinante columns - Date fiscale
            ("mf_platitor_tva", "BOOLEAN"),
            ("mf_tva_data", "VARCHAR(200)"),
            ("mf_impozit_profit", "VARCHAR(200)"),
            ("mf_impozit_micro", "VARCHAR(200)"),
            ("mf_accize", "BOOLEAN"),
            ("mf_cas_data", "VARCHAR(200)"),
            # MFinante columns - Bilanț cel mai recent
            ("mf_an_bilant", "VARCHAR(10)"),
            ("mf_cifra_afaceri", "FLOAT"),
            ("mf_venituri_totale", "FLOAT"),
            ("mf_cheltuieli_totale", "FLOAT"),
            ("mf_profit_brut", "FLOAT"),
            ("mf_pierdere_bruta", "FLOAT"),
            ("mf_profit_net", "FLOAT"),
            ("mf_pierdere_neta", "FLOAT"),
            ("mf_numar_angajati", "INTEGER"),
            ("mf_active_imobilizate", "FLOAT"),
            ("mf_active_circulante", "FLOAT"),
            ("mf_capitaluri_proprii", "FLOAT"),
            ("mf_datorii", "FLOAT"),
            # MFinante metadata
            ("mf_ani_disponibili", "VARCHAR(200)"),
            ("mf_last_sync", "TIMESTAMP"),
            ("mf_sync_status", "VARCHAR(50)")
        ]
        
        added = []
        for col_name, col_type in new_columns:
            try:
                await database.execute(f"""
                    ALTER TABLE firme ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                """)
                added.append(col_name)
            except Exception as e:
                logger.warning(f"Could not add column {col_name}: {e}")
        
        # Also ensure timeline_events table exists
        try:
            await database.execute("""
                CREATE TABLE IF NOT EXISTS timeline_events (
                    id BIGSERIAL PRIMARY KEY,
                    dosar_id BIGINT REFERENCES dosare(id),
                    tip VARCHAR(50),
                    data TIMESTAMP,
                    descriere TEXT,
                    detalii JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
        except Exception as e:
            logger.warning(f"Could not create timeline_events: {e}")
        
        # Create bilanturi table for historical financial data
        try:
            await database.execute("""
                CREATE TABLE IF NOT EXISTS bilanturi (
                    id BIGSERIAL PRIMARY KEY,
                    firma_id BIGINT REFERENCES firme(id) ON DELETE CASCADE,
                    an VARCHAR(10) NOT NULL,
                    
                    -- Indicatori principali
                    cifra_afaceri_neta FLOAT,
                    venituri_totale FLOAT,
                    cheltuieli_totale FLOAT,
                    profit_brut FLOAT,
                    pierdere_bruta FLOAT,
                    profit_net FLOAT,
                    pierdere_neta FLOAT,
                    numar_angajati INTEGER,
                    
                    -- Indicatori patrimoniali (active)
                    active_imobilizate FLOAT,
                    active_circulante FLOAT,
                    stocuri FLOAT,
                    creante FLOAT,
                    casa_conturi_banci FLOAT,
                    cheltuieli_avans FLOAT,
                    
                    -- Pasive
                    capitaluri_proprii FLOAT,
                    capital_subscris FLOAT,
                    patrimoniul_regiei FLOAT,
                    provizioane FLOAT,
                    datorii FLOAT,
                    venituri_avans FLOAT,
                    
                    -- Alte
                    repartizare_profit FLOAT,
                    raw_data JSONB,
                    
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    
                    UNIQUE(firma_id, an)
                )
            """)
            added.append("TABLE:bilanturi")
            
            # Create indexes for bilanturi
            await database.execute("""
                CREATE INDEX IF NOT EXISTS idx_bilanturi_firma_id ON bilanturi(firma_id)
            """)
            await database.execute("""
                CREATE INDEX IF NOT EXISTS idx_bilanturi_an ON bilanturi(an)
            """)
        except Exception as e:
            logger.warning(f"Could not create bilanturi table: {e}")
        
        # Create index on judet for filtering
        try:
            await database.execute("""
                CREATE INDEX IF NOT EXISTS idx_firme_judet ON firme(judet)
            """)
        except Exception:
            pass
        
        return {
            "success": True,
            "columns_added": added,
            "message": f"Schema migration complete. Added {len(added)} columns."
        }
    except Exception as e:
        logger.error(f"Error in schema migration: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/diagnostics/indexes")
async def get_database_indexes():
    """Get list of all indexes in the database"""
    try:
        indexes = await database.fetch_all("""
            SELECT 
                indexname,
                tablename,
                indexdef
            FROM pg_indexes 
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)
        
        return [
            {
                "name": r["indexname"],
                "table": r["tablename"],
                "definition": r["indexdef"]
            }
            for r in indexes
        ]
    except Exception as e:
        logger.error(f"Error fetching indexes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/diagnostics/create-indexes")
async def create_performance_indexes():
    """Create indexes for better performance"""
    try:
        created = []
        
        # Index on firme.denumire_normalized for faster searches
        try:
            await database.execute("""
                CREATE INDEX IF NOT EXISTS idx_firme_denumire_normalized 
                ON firme(denumire_normalized)
            """)
            created.append("idx_firme_denumire_normalized")
        except Exception:
            pass
        
        # Index on firme.cui for faster lookups
        try:
            await database.execute("""
                CREATE INDEX IF NOT EXISTS idx_firme_cui 
                ON firme(cui) WHERE cui IS NOT NULL AND cui != ''
            """)
            created.append("idx_firme_cui")
        except Exception:
            pass
        
        # Index on dosare.firma_id for faster joins
        try:
            await database.execute("""
                CREATE INDEX IF NOT EXISTS idx_dosare_firma_id 
                ON dosare(firma_id)
            """)
            created.append("idx_dosare_firma_id")
        except Exception:
            pass
        
        # Index on dosare.numar_dosar
        try:
            await database.execute("""
                CREATE INDEX IF NOT EXISTS idx_dosare_numar 
                ON dosare(numar_dosar)
            """)
            created.append("idx_dosare_numar")
        except Exception:
            pass
        
        # Index on timeline_events.dosar_id
        try:
            await database.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_dosar_id 
                ON timeline_events(dosar_id)
            """)
            created.append("idx_timeline_dosar_id")
        except Exception:
            pass
        
        # Run analyze after creating indexes
        await database.execute("ANALYZE firme")
        await database.execute("ANALYZE dosare")
        await database.execute("ANALYZE timeline_events")
        
        return {
            "success": True,
            "created_indexes": created,
            "message": f"Created {len(created)} indexes"
        }
    except Exception as e:
        logger.error(f"Error creating indexes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# ANAF SYNC ENDPOINTS
# ============================================

ANAF_API_URL = "https://webservicesp.anaf.ro/api/PlatitorTvaRest/v9/tva"
ANAF_BATCH_SIZE = 100  # Max 100 CUIs per request
ANAF_RATE_LIMIT_SECONDS = 1.1  # 1 request per second + buffer


@api_router.get("/anaf/sync-progress")
async def get_anaf_sync_progress():
    """Get current ANAF sync progress"""
    return anaf_sync_progress


@api_router.get("/anaf/stats")
async def get_anaf_stats():
    """Get ANAF sync statistics"""
    db = SessionLocal()
    try:
        total_firme = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '').count()
        synced = db.query(Firma).filter(Firma.anaf_last_sync.isnot(None)).count()
        found = db.query(Firma).filter(Firma.anaf_sync_status == 'found').count()
        not_found = db.query(Firma).filter(Firma.anaf_sync_status == 'not_found').count()
        errors = db.query(Firma).filter(Firma.anaf_sync_status == 'error').count()
        active = db.query(Firma).filter(Firma.anaf_stare.ilike('%ACTIV%'), ~Firma.anaf_stare.ilike('%INACTIV%'), ~Firma.anaf_stare.ilike('%RADIERE%')).count()
        radiate = db.query(Firma).filter(Firma.anaf_stare.ilike('%RADIERE%')).count()
        platitori_tva = db.query(Firma).filter(Firma.anaf_platitor_tva == True).count()
        e_factura = db.query(Firma).filter(Firma.anaf_e_factura == True).count()
        
        return {
            "total_firme_cu_cui": total_firme,
            "synced": synced,
            "not_synced": total_firme - synced,
            "found": found,
            "not_found": not_found,
            "errors": errors,
            "active": active,
            "radiate": radiate,
            "platitori_tva": platitori_tva,
            "e_factura": e_factura
        }
    finally:
        db.close()


@api_router.post("/anaf/sync")
async def start_anaf_sync(
    background_tasks: BackgroundTasks,
    limit: int = None,
    only_unsynced: bool = True,
    judet: str = None
):
    """
    Start ANAF sync for companies.
    - limit: max number of companies to sync (None = all)
    - only_unsynced: only sync companies without anaf_last_sync
    - judet: filter by judet
    """
    global anaf_sync_progress
    
    if anaf_sync_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync already in progress")
    
    # Reset progress
    anaf_sync_progress = {
        "active": True,
        "total_firms": 0,
        "processed": 0,
        "found": 0,
        "not_found": 0,
        "errors": 0,
        "current_batch": 0,
        "total_batches": 0,
        "last_update": datetime.utcnow().isoformat(),
        "eta_seconds": None
    }
    
    # Start background task
    background_tasks.add_task(run_anaf_sync, limit, only_unsynced, judet)
    
    return {"message": "ANAF sync started", "status": "running"}


async def run_anaf_sync(limit: int, only_unsynced: bool, judet: str):
    """Background task to sync with ANAF API"""
    global anaf_sync_progress
    
    db = SessionLocal()
    try:
        # Build query
        query = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '')
        
        if only_unsynced:
            query = query.filter(Firma.anaf_last_sync.is_(None))
        
        if judet:
            query = query.filter(Firma.judet.ilike(f"%{judet}%"))
        
        if limit:
            query = query.limit(limit)
        
        firms = query.all()
        total = len(firms)
        
        anaf_sync_progress["total_firms"] = total
        anaf_sync_progress["total_batches"] = (total + ANAF_BATCH_SIZE - 1) // ANAF_BATCH_SIZE
        
        logger.info(f"[ANAF] Starting sync for {total} firms in {anaf_sync_progress['total_batches']} batches")
        
        start_time = datetime.utcnow()
        
        # Process in batches of 100
        for batch_num in range(0, total, ANAF_BATCH_SIZE):
            batch = firms[batch_num:batch_num + ANAF_BATCH_SIZE]
            anaf_sync_progress["current_batch"] = batch_num // ANAF_BATCH_SIZE + 1
            
            # Prepare request
            today = datetime.utcnow().strftime("%Y-%m-%d")
            request_data = [{"cui": int(f.cui), "data": today} for f in batch if f.cui and f.cui.isdigit()]
            
            if not request_data:
                continue
            
            try:
                # Call ANAF API
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        ANAF_API_URL,
                        json=request_data,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Process found companies
                            found_map = {}
                            for item in data.get("found", []):
                                cui = str(item.get("date_generale", {}).get("cui", ""))
                                found_map[cui] = item
                            
                            # Update database
                            for firma in batch:
                                if firma.cui in found_map:
                                    item = found_map[firma.cui]
                                    dg = item.get("date_generale", {})
                                    tva = item.get("inregistrare_scop_Tva", {})
                                    rtvai = item.get("inregistrare_RTVAI", {})
                                    inactiv = item.get("stare_inactiv", {})
                                    split = item.get("inregistrare_SplitTVA", {})
                                    sediu = item.get("adresa_sediu_social", {})
                                    
                                    firma.anaf_denumire = dg.get("denumire")
                                    firma.anaf_adresa = dg.get("adresa")
                                    firma.anaf_nr_reg_com = dg.get("nrRegCom")
                                    firma.anaf_telefon = dg.get("telefon")
                                    firma.anaf_fax = dg.get("fax")
                                    firma.anaf_cod_postal = dg.get("codPostal")
                                    firma.anaf_stare = dg.get("stare_inregistrare")
                                    firma.anaf_data_inregistrare = dg.get("data_inregistrare")
                                    firma.anaf_cod_caen = dg.get("cod_CAEN")
                                    firma.anaf_forma_juridica = dg.get("forma_juridica")
                                    firma.anaf_forma_organizare = dg.get("forma_organizare")
                                    firma.anaf_forma_proprietate = dg.get("forma_de_proprietate")
                                    firma.anaf_organ_fiscal = dg.get("organFiscalCompetent")
                                    
                                    firma.anaf_platitor_tva = tva.get("scpTVA", False)
                                    firma.anaf_tva_incasare = rtvai.get("statusTvaIncasare", False)
                                    firma.anaf_split_tva = split.get("statusSplitTVA", False)
                                    firma.anaf_inactiv = inactiv.get("statusInactivi", False)
                                    firma.anaf_e_factura = dg.get("statusRO_e_Factura", False)
                                    
                                    firma.anaf_sediu_judet = sediu.get("sdenumire_Judet")
                                    firma.anaf_sediu_localitate = sediu.get("sdenumire_Localitate")
                                    firma.anaf_sediu_strada = sediu.get("sdenumire_Strada")
                                    firma.anaf_sediu_numar = sediu.get("snumar_Strada")
                                    firma.anaf_sediu_cod_postal = sediu.get("scod_Postal")
                                    
                                    firma.anaf_last_sync = datetime.utcnow()
                                    firma.anaf_sync_status = "found"
                                    anaf_sync_progress["found"] += 1
                                else:
                                    firma.anaf_last_sync = datetime.utcnow()
                                    firma.anaf_sync_status = "not_found"
                                    anaf_sync_progress["not_found"] += 1
                                
                                anaf_sync_progress["processed"] += 1
                            
                            db.commit()
                        else:
                            logger.error(f"[ANAF] API error: {response.status}")
                            for firma in batch:
                                firma.anaf_sync_status = "error"
                                anaf_sync_progress["errors"] += 1
                                anaf_sync_progress["processed"] += 1
                            db.commit()
                            
            except Exception as e:
                logger.error(f"[ANAF] Batch error: {e}")
                for firma in batch:
                    firma.anaf_sync_status = "error"
                    anaf_sync_progress["errors"] += 1
                    anaf_sync_progress["processed"] += 1
                db.commit()
            
            # Update progress and ETA
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if anaf_sync_progress["processed"] > 0:
                rate = anaf_sync_progress["processed"] / elapsed
                remaining = total - anaf_sync_progress["processed"]
                anaf_sync_progress["eta_seconds"] = int(remaining / rate) if rate > 0 else None
            
            anaf_sync_progress["last_update"] = datetime.utcnow().isoformat()
            
            # Log progress every 10 batches
            if anaf_sync_progress["current_batch"] % 10 == 0:
                logger.info(f"[ANAF] Progress: {anaf_sync_progress['processed']}/{total} ({anaf_sync_progress['found']} found, {anaf_sync_progress['not_found']} not found)")
            
            # Rate limiting
            await asyncio.sleep(ANAF_RATE_LIMIT_SECONDS)
        
        logger.info(f"[ANAF] Sync complete: {anaf_sync_progress['processed']} processed, {anaf_sync_progress['found']} found, {anaf_sync_progress['not_found']} not found")
        
    except Exception as e:
        logger.error(f"[ANAF] Sync error: {e}")
    finally:
        db.close()
        anaf_sync_progress["active"] = False


@api_router.post("/anaf/sync-stop")
async def stop_anaf_sync():
    """Stop the ANAF sync (will stop after current batch)"""
    global anaf_sync_progress
    anaf_sync_progress["active"] = False
    return {"message": "Sync stop requested"}


@api_router.get("/anaf/test/{cui}")
async def test_anaf_api(cui: str):
    """Test ANAF API with a single CUI"""
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        request_data = [{"cui": int(cui), "data": today}]
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                ANAF_API_URL,
                json=request_data,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                return await response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# MFINANTE CRAWLER (pentru bilanțuri)
# ============================================

MFINANTE_URL = "https://mfinante.gov.ro/apps/infocodfiscal.html"

# MFinante sync progress
mfinante_sync_progress = {
    "active": False,
    "session_valid": False,
    "total_firms": 0,
    "processed": 0,
    "found": 0,
    "errors": 0,
    "last_update": None,
    "last_cui": None
}

# Store session cookies
mfinante_session = {
    "jsessionid": None,
    "cookies": {}
}

# Store for CAPTCHA session
captcha_session = {
    "cookies": None,
    "jsessionid": None
}


@api_router.get("/mfinante/captcha/init")
async def init_mfinante_captcha():
    """
    Initialize a new CAPTCHA session - fetches the MFinante page to get cookies and session.
    Returns the CAPTCHA image URL that can be displayed to the user.
    """
    global captcha_session
    
    try:
        # Create a new session and fetch the page
        jar = aiohttp.CookieJar()
        async with aiohttp.ClientSession(cookie_jar=jar) as session:
            # First request to get initial cookies
            async with session.get(
                MFINANTE_URL,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
                },
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True
            ) as response:
                html = await response.text()
                
                # Extract jsessionid from URL or cookies
                jsessionid = None
                
                # Check URL for jsessionid
                final_url = str(response.url)
                if "jsessionid=" in final_url:
                    jsessionid = final_url.split("jsessionid=")[1].split("?")[0].split(";")[0]
                
                # Also check cookies
                cookies_dict = {}
                for cookie in jar:
                    cookies_dict[cookie.key] = cookie.value
                    if cookie.key.upper() == "JSESSIONID":
                        jsessionid = cookie.value
                
                if not jsessionid:
                    # Try to find in Set-Cookie header
                    for key, val in response.headers.items():
                        if key.lower() == "set-cookie" and "jsessionid" in val.lower():
                            parts = val.split(";")
                            for p in parts:
                                if "jsessionid" in p.lower():
                                    jsessionid = p.split("=")[1].strip()
                                    break
                
                if not jsessionid:
                    raise HTTPException(status_code=500, detail="Could not obtain session from MFinante")
                
                # Store for later use
                captcha_session["jsessionid"] = jsessionid
                captcha_session["cookies"] = cookies_dict
                
                # Generate timestamp to prevent caching
                import time
                timestamp = int(time.time() * 1000)
                
                return {
                    "success": True,
                    "jsessionid": jsessionid,
                    "captcha_url": f"/api/mfinante/captcha/image?t={timestamp}",
                    "message": "CAPTCHA session initialized. Load the captcha image and solve it."
                }
                
    except Exception as e:
        logger.error(f"[MFINANTE] CAPTCHA init error: {e}")
        raise HTTPException(status_code=500, detail=f"Error initializing CAPTCHA: {str(e)}")


@api_router.get("/mfinante/captcha/image")
async def get_mfinante_captcha_image():
    """
    Fetch and return the CAPTCHA image from MFinante.
    Must call /mfinante/captcha/init first.
    """
    from fastapi.responses import Response
    
    if not captcha_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No CAPTCHA session. Call /mfinante/captcha/init first.")
    
    try:
        # The CAPTCHA image URL
        captcha_url = f"https://mfinante.gov.ro/apps/kaptcha.jpg;jsessionid={captcha_session['jsessionid']}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                captcha_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                    "Referer": MFINANTE_URL,
                },
                cookies=captcha_session.get("cookies", {}),
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
                
    except Exception as e:
        logger.error(f"[MFINANTE] CAPTCHA image error: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching CAPTCHA: {str(e)}")


@api_router.post("/mfinante/captcha/solve")
async def solve_mfinante_captcha(captcha_code: str, test_cui: str = "14918042"):
    """
    Submit the CAPTCHA solution and validate the session.
    If successful, the session is automatically set for future requests.
    """
    global mfinante_session, captcha_session
    
    if not captcha_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No CAPTCHA session. Call /mfinante/captcha/init first.")
    
    try:
        # Submit the form with CAPTCHA
        url = f"{MFINANTE_URL};jsessionid={captcha_session['jsessionid']}"
        
        form_data = {
            "cod": test_cui,
            "captcha": captcha_code,
            "method.vizualizare": "VIZUALIZARE"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                data=form_data,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": MFINANTE_URL,
                },
                cookies=captcha_session.get("cookies", {}),
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True
            ) as response:
                html = await response.text()
                
                # Check if CAPTCHA was correct
                if "Cod de validare" in html and "kaptcha" in html:
                    # CAPTCHA was wrong, still showing CAPTCHA page
                    return {
                        "success": False,
                        "error": "CAPTCHA incorect. Încercați din nou.",
                        "need_new_captcha": True
                    }
                
                # Check if we got actual data
                if "Date de identificare" in html or "Denumire" in html or "AGENTUL ECONOMIC" in html:
                    # Success! Set the main session
                    mfinante_session["jsessionid"] = captcha_session["jsessionid"]
                    mfinante_session["cookies"] = captcha_session.get("cookies", {})
                    
                    # Update sync progress
                    mfinante_sync_progress["session_valid"] = True
                    
                    return {
                        "success": True,
                        "message": "CAPTCHA rezolvat cu succes! Sesiunea a fost setată.",
                        "session_valid": True,
                        "jsessionid": captcha_session["jsessionid"][:20] + "..."
                    }
                
                # Unknown response
                return {
                    "success": False,
                    "error": "Răspuns neașteptat de la MFinante. Încercați din nou.",
                    "need_new_captcha": True
                }
                
    except Exception as e:
        logger.error(f"[MFINANTE] CAPTCHA solve error: {e}")
        raise HTTPException(status_code=500, detail=f"Error solving CAPTCHA: {str(e)}")


@api_router.get("/mfinante/session-status")
async def get_mfinante_session_status():
    """Get current MFinante session status"""
    return {
        "session_valid": mfinante_session.get("jsessionid") is not None,
        "jsessionid": mfinante_session.get("jsessionid", "")[:20] + "..." if mfinante_session.get("jsessionid") else None,
        "progress": mfinante_sync_progress
    }


@api_router.post("/mfinante/set-session")
async def set_mfinante_session(jsessionid: str, cookies: dict = None):
    """
    Set MFinante session after manually solving CAPTCHA.
    
    How to get the session:
    1. Go to https://mfinante.gov.ro/apps/infocodfiscal.html
    2. Solve the CAPTCHA and submit with any CUI
    3. Open DevTools (F12) -> Network tab
    4. Find the request to infocodfiscal.html
    5. Copy the jsessionid from the URL or Cookie header
    """
    global mfinante_session
    mfinante_session["jsessionid"] = jsessionid
    mfinante_session["cookies"] = cookies or {}
    
    # Test the session
    test_valid = await test_mfinante_session()
    
    return {
        "success": True,
        "session_set": True,
        "session_valid": test_valid,
        "message": "Session set. " + ("Session is valid!" if test_valid else "Session may be invalid, try solving CAPTCHA again.")
    }


async def test_mfinante_session():
    """Test if the MFinante session is valid"""
    if not mfinante_session.get("jsessionid"):
        return False
    
    try:
        url = f"{MFINANTE_URL};jsessionid={mfinante_session['jsessionid']}?cod=14918042"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
                },
                cookies=mfinante_session.get("cookies", {}),
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True
            ) as response:
                text = await response.text()
                # Check if we got actual data (not CAPTCHA page)
                if "Cod de validare" in text and "kaptcha" in text:
                    return False  # Still asking for CAPTCHA
                if "Date de identificare" in text or "Denumire" in text:
                    return True
                return False
    except Exception as e:
        logger.error(f"[MFINANTE] Session test error: {e}")
        return False


@api_router.get("/mfinante/test/{cui}")
async def test_mfinante_cui(cui: str):
    """Test MFinante with a single CUI using current session"""
    if not mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session set. Please solve CAPTCHA first.")
    
    try:
        data = await fetch_mfinante_data(cui)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def fetch_mfinante_data(cui: str):
    """Fetch company data from MFinante"""
    import re
    from bs4 import BeautifulSoup
    
    url = f"{MFINANTE_URL};jsessionid={mfinante_session['jsessionid']}?cod={cui}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
                "Referer": MFINANTE_URL,
            },
            cookies=mfinante_session.get("cookies", {}),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            html = await response.text()
    
    # Check if session expired
    if "Cod de validare" in html and "kaptcha" in html:
        raise Exception("Session expired - CAPTCHA required")
    
    soup = BeautifulSoup(html, 'html.parser')
    
    result = {
        "cui": cui,
        "found": False,
        "date_identificare": {},
        "date_fiscale": {},
        "bilanturi_disponibili": [],
        "raw_text": ""
    }
    
    # Check if company was found
    if "AGENTUL ECONOMIC CU CODUL" not in html:
        return result
    
    result["found"] = True
    
    # Parse all rows (col-sm-6 pairs)
    rows = soup.find_all('div', class_='row')
    
    for row in rows:
        cols = row.find_all('div', class_='col-sm-6')
        if len(cols) >= 2:
            label = cols[0].get_text(strip=True).lower()
            value = cols[1].get_text(strip=True)
            
            # Date identificare
            if 'denumire' in label:
                result["date_identificare"]["denumire"] = value
            elif 'adresa' in label and 'adresa' not in result["date_identificare"]:
                result["date_identificare"]["adresa"] = value
            elif 'judetul' in label:
                result["date_identificare"]["judet"] = value
            elif 'inmatriculare' in label or 'registrul' in label:
                result["date_identificare"]["nr_reg_com"] = value
            elif 'postal' in label:
                result["date_identificare"]["cod_postal"] = value
            elif 'telefon' in label:
                result["date_identificare"]["telefon"] = value
            elif 'stare societate' in label:
                result["date_identificare"]["stare"] = value
            
            # Date fiscale
            elif 'taxa pe valoarea' in label or 'tva' in label.lower():
                result["date_fiscale"]["tva_data"] = value
                result["date_fiscale"]["platitor_tva"] = value != 'NU' and 'NU' not in value
            elif 'impozit pe profit' in label:
                result["date_fiscale"]["impozit_profit"] = value
            elif 'microintreprinderi' in label:
                result["date_fiscale"]["micro_data"] = value
            elif 'accize' in label:
                result["date_fiscale"]["accize"] = value != 'NU'
            elif 'asigurari sociale' in label and 'sanatate' not in label:
                result["date_fiscale"]["cas_data"] = value
    
    # Parse available balance sheet years
    selects = soup.find_all('select', {'name': 'an'})
    for select in selects:
        options = select.find_all('option')
        for opt in options:
            value = opt.get('value', '')
            text = opt.get_text(strip=True)
            if value and text:
                result["bilanturi_disponibili"].append({"an": text, "value": value})
    
    return result


async def fetch_mfinante_bilant(cui: str, an_value: str):
    """Fetch balance sheet for a specific year - extracts ALL available financial indicators"""
    import re
    from bs4 import BeautifulSoup
    
    url = f"{MFINANTE_URL};jsessionid={mfinante_session['jsessionid']}"
    
    # POST request for balance sheet
    data = {
        "cod": cui,
        "an": an_value,
        "method.bilant": "VIZUALIZARE"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            data=data,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": f"{MFINANTE_URL}?cod={cui}",
            },
            cookies=mfinante_session.get("cookies", {}),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            html = await response.text()
    
    if "Cod de validare" in html and "kaptcha" in html:
        raise Exception("Session expired - CAPTCHA required")
    
    soup = BeautifulSoup(html, 'html.parser')
    
    bilant = {
        "an": an_value.replace("WEB_UU_AN", "") if "WEB_UU_AN" in an_value else an_value,
        "indicatori": {},
        "raw_labels": []  # Pentru debugging - să vedem ce etichete există
    }
    
    def parse_value(value_text):
        """Parse a Romanian number format to float"""
        if not value_text or value_text == '-' or value_text.strip() == '':
            return None
        # Remove spaces and convert
        clean_value = re.sub(r'[^\d.,\-]', '', value_text)
        if clean_value:
            try:
                # Romanian format: 1.234.567,89 -> 1234567.89
                clean_value = clean_value.replace('.', '').replace(',', '.')
                return float(clean_value)
            except:
                return None
        return None
    
    # Parse financial indicators from all rows
    rows = soup.find_all('div', class_='row')
    for row in rows:
        cols = row.find_all('div', class_='col-sm-6')
        if len(cols) >= 2:
            label = cols[0].get_text(strip=True).lower()
            value_text = cols[1].get_text(strip=True)
            value = parse_value(value_text)
            
            # Store raw label for debugging
            if label and len(label) > 3:
                bilant["raw_labels"].append({"label": label, "value": value_text})
            
            # Indicatori principali - Cont Profit și Pierdere
            if 'cifra' in label and 'afaceri' in label and 'neta' in label:
                bilant["indicatori"]["cifra_afaceri_neta"] = value
            elif 'venituri totale' in label:
                bilant["indicatori"]["venituri_totale"] = value
            elif 'cheltuieli totale' in label:
                bilant["indicatori"]["cheltuieli_totale"] = value
            elif 'profit brut' in label:
                bilant["indicatori"]["profit_brut"] = value
            elif 'pierdere brut' in label:
                bilant["indicatori"]["pierdere_bruta"] = value
            elif 'profit net' in label:
                bilant["indicatori"]["profit_net"] = value
            elif 'pierdere net' in label:
                bilant["indicatori"]["pierdere_neta"] = value
            elif 'numar mediu' in label and 'salariati' in label:
                bilant["indicatori"]["numar_angajati"] = int(value) if value else None
            
            # Active
            elif 'active imobilizate' in label and 'total' not in label:
                bilant["indicatori"]["active_imobilizate"] = value
            elif 'active circulante' in label and 'total' not in label:
                bilant["indicatori"]["active_circulante"] = value
            elif 'stocuri' in label:
                bilant["indicatori"]["stocuri"] = value
            elif 'creante' in label:
                bilant["indicatori"]["creante"] = value
            elif 'casa' in label and 'banci' in label:
                bilant["indicatori"]["casa_conturi_banci"] = value
            elif 'cheltuieli' in label and 'avans' in label:
                bilant["indicatori"]["cheltuieli_avans"] = value
            
            # Pasive
            elif 'capitaluri proprii' in label or 'capital propriu' in label:
                bilant["indicatori"]["capitaluri_proprii"] = value
            elif 'capital subscris' in label or 'capital social' in label:
                bilant["indicatori"]["capital_subscris"] = value
            elif 'patrimoniul regiei' in label:
                bilant["indicatori"]["patrimoniul_regiei"] = value
            elif 'provizioane' in label:
                bilant["indicatori"]["provizioane"] = value
            elif 'datorii' in label and 'total' not in label:
                bilant["indicatori"]["datorii"] = value
            elif 'venituri' in label and 'avans' in label:
                bilant["indicatori"]["venituri_avans"] = value
            
            # Alte
            elif 'repartizare' in label and 'profit' in label:
                bilant["indicatori"]["repartizare_profit"] = value
    
    return bilant


@api_router.get("/mfinante/bilant/{cui}/{an}")
async def get_mfinante_bilant(cui: str, an: str):
    """Get balance sheet for a specific year"""
    if not mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session set")
    
    try:
        # Format an_value
        an_value = f"WEB_UU_AN{an}" if not an.startswith("WEB_") else an
        bilant = await fetch_mfinante_bilant(cui, an_value)
        return bilant
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/mfinante/full/{cui}")
async def get_mfinante_full(cui: str):
    """Get all available data including latest balance sheet"""
    if not mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session set")
    
    try:
        # Get basic data
        data = await fetch_mfinante_data(cui)
        
        # Get latest balance sheet if available
        if data["found"] and data["bilanturi_disponibili"]:
            # Get the most recent year
            latest = data["bilanturi_disponibili"][-1]
            try:
                bilant = await fetch_mfinante_bilant(cui, latest["value"])
                data["bilant_recent"] = bilant
            except Exception as e:
                data["bilant_error"] = str(e)
        
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/mfinante/sync")
async def start_mfinante_sync(
    background_tasks: BackgroundTasks,
    limit: int = 100,
    only_without_bilant: bool = True
):
    """
    Start MFinante sync for companies.
    Requires valid session (solve CAPTCHA first).
    """
    global mfinante_sync_progress
    
    if not mfinante_session.get("jsessionid"):
        raise HTTPException(status_code=400, detail="No session set. Please solve CAPTCHA first at /mfinante/set-session")
    
    if mfinante_sync_progress["active"]:
        raise HTTPException(status_code=400, detail="Sync already in progress")
    
    # Reset progress
    mfinante_sync_progress = {
        "active": True,
        "session_valid": True,
        "total_firms": 0,
        "processed": 0,
        "found": 0,
        "errors": 0,
        "last_update": datetime.utcnow().isoformat(),
        "last_cui": None
    }
    
    background_tasks.add_task(run_mfinante_sync, limit, only_without_bilant)
    
    return {"message": "MFinante sync started", "status": "running"}


async def run_mfinante_sync(limit: int, only_without_bilant: bool):
    """Background task to sync with MFinante - saves ALL data including historical balance sheets"""
    global mfinante_sync_progress
    
    db = SessionLocal()
    try:
        # Get firms to sync
        query = db.query(Firma).filter(Firma.cui.isnot(None), Firma.cui != '')
        
        if only_without_bilant:
            # Only firms without financial data
            query = query.filter(
                (Firma.mf_cifra_afaceri.is_(None)) | 
                (Firma.mf_last_sync.is_(None))
            )
        
        query = query.limit(limit)
        firms = query.all()
        
        mfinante_sync_progress["total_firms"] = len(firms)
        logger.info(f"[MFINANTE] Starting sync for {len(firms)} firms")
        
        for firma in firms:
            if not mfinante_sync_progress["active"]:
                logger.info("[MFINANTE] Sync stopped by user")
                break
            
            mfinante_sync_progress["last_cui"] = firma.cui
            
            try:
                data = await fetch_mfinante_data(firma.cui)
                
                if data.get("found"):
                    # === Update firma with identification data ===
                    di = data.get("date_identificare", {})
                    df = data.get("date_fiscale", {})
                    
                    # Date identificare
                    firma.mf_denumire = di.get("denumire")
                    firma.mf_adresa = di.get("adresa")
                    firma.mf_judet = di.get("judet")
                    firma.mf_cod_postal = di.get("cod_postal")
                    firma.mf_telefon = di.get("telefon")
                    firma.mf_nr_reg_com = di.get("nr_reg_com")
                    firma.mf_stare = di.get("stare")
                    
                    # Date fiscale
                    firma.mf_platitor_tva = df.get("platitor_tva")
                    firma.mf_tva_data = df.get("tva_data")
                    firma.mf_impozit_profit = df.get("impozit_profit")
                    firma.mf_impozit_micro = df.get("micro_data")
                    firma.mf_accize = df.get("accize")
                    firma.mf_cas_data = df.get("cas_data")
                    
                    # Store available years
                    ani_disponibili = [b["an"] for b in data.get("bilanturi_disponibili", [])]
                    firma.mf_ani_disponibili = ",".join(ani_disponibili) if ani_disponibili else None
                    
                    # === Fetch ALL available balance sheets ===
                    bilanturi_disponibili = data.get("bilanturi_disponibili", [])
                    latest_bilant = None
                    
                    for bilant_info in bilanturi_disponibili:
                        try:
                            await asyncio.sleep(0.5)  # Small delay between requests
                            
                            bilant_data = await fetch_mfinante_bilant(firma.cui, bilant_info["value"])
                            indicatori = bilant_data.get("indicatori", {})
                            an = bilant_data.get("an", bilant_info["an"])
                            
                            if indicatori:
                                # Check if bilant already exists for this firma+year
                                existing = db.query(Bilant).filter(
                                    Bilant.firma_id == firma.id,
                                    Bilant.an == an
                                ).first()
                                
                                if existing:
                                    # Update existing
                                    existing.cifra_afaceri_neta = indicatori.get("cifra_afaceri_neta")
                                    existing.venituri_totale = indicatori.get("venituri_totale")
                                    existing.cheltuieli_totale = indicatori.get("cheltuieli_totale")
                                    existing.profit_brut = indicatori.get("profit_brut")
                                    existing.pierdere_bruta = indicatori.get("pierdere_bruta")
                                    existing.profit_net = indicatori.get("profit_net")
                                    existing.pierdere_neta = indicatori.get("pierdere_neta")
                                    existing.numar_angajati = indicatori.get("numar_angajati")
                                    existing.active_imobilizate = indicatori.get("active_imobilizate")
                                    existing.active_circulante = indicatori.get("active_circulante")
                                    existing.stocuri = indicatori.get("stocuri")
                                    existing.creante = indicatori.get("creante")
                                    existing.casa_conturi_banci = indicatori.get("casa_conturi_banci")
                                    existing.cheltuieli_avans = indicatori.get("cheltuieli_avans")
                                    existing.capitaluri_proprii = indicatori.get("capitaluri_proprii")
                                    existing.capital_subscris = indicatori.get("capital_subscris")
                                    existing.patrimoniul_regiei = indicatori.get("patrimoniul_regiei")
                                    existing.provizioane = indicatori.get("provizioane")
                                    existing.datorii = indicatori.get("datorii")
                                    existing.venituri_avans = indicatori.get("venituri_avans")
                                    existing.repartizare_profit = indicatori.get("repartizare_profit")
                                    existing.raw_data = indicatori
                                    existing.updated_at = datetime.utcnow()
                                else:
                                    # Create new bilant record
                                    new_bilant = Bilant(
                                        firma_id=firma.id,
                                        an=an,
                                        cifra_afaceri_neta=indicatori.get("cifra_afaceri_neta"),
                                        venituri_totale=indicatori.get("venituri_totale"),
                                        cheltuieli_totale=indicatori.get("cheltuieli_totale"),
                                        profit_brut=indicatori.get("profit_brut"),
                                        pierdere_bruta=indicatori.get("pierdere_bruta"),
                                        profit_net=indicatori.get("profit_net"),
                                        pierdere_neta=indicatori.get("pierdere_neta"),
                                        numar_angajati=indicatori.get("numar_angajati"),
                                        active_imobilizate=indicatori.get("active_imobilizate"),
                                        active_circulante=indicatori.get("active_circulante"),
                                        stocuri=indicatori.get("stocuri"),
                                        creante=indicatori.get("creante"),
                                        casa_conturi_banci=indicatori.get("casa_conturi_banci"),
                                        cheltuieli_avans=indicatori.get("cheltuieli_avans"),
                                        capitaluri_proprii=indicatori.get("capitaluri_proprii"),
                                        capital_subscris=indicatori.get("capital_subscris"),
                                        patrimoniul_regiei=indicatori.get("patrimoniul_regiei"),
                                        provizioane=indicatori.get("provizioane"),
                                        datorii=indicatori.get("datorii"),
                                        venituri_avans=indicatori.get("venituri_avans"),
                                        repartizare_profit=indicatori.get("repartizare_profit"),
                                        raw_data=indicatori
                                    )
                                    db.add(new_bilant)
                                
                                # Keep track of latest (most recent year)
                                if latest_bilant is None or an > latest_bilant["an"]:
                                    latest_bilant = {"an": an, "indicatori": indicatori}
                                    
                        except Exception as e:
                            logger.warning(f"[MFINANTE] Could not fetch bilant {bilant_info['an']} for CUI {firma.cui}: {e}")
                            continue
                    
                    # === Update firma with LATEST balance sheet data for quick access ===
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
                    mfinante_sync_progress["found"] += 1
                else:
                    firma.mf_sync_status = "not_found"
                
                db.commit()
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"[MFINANTE] Error for CUI {firma.cui}: {error_msg}")
                
                if "Session expired" in error_msg or "CAPTCHA" in error_msg:
                    mfinante_sync_progress["session_valid"] = False
                    logger.error("[MFINANTE] Session expired! Need new CAPTCHA.")
                    break
                
                firma.mf_sync_status = "error"
                db.commit()
                mfinante_sync_progress["errors"] += 1
            
            mfinante_sync_progress["processed"] += 1
            mfinante_sync_progress["last_update"] = datetime.utcnow().isoformat()
            
            # Rate limiting - be gentle with MFinante
            await asyncio.sleep(2)  # 2 seconds between requests
        
        logger.info(f"[MFINANTE] Sync complete: {mfinante_sync_progress['processed']} processed, {mfinante_sync_progress['found']} found")
        
    except Exception as e:
        logger.error(f"[MFINANTE] Sync error: {e}")
    finally:
        db.close()
        mfinante_sync_progress["active"] = False


@api_router.post("/mfinante/sync-stop")
async def stop_mfinante_sync():
    """Stop the MFinante sync"""
    global mfinante_sync_progress
    mfinante_sync_progress["active"] = False
    return {"message": "MFinante sync stop requested"}


@api_router.get("/mfinante/stats")
async def get_mfinante_stats():
    """Get MFinante sync statistics"""
    db = SessionLocal()
    try:
        total = db.query(Firma).filter(Firma.cui.isnot(None)).count()
        synced = db.query(Firma).filter(Firma.mf_last_sync.isnot(None)).count()
        with_cifra = db.query(Firma).filter(Firma.mf_cifra_afaceri.isnot(None)).count()
        total_bilanturi = db.query(Bilant).count()
        
        return {
            "total_firme": total,
            "synced_mfinante": synced,
            "not_synced": total - synced,
            "with_cifra_afaceri": with_cifra,
            "total_bilanturi_istorice": total_bilanturi,
            "session_status": {
                "has_session": mfinante_session.get("jsessionid") is not None,
                "session_valid": mfinante_sync_progress.get("session_valid", False)
            }
        }
    finally:
        db.close()


@api_router.get("/bilanturi/firma/{firma_id}")
async def get_bilanturi_firma(firma_id: int):
    """Get all historical balance sheets for a company"""
    db = SessionLocal()
    try:
        bilanturi = db.query(Bilant).filter(Bilant.firma_id == firma_id).order_by(Bilant.an.desc()).all()
        
        return [
            {
                "id": b.id,
                "an": b.an,
                "cifra_afaceri_neta": b.cifra_afaceri_neta,
                "venituri_totale": b.venituri_totale,
                "cheltuieli_totale": b.cheltuieli_totale,
                "profit_brut": b.profit_brut,
                "pierdere_bruta": b.pierdere_bruta,
                "profit_net": b.profit_net,
                "pierdere_neta": b.pierdere_neta,
                "numar_angajati": b.numar_angajati,
                "active_imobilizate": b.active_imobilizate,
                "active_circulante": b.active_circulante,
                "stocuri": b.stocuri,
                "creante": b.creante,
                "casa_conturi_banci": b.casa_conturi_banci,
                "capitaluri_proprii": b.capitaluri_proprii,
                "capital_subscris": b.capital_subscris,
                "datorii": b.datorii,
                "created_at": b.created_at.isoformat() if b.created_at else None
            }
            for b in bilanturi
        ]
    finally:
        db.close()


@api_router.get("/bilanturi/cui/{cui}")
async def get_bilanturi_by_cui(cui: str):
    """Get all historical balance sheets for a company by CUI"""
    db = SessionLocal()
    try:
        firma = db.query(Firma).filter(Firma.cui == cui).first()
        if not firma:
            raise HTTPException(status_code=404, detail=f"Firma cu CUI {cui} nu a fost găsită")
        
        bilanturi = db.query(Bilant).filter(Bilant.firma_id == firma.id).order_by(Bilant.an.desc()).all()
        
        return {
            "firma": {
                "id": firma.id,
                "cui": firma.cui,
                "denumire": firma.denumire,
                "mf_denumire": firma.mf_denumire,
                "mf_stare": firma.mf_stare,
                "mf_ani_disponibili": firma.mf_ani_disponibili
            },
            "bilanturi": [
                {
                    "id": b.id,
                    "an": b.an,
                    "cifra_afaceri_neta": b.cifra_afaceri_neta,
                    "venituri_totale": b.venituri_totale,
                    "cheltuieli_totale": b.cheltuieli_totale,
                    "profit_brut": b.profit_brut,
                    "pierdere_bruta": b.pierdere_bruta,
                    "profit_net": b.profit_net,
                    "pierdere_neta": b.pierdere_neta,
                    "numar_angajati": b.numar_angajati,
                    "active_imobilizate": b.active_imobilizate,
                    "active_circulante": b.active_circulante,
                    "capitaluri_proprii": b.capitaluri_proprii,
                    "datorii": b.datorii
                }
                for b in bilanturi
            ]
        }
    finally:
        db.close()


@api_router.get("/bilanturi/stats")
async def get_bilanturi_stats():
    """Get statistics about stored balance sheets"""
    db = SessionLocal()
    try:
        total = db.query(Bilant).count()
        firme_cu_bilanturi = db.query(Bilant.firma_id).distinct().count()
        
        # Get count by year
        from sqlalchemy import func
        by_year = db.query(
            Bilant.an,
            func.count(Bilant.id).label('count')
        ).group_by(Bilant.an).order_by(Bilant.an.desc()).all()
        
        return {
            "total_bilanturi": total,
            "firme_cu_bilanturi": firme_cu_bilanturi,
            "by_year": [{"an": r.an, "count": r.count} for r in by_year]
        }
    finally:
        db.close()


# App setup
app.include_router(api_router)


@app.on_event("startup")
async def startup_event():
    await database.connect()
    scheduler.start()
    config = await mongo_db.job_config.find_one({}, {"_id": 0})
    if config and config.get('cron_enabled'):
        update_scheduler(config.get('schedule_hour', 2), config.get('schedule_minute', 0), True)
    logger.info("Application started with PostgreSQL support")


@app.on_event("shutdown")
async def shutdown_event():
    await database.disconnect()
    scheduler.shutdown()
    mongo_client.close()
