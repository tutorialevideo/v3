from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks, UploadFile, File
from fastapi.responses import FileResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey, JSON, Boolean, BigInteger
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
    cui = Column(String(20), unique=True, nullable=True, index=True)
    denumire = Column(String(500), nullable=False, index=True)
    denumire_normalized = Column(String(500), index=True)  # For search
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
async def get_firme(skip: int = 0, limit: int = 100, search: str = None):
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
            "created_at": firma.created_at.isoformat() if firma.created_at else None,
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


@api_router.post("/db/import-cui")
async def import_cui_csv(
    file: UploadFile = File(...),
    cui_column: int = 1,
    denumire_column: int = 0,
    has_header: bool = False,
    only_companies: bool = True
):
    """
    Import companies from ONRC file directly into database.
    Creates new firms with CUI - later dosare will be matched to these firms.
    
    Default settings for ONRC files:
    - cui_column: 1 (second column)
    - denumire_column: 0 (first column)
    - has_header: false
    - only_companies: true (excludes PFA, II, IF - only SRL, SA, etc.)
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
    
    cui_col_idx = cui_column
    denumire_col_idx = denumire_column
    start_row = 1 if has_header else 0
    
    db = SessionLocal()
    results = {
        "total_rows": 0,
        "processed": 0,
        "skipped_not_company": 0,
        "created_new": 0,
        "already_exists": 0,
        "updated_cui": 0,
        "skipped_no_cui": 0,
        "sample_created": [],
        "delimiter_detected": delimiter
    }
    
    # Build lookup of existing firms by normalized name and by CUI
    existing_by_name = {}
    existing_by_cui = {}
    all_firme = db.query(Firma).all()
    for firma in all_firme:
        if firma.denumire_normalized:
            existing_by_name[firma.denumire_normalized] = firma
        if firma.cui:
            existing_by_cui[firma.cui] = firma
    
    logger.info(f"Existing firms in DB: {len(all_firme)}")
    
    try:
        batch_count = 0
        
        for i, line in enumerate(lines[start_row:], start=start_row):
            if not line.strip():
                continue
                
            results["total_rows"] += 1
            
            cols = line.split(delimiter)
            
            if len(cols) <= max(cui_col_idx, denumire_col_idx):
                continue
            
            denumire_value = cols[denumire_col_idx].strip() if cols[denumire_col_idx] else None
            cui_value = cols[cui_col_idx].strip() if cols[cui_col_idx] else None
            
            if not denumire_value:
                continue
            
            # Filter: only companies (SRL, SA, etc.)
            if only_companies and not is_company(denumire_value):
                results["skipped_not_company"] += 1
                continue
            
            # Skip if no valid CUI
            if not cui_value or cui_value == '0' or len(cui_value) < 2:
                results["skipped_no_cui"] += 1
                continue
            
            results["processed"] += 1
            
            denumire_normalized = normalize_company_name(denumire_value)
            
            # Check if firm already exists by CUI
            existing_firma = existing_by_cui.get(cui_value)
            
            if not existing_firma:
                # Check by normalized name
                existing_firma = existing_by_name.get(denumire_normalized)
            
            if existing_firma:
                # Firm exists
                if existing_firma.cui and existing_firma.cui == cui_value:
                    results["already_exists"] += 1
                elif not existing_firma.cui:
                    # Update CUI if missing
                    existing_firma.cui = cui_value
                    existing_firma.updated_at = datetime.utcnow()
                    results["updated_cui"] += 1
                else:
                    results["already_exists"] += 1
            else:
                # Create new firm
                new_firma = Firma(
                    cui=cui_value,
                    denumire=denumire_value,
                    denumire_normalized=denumire_normalized
                )
                db.add(new_firma)
                
                # Update lookup dictionaries
                existing_by_name[denumire_normalized] = new_firma
                existing_by_cui[cui_value] = new_firma
                
                results["created_new"] += 1
                
                if len(results["sample_created"]) < 10:
                    results["sample_created"].append({
                        "denumire": denumire_value[:50],
                        "cui": cui_value
                    })
            
            batch_count += 1
            if batch_count >= 1000:
                db.commit()
                batch_count = 0
                logger.info(f"Processed {results['total_rows']} rows, created {results['created_new']} new firms")
        
        db.commit()
        logger.info(f"Import complete: {results}")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing file: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    finally:
        db.close()
    
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
        # Table counts
        firme_count = await database.fetch_one("SELECT COUNT(*) as cnt FROM firme")
        dosare_count = await database.fetch_one("SELECT COUNT(*) as cnt FROM dosare")
        timeline_count = await database.fetch_one("SELECT COUNT(*) as cnt FROM timeline_events")
        
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
        
        # Duplicate counts
        denumire_dupes = await database.fetch_one("""
            SELECT COUNT(*) as cnt FROM (
                SELECT denumire_normalized 
                FROM firme 
                GROUP BY denumire_normalized 
                HAVING COUNT(*) > 1
            ) as dupes
        """)
        
        cui_dupes = await database.fetch_one("""
            SELECT COUNT(*) as cnt FROM (
                SELECT cui 
                FROM firme 
                WHERE cui IS NOT NULL AND cui != ''
                GROUP BY cui 
                HAVING COUNT(*) > 1
            ) as dupes
        """)
        
        # Firme without CUI
        no_cui = await database.fetch_one("""
            SELECT COUNT(*) as cnt FROM firme WHERE cui IS NULL OR cui = ''
        """)
        
        # Orphaned dosare (no firma)
        orphaned_dosare = await database.fetch_one("""
            SELECT COUNT(*) as cnt FROM dosare 
            WHERE firma_id NOT IN (SELECT id FROM firme)
        """)
        
        return {
            "counts": {
                "firme": int(firme_count["cnt"]) if firme_count else 0,
                "dosare": int(dosare_count["cnt"]) if dosare_count else 0,
                "timeline_events": int(timeline_count["cnt"]) if timeline_count else 0
            },
            "table_sizes": table_sizes_list,
            "issues": {
                "duplicate_denumiri": int(denumire_dupes["cnt"]) if denumire_dupes else 0,
                "duplicate_cui": int(cui_dupes["cnt"]) if cui_dupes else 0,
                "firme_without_cui": int(no_cui["cnt"]) if no_cui else 0,
                "orphaned_dosare": int(orphaned_dosare["cnt"]) if orphaned_dosare else 0
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
        await database.execute("VACUUM ANALYZE firme")
        await database.execute("VACUUM ANALYZE dosare")
        await database.execute("VACUUM ANALYZE timeline_events")
        
        return {
            "success": True,
            "message": "Database optimized successfully"
        }
    except Exception as e:
        logger.error(f"Error optimizing database: {e}")
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
