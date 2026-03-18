from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
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

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Create downloads directory
DOWNLOADS_DIR = ROOT_DIR / 'downloads'
DOWNLOADS_DIR.mkdir(exist_ok=True)

# Create the main app
app = FastAPI()

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# SOAP Configuration
SOAP_URL = "http://portalquery.just.ro/query.asmx"
SOAP_ACTION = "portalquery.just.ro/CautareDosare"

# Complete list of all 246 institutions from WSDL
INSTITUTII = [
    # Curți de Apel (16)
    "CurteaMilitaradeApelBUCURESTI", "CurteadeApelALBAIULIA", "CurteadeApelBACAU",
    "CurteadeApelBRASOV", "CurteadeApelBUCURESTI", "CurteadeApelCLUJ",
    "CurteadeApelCONSTANTA", "CurteadeApelCRAIOVA", "CurteadeApelGALATI",
    "CurteadeApelIASI", "CurteadeApelORADEA", "CurteadeApelPITESTI",
    "CurteadeApelPLOIESTI", "CurteadeApelSUCEAVA", "CurteadeApelTARGUMURES",
    "CurteadeApelTIMISOARA",
    # Tribunale (52)
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
    # Judecătorii (178)
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


# Models
class JobConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schedule_hour: int = 2  # Default 2 AM
    schedule_minute: int = 0
    search_term: str = ""  # Company name to search
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class JobConfigUpdate(BaseModel):
    schedule_hour: Optional[int] = None
    schedule_minute: Optional[int] = None
    search_term: Optional[str] = None
    is_active: Optional[bool] = None


class JobRun(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    status: str = "running"  # running, completed, failed
    total_records: int = 0
    records_downloaded: int = 0
    error_message: Optional[str] = None
    files_created: List[str] = []


class SearchRequest(BaseModel):
    company_name: str
    institutie: Optional[str] = None


# Helper functions
def build_soap_request(nume_parte: str, institutie: str = "") -> str:
    """Build SOAP request for CautareDosare - simpler endpoint"""
    return f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
               xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CautareDosare xmlns="portalquery.just.ro">
      <numarDosar xsi:nil="true" />
      <obiectDosar xsi:nil="true" />
      <numeParte>{nume_parte}</numeParte>
      <institutie>{institutie}</institutie>
    </CautareDosare>
  </soap:Body>
</soap:Envelope>'''


def parse_soap_response(xml_content: str) -> List[dict]:
    """Parse SOAP response and extract dosare data"""
    try:
        root = ET.fromstring(xml_content)
        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'pq': 'portalquery.just.ro'
        }
        
        dosare = []
        # Find all Dosar elements
        for dosar in root.findall('.//pq:Dosar', namespaces):
            dosar_dict = {}
            for child in dosar:
                tag = child.tag.replace('{portalquery.just.ro}', '')
                dosar_dict[tag] = child.text or ""
                
                # Handle nested elements like Sedinte, Parti, CaiAtac
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


async def fetch_dosare(session: aiohttp.ClientSession, nume_parte: str, institutie: str = "") -> List[dict]:
    """Fetch dosare from portal"""
    soap_body = build_soap_request(nume_parte, institutie)
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': f'"{SOAP_ACTION}"'
    }
    
    try:
        async with session.post(SOAP_URL, data=soap_body, headers=headers, timeout=60) as response:
            if response.status == 200:
                content = await response.text()
                return parse_soap_response(content)
            else:
                logger.error(f"HTTP Error {response.status} for {institutie}")
                return []
    except asyncio.TimeoutError:
        logger.error(f"Timeout for {institutie}")
        return []
    except Exception as e:
        logger.error(f"Error fetching from {institutie}: {e}")
        return []


async def run_download_job(search_term: str, job_run_id: str):
    """Run the download job for all institutions"""
    logger.info(f"Starting download job for: {search_term}")
    
    all_dosare = []
    files_created = []
    processed_institutions = 0
    
    async with aiohttp.ClientSession() as session:
        # Iterate through all institutions
        for institutie in INSTITUTII:
            logger.info(f"Searching in {institutie}...")
            dosare = await fetch_dosare(session, search_term, institutie)
            all_dosare.extend(dosare)
            processed_institutions += 1
            
            # Update progress
            await db.job_runs.update_one(
                {"id": job_run_id},
                {"$set": {
                    "records_downloaded": len(all_dosare),
                    "progress_message": f"Procesare {processed_institutions}/{len(INSTITUTII)} instituții"
                }}
            )
            
            # Small delay to be nice to the server
            await asyncio.sleep(0.5)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dosare_{search_term.replace(' ', '_')}_{timestamp}.json"
    filepath = DOWNLOADS_DIR / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({
            "search_term": search_term,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(all_dosare),
            "dosare": all_dosare
        }, f, ensure_ascii=False, indent=2)
    
    files_created.append(filename)
    logger.info(f"Saved {len(all_dosare)} records to {filename}")
    
    # Update job run
    await db.job_runs.update_one(
        {"id": job_run_id},
        {
            "$set": {
                "status": "completed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "total_records": len(all_dosare),
                "records_downloaded": len(all_dosare),
                "files_created": files_created
            }
        }
    )
    
    return len(all_dosare)


# API Endpoints
@api_router.get("/")
async def root():
    return {"message": "Portal JUST Downloader API"}


@api_router.get("/config")
async def get_config():
    """Get current job configuration"""
    config = await db.job_config.find_one({}, {"_id": 0})
    if not config:
        # Create default config
        default_config = JobConfig().model_dump()
        default_config['created_at'] = default_config['created_at'].isoformat()
        default_config['updated_at'] = default_config['updated_at'].isoformat()
        await db.job_config.insert_one(default_config)
        return default_config
    return config


@api_router.put("/config")
async def update_config(update: JobConfigUpdate):
    """Update job configuration"""
    update_data = {k: v for k, v in update.model_dump().items() if v is not None}
    update_data['updated_at'] = datetime.now(timezone.utc).isoformat()
    
    result = await db.job_config.find_one_and_update(
        {},
        {"$set": update_data},
        return_document=True,
        projection={"_id": 0}
    )
    
    if not result:
        # Create new config with updates
        config = JobConfig(**update_data)
        config_dict = config.model_dump()
        config_dict['created_at'] = config_dict['created_at'].isoformat()
        config_dict['updated_at'] = config_dict['updated_at'].isoformat()
        await db.job_config.insert_one(config_dict)
        return config_dict
    
    return result


@api_router.post("/run")
async def trigger_run(background_tasks: BackgroundTasks):
    """Manually trigger a download job"""
    config = await db.job_config.find_one({}, {"_id": 0})
    
    if not config or not config.get('search_term'):
        raise HTTPException(status_code=400, detail="No search term configured. Please set a company name first.")
    
    # Check if there's already a running job
    running = await db.job_runs.find_one({"status": "running"}, {"_id": 0})
    if running:
        raise HTTPException(status_code=409, detail="A job is already running")
    
    # Create new job run
    job_run = JobRun()
    job_run_dict = job_run.model_dump()
    job_run_dict['started_at'] = job_run_dict['started_at'].isoformat()
    await db.job_runs.insert_one(job_run_dict)
    
    # Start background job
    background_tasks.add_task(run_download_job, config['search_term'], job_run.id)
    
    return {"message": "Job started", "job_id": job_run.id}


@api_router.get("/runs")
async def get_runs():
    """Get job run history"""
    runs = await db.job_runs.find({}, {"_id": 0}).sort("started_at", -1).to_list(50)
    return runs


@api_router.get("/runs/current")
async def get_current_run():
    """Get currently running job"""
    running = await db.job_runs.find_one({"status": "running"}, {"_id": 0})
    return running


@api_router.get("/files")
async def list_files():
    """List downloaded files"""
    files = []
    for f in DOWNLOADS_DIR.iterdir():
        if f.is_file() and f.suffix == '.json':
            stat = f.stat()
            files.append({
                "name": f.name,
                "size": stat.st_size,
                "created": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
            })
    return sorted(files, key=lambda x: x['created'], reverse=True)


@api_router.get("/files/{filename}")
async def download_file(filename: str):
    """Download a specific file"""
    filepath = DOWNLOADS_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(filepath, filename=filename, media_type='application/json')


@api_router.delete("/files/{filename}")
async def delete_file(filename: str):
    """Delete a specific file"""
    filepath = DOWNLOADS_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="File not found")
    filepath.unlink()
    return {"message": "File deleted"}


@api_router.post("/search")
async def search_dosare(request: SearchRequest):
    """Search dosare by company name (preview without saving)"""
    async with aiohttp.ClientSession() as session:
        # If no specific institution, search in a few major ones for preview
        if not request.institutie:
            all_dosare = []
            preview_institutions = ["TribunalulBUCURESTI", "CurteadeApelBUCURESTI", "TribunalulCLUJ", "TribunalulTIMIS"]
            for inst in preview_institutions:
                dosare = await fetch_dosare(session, request.company_name, inst)
                all_dosare.extend(dosare)
                if len(all_dosare) >= 20:
                    break
            return {"total": len(all_dosare), "dosare": all_dosare[:20]}
        else:
            dosare = await fetch_dosare(session, request.company_name, request.institutie)
            return {"total": len(dosare), "dosare": dosare[:20]}


@api_router.get("/institutions")
async def get_institutions():
    """Get list of available institutions"""
    return INSTITUTII


@api_router.get("/stats")
async def get_stats():
    """Get download statistics"""
    total_runs = await db.job_runs.count_documents({})
    completed_runs = await db.job_runs.count_documents({"status": "completed"})
    failed_runs = await db.job_runs.count_documents({"status": "failed"})
    
    # Count total files and size
    total_files = 0
    total_size = 0
    for f in DOWNLOADS_DIR.iterdir():
        if f.is_file() and f.suffix == '.json':
            total_files += 1
            total_size += f.stat().st_size
    
    last_run = await db.job_runs.find_one({}, {"_id": 0}, sort=[("started_at", -1)])
    
    return {
        "total_runs": total_runs,
        "completed_runs": completed_runs,
        "failed_runs": failed_runs,
        "total_files": total_files,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "last_run": last_run
    }


# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
