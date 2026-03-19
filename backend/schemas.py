"""Pydantic request/response schemas."""
import uuid
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

# All valid CategorieCaz values from portalquery.just.ro WSDL
CATEGORII_CAZ = [
    "Litigiicuprofesionistii",
    "Civil",
    "Penal",
    "Faliment",
    "Contenciosadministrativsifiscal",
    "Minorisifamilie",
    "Litigiidemunca",
    "Altematerii",
    "Asigurarisociale",
    "ProprietateIntelectuala",
    "Dreptmaritimsifluvial",
    "Insolventapersoaneifizice",
]

CATEGORII_NUME = {
    "Litigiicuprofesionistii": "Litigii cu profesioniștii",
    "Civil": "Civil",
    "Penal": "Penal",
    "Faliment": "Faliment / Insolvență",
    "Contenciosadministrativsifiscal": "Contencios administrativ și fiscal",
    "Minorisifamilie": "Minori și familie",
    "Litigiidemunca": "Litigii de muncă",
    "Altematerii": "Alte materii",
    "Asigurarisociale": "Asigurări sociale",
    "ProprietateIntelectuala": "Proprietate intelectuală",
    "Dreptmaritimsifluvial": "Drept maritim și fluvial",
    "Insolventapersoaneifizice": "Insolvența persoanei fizice",
}


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
    # Category filter — None means all categories
    categorie_caz: Optional[str] = None
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
    categorie_caz: Optional[str] = None


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
    company_name: Optional[str] = None
    institutie: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None


class FirmaCreate(BaseModel):
    cui: Optional[str] = None
    denumire: str


class FirmaUpdate(BaseModel):
    cui: Optional[str] = None
    denumire: Optional[str] = None
