"""
Application constants: SOAP config, ANAF config, MFinante URL, institution list,
company name patterns, and file paths.
"""
import re
from pathlib import Path

ROOT_DIR = Path(__file__).parent
DOWNLOADS_DIR = ROOT_DIR / 'downloads'
DOWNLOADS_DIR.mkdir(exist_ok=True)

# ─── SOAP (Portal JUST) ───────────────────────────────────────────────────────
SOAP_URL = "http://portalquery.just.ro/query.asmx"
SOAP_ACTION_DOSARE2 = "portalquery.just.ro/CautareDosare2"

# ─── ANAF API ─────────────────────────────────────────────────────────────────
ANAF_API_URL = "https://webservicesp.anaf.ro/api/PlatitorTvaRest/v9/tva"
ANAF_BATCH_SIZE = 100
ANAF_RATE_LIMIT_SECONDS = 1.5

# ─── MFinante ─────────────────────────────────────────────────────────────────
MFINANTE_URL = "https://mfinante.gov.ro/apps/infocodfiscal.html"

# ─── Company detection patterns ───────────────────────────────────────────────
COMPANY_PATTERNS = [
    r'\bSRL\b', r'\bSA\b', r'\bSCS\b', r'\bSNC\b', r'\bSCA\b',
    r'\bSPRL\b', r'\bGMBH\b', r'\bLTD\b', r'\bLLC\b', r'\bINC\b',
    r'\bONG\b', r'\bASSOC\b',
    r'S\.R\.L\.', r'S\.A\.', r'S\.C\.S\.', r'S\.N\.C\.'
]
COMPANY_REGEX = re.compile('|'.join(COMPANY_PATTERNS), re.IGNORECASE)

EXCLUDE_PATTERNS = [
    r'PERSOANĂ FIZICĂ', r'PERSOANA FIZICA', r'PFA\b',
    r'ÎNTREPRINDERE INDIVIDUALĂ', r'INTREPRINDERE INDIVIDUALA',
    r'\bII\b', r'\bIF\b', r'CABINET INDIVIDUAL', r'BIROU INDIVIDUAL'
]
EXCLUDE_REGEX = re.compile('|'.join(EXCLUDE_PATTERNS), re.IGNORECASE)

# ─── Institutions (246 total) ─────────────────────────────────────────────────
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
