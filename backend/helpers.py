"""Helper / utility functions shared across route modules."""
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional

from constants import COMPANY_REGEX, EXCLUDE_REGEX


def normalize_company_name(name: str) -> str:
    name = name.upper().strip()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'[^\w\s]', '', name)
    return name


def is_company(name: str) -> bool:
    if EXCLUDE_REGEX.search(name):
        return False
    return bool(COMPANY_REGEX.search(name))


def extract_companies_from_parti(parti: list) -> list:
    companies = []
    for parte in parti:
        if not isinstance(parte, dict):
            continue
        name = parte.get('nume', '') or parte.get('numeParte', '') or parte.get('denumire', '')
        if name and is_company(name):
            # Clean long names: "SC FIRMA SRL PRIN ADMINISTRATOR JUDICIAR..." -> "SC FIRMA SRL"
            clean_name = _extract_core_company_name(name)
            companies.append({
                'denumire': clean_name,
                'denumire_normalized': normalize_company_name(clean_name),
                'denumire_raw': name,
                'calitate': parte.get('calitateParte', '')
            })
    return companies


def _extract_core_company_name(name: str) -> str:
    """Extract core company name, removing 'PRIN ADMINISTRATOR...' suffixes."""
    # Cut at common suffixes that follow the company name
    cut_patterns = [
        r'\s+REPREZENTAT[AĂ]?\b',
        r'\s+PRIN\s+(?:ADMINISTRATOR|LICHIDATOR|MANDATAR|CURATOR|TUTORE)',
        r'\s*[-–]\s*(?:SEDIUL|SUCURSALA|FILIALA|PUNCT\s+DE\s+LUCRU)',
    ]
    result = name.strip()
    for pat in cut_patterns:
        m = re.search(pat, result, re.IGNORECASE)
        if m:
            result = result[:m.start()].strip()
            break
    return result if len(result) >= 5 else name.strip()


def parse_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y']:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def build_soap_request(nume_parte: str, institutie: str,
                       date_start: str = "", date_end: str = "") -> str:
    from constants import SOAP_URL  # noqa (just for grouping)
    data_start = f"<dataStart>{date_start}</dataStart>" if date_start else '<dataStart xsi:nil="true" />'
    data_stop = f"<dataStop>{date_end}</dataStop>" if date_end else '<dataStop xsi:nil="true" />'
    nume_parte_xml = (
        f"<numeParte>{nume_parte}</numeParte>"
        if nume_parte and nume_parte.strip()
        else '<numeParte xsi:nil="true" />'
    )
    return f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CautareDosare2 xmlns="portalquery.just.ro">
      <numarDosar xsi:nil="true" />
      <obiectDosar xsi:nil="true" />
      {nume_parte_xml}
      <institutie>{institutie}</institutie>
      {data_start}
      {data_stop}
      <dataUltimaModificareStart xsi:nil="true" />
      <dataUltimaModificareStop xsi:nil="true" />
    </CautareDosare2>
  </soap:Body>
</soap:Envelope>'''


def parse_soap_response(xml_content: str) -> List[dict]:
    try:
        root = ET.fromstring(xml_content)
        ns = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/', 'pq': 'portalquery.just.ro'}
        dosare = []
        for dosar in root.findall('.//pq:Dosar', ns):
            d = {}
            for child in dosar:
                tag = child.tag.replace('{portalquery.just.ro}', '')
                d[tag] = child.text or ""
                if len(child) > 0:
                    nested = []
                    for n in child:
                        nd = {}
                        for nc in n:
                            ntag = nc.tag.replace('{portalquery.just.ro}', '')
                            nd[ntag] = nc.text or ""
                        if nd:
                            nested.append(nd)
                    if nested:
                        d[tag] = nested
            if d:
                dosare.append(d)
        return dosare
    except ET.ParseError:
        return []
