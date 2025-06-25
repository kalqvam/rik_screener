import pandas as pd
import xml.etree.ElementTree as ET
from typing import List, Optional, Dict, Any

def parse_annual_reports_response(xml_response: ET.Element, company_code: str) -> Optional[Dict[str, Any]]:
    try:
        ns = {
            'ns1': 'http://arireg.x-road.eu/producer/',
            'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/'
        }
        
        aruanded = xml_response.findall('.//ns1:aruanne', ns)
        
        if not aruanded:
            return None
        
        first_report = aruanded[0]
        
        aruande_aasta = first_report.find('ns1:aruande_aasta', ns)
        majandusaasta_algus = first_report.find('ns1:majandusaasta_algus', ns)
        majandusaasta_lopp = first_report.find('ns1:majandusaasta_lopp', ns)
        
        return {
            'company_code': company_code,
            'latest_year': aruande_aasta.text if aruande_aasta is not None else None,
            'period_start': majandusaasta_algus.text if majandusaasta_algus is not None else None,
            'period_end': majandusaasta_lopp.text if majandusaasta_lopp is not None else None
        }
        
    except Exception as e:
        print(f"Error parsing annual reports for {company_code}: {e}")
        return None

def parse_company_info_response(xml_response: ET.Element, company_code: str) -> Optional[str]:
    try:
        ns = {
            'ns1': 'http://arireg.x-road.eu/producer/',
            'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/'
        }
        
        evnimi = xml_response.find('.//ns1:evnimi', ns)
        
        if evnimi is not None:
            return evnimi.text
        
        return None
        
    except Exception as e:
        print(f"Error parsing company info for {company_code}: {e}")
        return None

def create_latest_reports_dataframe(reports_data: List[Dict[str, Any]], names_data: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    df = pd.DataFrame(reports_data)
    
    if names_data:
        df['company_name'] = df['company_code'].map(names_data)
        cols = ['company_code', 'company_name', 'latest_year', 'period_start', 'period_end']
        df = df[cols]
    
    return df
