import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any
from .soap_client import SOAPClient

def get_annual_reports_list(company_code: str) -> Optional[ET.Element]:
    client = SOAPClient()
    params = {"ariregistri_kood": company_code}
    return client.call_endpoint("majandusaastaAruanneteLoetelu_v1", params)

def get_company_basic_info(company_code: str) -> Optional[ET.Element]:
    client = SOAPClient()
    params = {"ariregistri_kood": company_code}
    return client.call_endpoint("lihtandmed_v2", params)
