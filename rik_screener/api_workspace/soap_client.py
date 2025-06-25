import requests
import xml.etree.ElementTree as ET
from typing import Dict, Optional
from .config_auth import get_api_config

class SOAPClient:
    def __init__(self):
        self.config = get_api_config()
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': ''
        })
    
    def build_envelope(self, operation: str, body_content: str) -> str:
        return f'''<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:xro="http://x-road.eu/xsd/xroad.xsd" 
                  xmlns:iden="http://x-road.eu/xsd/identifiers" 
                  xmlns:prod="http://arireg.x-road.eu/producer/">
    <soapenv:Body>
        <prod:{operation}>
            <prod:keha>
                <prod:ariregister_kasutajanimi>{self.config.username}</prod:ariregister_kasutajanimi>
                <prod:ariregister_parool>{self.config.password}</prod:ariregister_parool>
                {body_content}
            </prod:keha>
        </prod:{operation}>
    </soapenv:Body>
</soapenv:Envelope>'''
    
    def send_request(self, envelope: str) -> Optional[ET.Element]:
        self.config.wait_for_rate_limit()
        
        print(f"SOAP Request URL: {self.config.base_url}")
        print(f"SOAP Request Headers: {self.session.headers}")
        print(f"SOAP Request Body:\n{envelope}")
        print("-" * 50)
        
        try:
            response = self.session.post(
                self.config.base_url,
                data=envelope.encode('utf-8'),
                timeout=30
            )
            
            print(f"Response Status: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Response Content:\n{response.text[:1000]}...")
            print("-" * 50)
            
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            return root
            
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None
        except ET.ParseError as e:
            print(f"XML parsing failed: {e}")
            return None
    
    def call_endpoint(self, operation: str, params: Dict[str, str]) -> Optional[ET.Element]:
        body_parts = []
        for key, value in params.items():
            body_parts.append(f"<prod:{key}>{value}</prod:{key}>")
        
        body_content = "\n                ".join(body_parts)
        envelope = self.build_envelope(operation, body_content)
        
        return self.send_request(envelope)
