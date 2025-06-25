import pandas as pd
from typing import List, Optional
from .config_auth import set_api_config
from .endpoints import get_annual_reports_list, get_company_basic_info
from .data_processors import parse_annual_reports_response, parse_company_info_response, create_latest_reports_dataframe
from .utils import validate_company_codes, format_progress

def get_latest_reports_info(
    company_codes: List[str],
    username: str,
    password: str,
    include_names: bool = False,
    rate_limit: int = 20
) -> pd.DataFrame:
    
    set_api_config(username, password, rate_limit)
    
    valid_codes = validate_company_codes(company_codes)
    
    if not valid_codes:
        print("No valid company codes provided")
        return pd.DataFrame()
    
    print(f"Processing {len(valid_codes)} companies with rate limit {rate_limit}/min")
    
    reports_data = []
    
    for i, company_code in enumerate(valid_codes, 1):
        print(format_progress(i, len(valid_codes), "Fetching reports"))
        
        xml_response = get_annual_reports_list(company_code)
        
        if xml_response is not None:
            report_info = parse_annual_reports_response(xml_response, company_code)
            if report_info:
                reports_data.append(report_info)
        else:
            print(f"Failed to get data for company {company_code}")
    
    names_data = None
    
    if include_names and reports_data:
        print(f"\nFetching company names...")
        names_data = {}
        
        for i, report in enumerate(reports_data, 1):
            company_code = report['company_code']
            print(format_progress(i, len(reports_data), "Fetching names"))
            
            xml_response = get_company_basic_info(company_code)
            
            if xml_response is not None:
                company_name = parse_company_info_response(xml_response, company_code)
                if company_name:
                    names_data[company_code] = company_name
    
    df = create_latest_reports_dataframe(reports_data, names_data)
    
    print(f"\nCompleted: {len(df)} companies processed successfully")
    
    return df
