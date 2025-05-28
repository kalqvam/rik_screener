import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

from ..utils import (
    get_config,
    safe_read_csv,
    safe_write_csv,
    log_info,
    log_warning,
    log_error
)


def add_company_age(
    input_file: str = "companies_with_industry.csv",
    output_file: str = "companies_with_age.csv",
    legal_data_file: str = "legal_data.csv"
) -> pd.DataFrame:
    log_info(f"Loading companies from {input_file}")
    
    companies_df = safe_read_csv(input_file)
    if companies_df is None:
        log_error(f"Failed to load input file {input_file}")
        return None
    
    log_info(f"Loaded {len(companies_df)} companies")
    
    log_info(f"Loading legal data from {legal_data_file}")
    legal_df = safe_read_csv(legal_data_file, usecols=["ariregistri_kood", "ettevotja_esmakande_kpv"])
    if legal_df is None:
        log_error(f"Failed to load legal data file {legal_data_file}")
        return companies_df
    
    log_info(f"Loaded legal data for {len(legal_df)} companies")
    
    legal_df['ariregistri_kood'] = legal_df['ariregistri_kood'].astype(str)
    
    legal_df['registration_date'] = pd.to_datetime(
        legal_df['ettevotja_esmakande_kpv'], 
        format='%d.%m.%Y', 
        errors='coerce'
    )
    
    valid_dates = legal_df['registration_date'].notna().sum()
    log_info(f"Successfully parsed {valid_dates} out of {len(legal_df)} registration dates")
    
    today = datetime.now()
    legal_df['company_age_years'] = (today - legal_df['registration_date']).dt.days / 365.25
    
    legal_dict = dict(zip(legal_df['ariregistri_kood'], legal_df['company_age_years']))
    
    companies_df['company_code_str'] = companies_df['company_code'].astype(str)
    companies_df['company_age_years'] = companies_df['company_code_str'].map(legal_dict)
    companies_df = companies_df.drop(columns=['company_code_str'])
    
    matched_count = companies_df['company_age_years'].notna().sum()
    log_info(f"Successfully matched company age for {matched_count} out of {len(companies_df)} companies")
    
    if matched_count == 0:
        log_warning("No companies were matched with legal data")
    
    if safe_write_csv(companies_df, output_file):
        log_info(f"Saved {len(companies_df)} companies with age data to {output_file}")
    else:
        log_error(f"Failed to save results to {output_file}")
    
    return companies_df
