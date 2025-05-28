import pandas as pd
from typing import Optional

from ..utils import (
    get_config,
    safe_read_csv,
    safe_write_csv,
    log_info,
    log_warning,
    log_error
)


def add_company_names(
    input_file: str = "ranked_companies.csv",
    output_file: str = "final_companies_with_names.csv",
    legal_data_file: str = "legal_data.csv"
) -> pd.DataFrame:
    log_info(f"Loading ranked companies from {input_file}")
    
    companies_df = safe_read_csv(input_file)
    if companies_df is None:
        log_error(f"Failed to load input file {input_file}")
        return None
    
    log_info(f"Loaded {len(companies_df)} companies")
    
    log_info(f"Loading legal data from {legal_data_file}")
    
    legal_df = safe_read_csv(legal_data_file, separator=';')
    if legal_df is None:
        log_error(f"Failed to load legal data file {legal_data_file}")
        return companies_df
    
    if 'ariregistri_kood' not in legal_df.columns or 'nimi' not in legal_df.columns:
        log_error(f"Required columns not found in {legal_data_file}")
        log_error(f"Available columns: {legal_df.columns.tolist()}")
        return companies_df
    
    legal_df = legal_df[['ariregistri_kood', 'nimi']].copy()
    
    log_info(f"Loaded legal data for {len(legal_df)} companies")
    
    legal_df['ariregistri_kood'] = legal_df['ariregistri_kood'].astype(str)
    legal_df['nimi'] = legal_df['nimi'].astype(str)
    
    if legal_df['ariregistri_kood'].duplicated().any():
        duplicate_count = legal_df['ariregistri_kood'].duplicated().sum()
        log_warning(f"Found {duplicate_count} duplicate company codes in legal data, keeping first occurrence")
        legal_df = legal_df.drop_duplicates(subset='ariregistri_kood', keep='first')
    
    legal_dict = dict(zip(legal_df['ariregistri_kood'], legal_df['nimi']))
    
    companies_df['company_code_str'] = companies_df['company_code'].astype(str)
    companies_df['company_name'] = companies_df['company_code_str'].map(legal_dict)
    companies_df = companies_df.drop(columns=['company_code_str'])
    
    matched_count = companies_df['company_name'].notna().sum()
    log_info(f"Successfully matched company names for {matched_count} out of {len(companies_df)} companies")
    
    if matched_count == 0:
        log_warning("No companies were matched with legal data")
    
    cols = companies_df.columns.tolist()
    if 'company_name' in cols:
        cols.remove('company_name')
        cols.insert(0, 'company_name')
        companies_df = companies_df[cols]
    
    if safe_write_csv(companies_df, output_file, encoding='utf-8-sig'):
        log_info(f"Saved {len(companies_df)} companies with names to {output_file}")
    else:
        log_error(f"Failed to save results to {output_file}")
    
    return companies_df
