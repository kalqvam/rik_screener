import pandas as pd
from typing import List, Optional, Union

from ..utils import (
    get_config,
    safe_read_csv,
    safe_write_csv,
    log_info,
    log_warning,
    log_error
)


def filter_companies(
    year: int = 2023,
    legal_forms: list = ["AS", "OÜ"],
    output_file: Optional[str] = "filtered_companies.csv",
    return_dataframe: bool = False
) -> Union[pd.DataFrame, None]:
    log_info(f"Reading general company data for {year}")

    config = get_config()
    
    header_data = safe_read_csv("general_data.csv", nrows=0)
    if header_data is None:
        log_error("Failed to read general_data.csv header")
        return None

    log_info(f"Available columns in general_data.csv: {header_data.columns.tolist()}")

    column_mapping = {
        "report_id": "report_id",
        "registrikood": "registrikood",
        "aruandeaast": "aruandeaast",
        "õiguslik vorm": "õiguslik vorm",
        "staatus": "staatus"
    }

    available_columns = header_data.columns.tolist()
    usecols = [col for expected, col in column_mapping.items() if col in available_columns]

    general_data = safe_read_csv("general_data.csv", usecols=usecols)
    if general_data is None:
        log_error("Failed to read general_data.csv")
        return None

    inverse_mapping = {v: k for k, v in column_mapping.items() if v in usecols}
    general_data = general_data.rename(columns=inverse_mapping)

    filtered_companies = general_data[
        (general_data["aruandeaast"] == year) &
        (general_data["õiguslik vorm"].isin(legal_forms)) &
        (general_data["staatus"] == "Registrisse kantud")
    ]

    if filtered_companies.empty:
        log_warning("No companies match the filtering criteria")
        return None

    log_info(f"Found {len(filtered_companies)} active companies with the specified legal forms")

    filtered_companies = filtered_companies.rename(columns={
        "aruandeaast": "year",
        "registrikood": "company_code",
        "õiguslik vorm": "legal_form"
    })

    filtered_companies = filtered_companies.drop(columns=["staatus"])

    if output_file and not return_dataframe:
        if safe_write_csv(filtered_companies, output_file):
            log_info(f"Saved {len(filtered_companies)} filtered companies to {output_file}")
        else:
            log_error(f"Failed to save filtered companies to {output_file}")

    return filtered_companies
