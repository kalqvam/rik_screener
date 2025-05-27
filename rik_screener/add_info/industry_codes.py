import pandas as pd
import numpy as np
from typing import List

from ..utils import (
    get_config,
    safe_read_csv,
    safe_write_csv,
    log_step,
    log_info,
    log_warning,
    log_error
)


def add_industry_classifications(
    input_file: str = "companies_with_ratios.csv",
    output_file: str = "companies_with_industry.csv",
    revenues_file: str = "revenues.csv",
    years: list = None
) -> pd.DataFrame:
    log_step("ADDING INDUSTRY CLASSIFICATIONS")
    log_info(f"Loading companies from {input_file}")

    companies_df = safe_read_csv(input_file)
    if companies_df is None:
        log_error(f"Failed to load input file {input_file}")
        return None

    log_info(f"Loaded {len(companies_df)} companies")

    config = get_config()
    if years is None:
        years = config.get_years()

    years = sorted(years, reverse=True)

    revenues_header = safe_read_csv(revenues_file, nrows=0)
    if revenues_header is None:
        log_error(f"Revenue file {revenues_file} not found")
        return companies_df

    log_info(f"Loading industry revenue data from {revenues_file}")
    log_info(f"Available columns in revenues file: {revenues_header.columns.tolist()}")

    for year in years:
        report_id_col = f"report_id_{year}"

        if report_id_col not in companies_df.columns:
            log_warning(f"{report_id_col} not found in companies data. Skipping year {year}")
            continue

        log_info(f"Processing industry classifications for year {year}")

        report_ids = companies_df[report_id_col].dropna().astype(int).tolist()
        log_info(f"Found {len(report_ids)} report IDs to process for year {year}")

        if not report_ids:
            continue

        chunks = []
        chunk_size = config.get_default('chunk_size', 500000)

        try:
            for chunk in safe_read_csv(
                revenues_file,
                chunk_size=chunk_size,
                dtype={"report_id": int, "emtak": str}
            ):
                filtered_chunk = chunk[
                    (chunk["report_id"].isin(report_ids)) &
                    (chunk["põhitegevusala"] == "jah")
                ]

                if not filtered_chunk.empty:
                    chunks.append(filtered_chunk)

            if not chunks:
                log_warning(f"No industry data found for year {year}")
                continue

            industry_data = pd.concat(chunks, ignore_index=True)
            industry_data = industry_data[["report_id", "emtak"]]

            log_info(f"Found {len(industry_data)} industry classifications for year {year}")

            duplicates = industry_data["report_id"].duplicated()
            if duplicates.any():
                dupe_count = duplicates.sum()
                log_warning(f"Found {dupe_count} duplicate main industry codes. Using the first occurrence")
                industry_data = industry_data.drop_duplicates(subset="report_id", keep="first")

            industry_data = industry_data.rename(columns={"emtak": f"industry_code_{year}"})

            companies_df = pd.merge(
                companies_df,
                industry_data,
                left_on=report_id_col,
                right_on="report_id",
                how="left"
            )

            if "report_id" in companies_df.columns:
                companies_df = companies_df.drop(columns=["report_id"])

            assigned_count = companies_df[f"industry_code_{year}"].notna().sum()
            log_info(f"Assigned industry codes to {assigned_count} out of {len(companies_df)} companies for year {year}")

        except Exception as e:
            log_error(f"Error processing industry data for year {year}: {str(e)}")
            import traceback
            traceback.print_exc()

    if safe_write_csv(companies_df, output_file):
        log_info(f"Saved {len(companies_df)} companies with industry codes to {output_file}")
    else:
        log_error(f"Failed to save results to {output_file}")

    for year in years:
        industry_col = f"industry_code_{year}"
        if industry_col in companies_df.columns:
            unique_codes = companies_df[industry_col].dropna().unique()
            log_info(f"Found {len(unique_codes)} unique industry codes for {year}")
            log_info(f"Sample of industry codes: {unique_codes[:10]}")
            break

    return companies_df
