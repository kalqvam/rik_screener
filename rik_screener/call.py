import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

import pandas as pd
import numpy as np

from .utils import (
    get_config,
    log_step,
    log_info,
    log_warning,
    log_error
)

from .df_prep.general_filter import filter_companies
from .df_prep.multi_year_merger import merge_multiple_years
from .criteria_setup.calculations import calculate_ratios
from .add_info.industry_codes import add_industry_classifications
from .add_info.shareholder_data import add_ownership_data
from .post_processing.filtering import filter_and_rank

try:
    from google.colab import drive
    drive.mount('/content/drive')
    log_info("Google Drive mounted successfully")
except ImportError:
    log_info("Running outside of Google Colab")

config = get_config()
timestamp = config.get_timestamp()

log_step("MERGING MULTI-YEAR DATA")
years = config.get_years()
merged_file = f"merged_companies_{years[-1]}_{years[0]}_{timestamp}.csv"

merged_df = merge_multiple_years(
    years=years,
    legal_forms=config.get_default('legal_forms'),
    output_file=merged_file,
    require_all_years=True
)

if merged_df is None or merged_df.empty:
    log_error("No multi-year data available. Exiting")
    exit()

log_step("CALCULATING FINANCIAL RATIOS")
ratios_file = f"companies_with_ratios_{years[-1]}_{years[0]}_{timestamp}.csv"

formulas = {
    "EBITDA_Margin_2023": '("Ärikasum (kahjum)_2023" + abs("Põhivarade kulum ja väärtuse langus_2023")) / "Müügitulu_2023"',
    "EBITDA_2023": '("Ärikasum (kahjum)_2023" + abs("Põhivarade kulum ja väärtuse langus_2023"))',
    "Revenue_g_2023": '(("Müügitulu_2023" / "Müügitulu_2022") - 1) * 100',
    "Revenue_g_2022": '(("Müügitulu_2022" / "Müügitulu_2021") - 1) * 100',
    "EBITDA_CAGR_2Yr": '(pow((("Ärikasum (kahjum)_2023" + abs("Põhivarade kulum ja väärtuse langus_2023")) / ("Ärikasum (kahjum)_2021" + abs("Põhivarade kulum ja väärtuse langus_2021"))), 1/2) - 1) * 100'
}

financial_items = config.get_default('financial_items')

ratios_df = calculate_ratios(
    input_file=merged_file,
    output_file=ratios_file,
    years=years,
    formulas=formulas,
    financial_items=financial_items
)

if ratios_df is None or ratios_df.empty:
    log_error("Failed to calculate ratios. Exiting")
    exit()

log_info(f"Columns in ratios dataframe: {ratios_df.columns.tolist()}")

log_step("ADDING INDUSTRY CLASSIFICATIONS")
industry_file = f"companies_with_industry_{years[-1]}_{years[0]}_{timestamp}.csv"

industry_df = add_industry_classifications(
    input_file=ratios_file,
    output_file=industry_file,
    revenues_file="revenues.csv",
    years=years
)

if industry_df is None:
    log_warning("Failed to add industry classifications. Using ratios file for next step")
    industry_file = ratios_file
else:
    log_info(f"Columns in industry dataframe: {industry_df.columns.tolist()}")

log_step("ADDING OWNERSHIP DATA")
ownership_file = f"companies_with_ownership_{years[-1]}_{years[0]}_{timestamp}.csv"

ownership_filters = {
    "owner_count": {
        "min": 1
    },
    "percentages": {
        "exact": None
    }
}

ownership_df = add_ownership_data(
    input_file=industry_file,
    output_file=ownership_file,
    shareholders_file="shareholders.json",
    top_percentages=3,
    top_names=3,
    filters=ownership_filters
)

if ownership_df is None:
    log_warning("Failed to add ownership data. Using industry file for next step")
    current_file = industry_file
else:
    current_file = ownership_file
    log_info(f"Columns in ownership dataframe: {ownership_df.columns.tolist()}")

log_step("FILTERING AND RANKING")
ranked_file = f"ranked_companies_{years[-1]}_{years[0]}_{timestamp}.csv"

financial_filters = [
    {"column": "EBITDA_2023", "min": 400000, "max": None},
    {"column": "Müügitulu_2023", "min": 6000000, "max": None},
    {"column": "EBITDA_Margin_2023", "min": 0.14, "max": None},
    {"column": "Revenue_g_2023", "min": 0, "max": None},
    {"column": "Revenue_g_2022", "min": 0, "max": None},
    {"column": "EBITDA_CAGR_2Yr", "min": 0, "max": None},
    {"column": "Omakapital_2023", "min": 5000000, "max": 10000000}
]

available_columns = []
if ownership_df is not None:
    available_columns = ownership_df.columns.tolist()
elif industry_df is not None:
    available_columns = industry_df.columns.tolist()
else:
    available_columns = ratios_df.columns.tolist()

export_columns = ["company_code", "Müügitulu_2023", "EBITDA_2023", "Omakapital_2023", "EBITDA_Margin_2023", "EBITDA_CAGR_2Yr"]

if "industry_code_2023" in available_columns:
    export_columns.append("industry_code_2023")

if "owner_count" in available_columns:
    export_columns.append("owner_count")
if "top_3_percentages" in available_columns:
    export_columns.append("top_3_percentages")
if "top_3_owners" in available_columns:
    export_columns.append("top_3_owners")

ranked_df = filter_and_rank(
    input_file=current_file,
    output_file=ranked_file,
    sort_column="EBITDA_2023",
    filters=financial_filters,
    ascending=False,
    top_n=50,
    export_columns=export_columns
)

if ranked_df is None or ranked_df.empty:
    log_error("Failed to rank companies. Exiting")
    exit()

log_info("Top 10 companies after all filtering and ranking:")
log_info(str(ranked_df.head(10)))

log_info(f"Full results saved to: {config.get_file_path(ranked_file)}")
log_info("Analysis complete!")
