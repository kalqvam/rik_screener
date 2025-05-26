import pandas as pd
import os
import numpy as np
import re
from typing import List, Dict, Optional

BASE_PATH = "/content/drive/MyDrive/Python/prudentia"

def filter_companies(
    year: int = 2023,
    legal_forms: list = ["AS", "OÜ"],
    output_file: str = "filtered_companies.csv"
) -> pd.DataFrame:
    print(f"Reading general company data for {year}...")

    header_data = pd.read_csv(
        os.path.join(BASE_PATH, "general_data.csv"),
        nrows=0,
        encoding="utf-8",
        sep=";"
    )

    print(f"Available columns in general_data.csv: {header_data.columns.tolist()}")

    column_mapping = {
        "report_id": "report_id",
        "registrikood": "registrikood",
        "aruandeaast": "aruandeaast",
        "õiguslik vorm": "õiguslik vorm",
        "staatus": "staatus"
    }

    available_columns = header_data.columns.tolist()
    usecols = [col for expected, col in column_mapping.items() if col in available_columns]

    general_data = pd.read_csv(
        os.path.join(BASE_PATH, "general_data.csv"),
        usecols=usecols,
        encoding="utf-8",
        sep=";"
    )

    inverse_mapping = {v: k for k, v in column_mapping.items() if v in usecols}
    general_data = general_data.rename(columns=inverse_mapping)

    filtered_companies = general_data[
        (general_data["aruandeaast"] == year) &
        (general_data["õiguslik vorm"].isin(legal_forms)) &
        (general_data["staatus"] == "Registrisse kantud")
    ]

    if filtered_companies.empty:
        print("No companies match the filtering criteria")
        return None

    print(f"Found {len(filtered_companies)} active companies with the specified legal forms")

    filtered_companies = filtered_companies.rename(columns={
        "aruandeaast": "year",
        "registrikood": "company_code",
        "õiguslik vorm": "legal_form"
    })

    filtered_companies = filtered_companies.drop(columns=["staatus"])

    output_path = os.path.join(BASE_PATH, output_file)
    filtered_companies.to_csv(output_path, index=False, encoding="utf-8")

    print(f"Saved {len(filtered_companies)} filtered companies to {output_path}")
    return filtered_companies

"""
if __name__ == "__main__":
    # Mount Google Drive (required in Colab)
    try:
        from google.colab import drive
        drive.mount('/content/drive')
        print("Google Drive mounted successfully")
    except ImportError:
        print("Running outside of Google Colab")

    # Example usage
    filtered_df = filter_companies(
        year=2022,
        legal_forms=["AS", "OÜ"],
        output_file="filtered_companies_2022.csv"
    )

    if filtered_df is not None:
        print("\nFirst 5 filtered companies:")
        print(filtered_df.head())
"""
