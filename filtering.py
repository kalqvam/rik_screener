import pandas as pd
import os
import numpy as np
from typing import List, Dict, Optional

def filter_and_rank(
    input_file: str = "companies_with_ratios.csv",
    output_file: str = "ranked_companies.csv",
    sort_column: str = "EBITDA_Margin",
    filters: list = None,  # List of filter dictionaries
    ascending: bool = False,
    top_n: int = None,
    export_columns: list = None
) -> pd.DataFrame:
    print(f"Loading companies with ratios from {input_file}...")
    input_path = os.path.join(BASE_PATH, input_file)

    if not os.path.exists(input_path):
        print(f"Error: Input file {input_path} not found")
        return None

    companies_df = pd.read_csv(input_path)
    original_count = len(companies_df)

    # Apply all filters if specified
    if filters:
        for filter_idx, filter_dict in enumerate(filters):
            column = filter_dict.get("column")
            min_val = filter_dict.get("min")
            max_val = filter_dict.get("max")

            if column not in companies_df.columns:
                print(f"Warning: Filter column '{column}' not found in data. Skipping this filter.")
                continue

            before_filter = len(companies_df)

            # Apply minimum value filter if specified
            if min_val is not None:
                companies_df = companies_df[companies_df[column] >= min_val]

            # Apply maximum value filter if specified
            if max_val is not None:
                companies_df = companies_df[companies_df[column] <= max_val]

            after_filter = len(companies_df)
            filtered_out = before_filter - after_filter

            print(f"Filter {filter_idx+1}: {column} " +
                  (f"min={min_val} " if min_val is not None else "") +
                  (f"max={max_val} " if max_val is not None else "") +
                  f"removed {filtered_out} companies")

            if companies_df.empty:
                print(f"No companies remain after applying filter {filter_idx+1} on {column}")
                return None

    total_filtered = original_count - len(companies_df)
    print(f"Total filtered: {total_filtered} companies")
    print(f"Remaining companies: {len(companies_df)}")

    # Sort companies
    if sort_column not in companies_df.columns:
        print(f"Error: Sort column '{sort_column}' not found in data")
        return None

    companies_df = companies_df.sort_values(by=sort_column, ascending=ascending)
    print(f"Sorted companies by {sort_column} ({'ascending' if ascending else 'descending'})")

    # Limit to top N if specified
    if top_n is not None and top_n > 0:
        companies_df = companies_df.head(top_n)
        print(f"Limited results to top {top_n} companies")

    # Select export columns if specified
    if export_columns is not None:
        # Verify all export columns exist
        missing_columns = [col for col in export_columns if col not in companies_df.columns]
        if missing_columns:
            print(f"Warning: These export columns were not found: {missing_columns}")
            # Keep only existing columns
            export_columns = [col for col in export_columns if col in companies_df.columns]

        companies_df = companies_df[export_columns]
        print(f"Selected {len(export_columns)} columns for export")

    # Save the results to a CSV file
    output_path = os.path.join(BASE_PATH, output_file)
    companies_df.to_csv(
        output_path,
        index=False,
        encoding='utf-8-sig',  # This adds BOM for Excel to recognize UTF-8
        decimal='.'
    )

    print(f"Saved {len(companies_df)} ranked companies to {output_path}")
    return companies_df
