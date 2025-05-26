import pandas as pd
import os
from typing import List, Dict, Optional

BASE_PATH = "/content/drive/MyDrive/Python/rik_screener"

print("DEBUG: About to import filter_companies...")
try:
    from data_preparation import filter_companies
    print("DEBUG: filter_companies imported successfully in multi_year_merger.py")
except Exception as e:
    print(f"DEBUG: Failed to import filter_companies in multi_year_merger.py: {e}")
    import traceback
    traceback.print_exc()

print("DEBUG: multi_year_merger.py module loading complete")


def merge_multiple_years(
    years: List[int],
    legal_forms: List[str] = ["AS", "OÜ"],
    output_file: str = "merged_companies_multiyear.csv",
    require_all_years: bool = True
) -> pd.DataFrame:

    if not years:
        print("Error: No years specified")
        return None

    print(f"Processing data for years: {years}")

    year_dfs = {}

    for year in years:
        print(f"\nProcessing year {year}...")
        year_df = filter_companies(
            year=year,
            legal_forms=legal_forms,
            output_file=f"temp_filtered_companies_{year}.csv"
        )

        if year_df is None or year_df.empty:
            print(f"Warning: No data available for year {year}")
            if require_all_years:
                print("Since require_all_years=True, cannot continue without data for all years")
                return None
            continue

        suffix = f"_{year}"
        rename_cols = {col: f"{col}{suffix}" for col in year_df.columns
                       if col != 'company_code'}
        year_df = year_df.rename(columns=rename_cols)

        year_dfs[year] = year_df

    if len(year_dfs) < len(years) and require_all_years:
        print(f"Not all years have data ({len(year_dfs)} out of {len(years)})")
        return None

    if not year_dfs:
        print("No data available for any of the specified years")
        return None

    if require_all_years and len(year_dfs) > 1:
        common_companies = set(year_dfs[years[0]]['company_code'])
        for year in years[1:]:
            if year in year_dfs:
                common_companies &= set(year_dfs[year]['company_code'])

        print(f"Found {len(common_companies)} companies with data for all specified years")

        if not common_companies:
            print("No companies have data for all specified years")
            return None

        for year in years:
            if year in year_dfs:
                year_dfs[year] = year_dfs[year][year_dfs[year]['company_code'].isin(common_companies)]

    merged_data = year_dfs[years[0]]

    for year in years[1:]:
        if year in year_dfs:
            merged_data = pd.merge(
                merged_data,
                year_dfs[year],
                on='company_code',
                how='inner',
                suffixes=('', f'_dup_{year}')
            )

            dup_cols = [col for col in merged_data.columns if f'_dup_{year}' in col]
            if dup_cols:
                print(f"Warning: Dropping {len(dup_cols)} duplicate columns from the merge")
                merged_data = merged_data.drop(columns=dup_cols)

    if not merged_data.empty:
        output_path = os.path.join(BASE_PATH, output_file)
        merged_data.to_csv(output_path, index=False, encoding="utf-8")
        print(f"Saved {len(merged_data)} companies with multi-year data to {output_path}")

    for year in years:
        temp_file = os.path.join(BASE_PATH, f"temp_filtered_companies_{year}.csv")
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                print(f"Removed temporary file: {temp_file}")
            except Exception as e:
                print(f"Could not remove temporary file {temp_file}: {e}")

    return merged_data

"""
if __name__ == "__main__":
    # Mount Google Drive (required in Colab)
    try:
        from google.colab import drive
        drive.mount('/content/drive')
        print("Google Drive mounted successfully")
    except ImportError:
        print("Running outside of Google Colab")

    # Example usage - get data for 3 consecutive years
    merged_df = merge_multiple_years(
        years=[2023],  # Most recent first
        legal_forms=["AS", "OÜ"],
        output_file="merged_companies_multi_year.csv",
        require_all_years=True
    )

    if merged_df is not None:
        print("\nSample of merged data:")
        # Show a few columns from different years to demonstrate the structure
        sample_cols = ['company_code']
        available_cols = [col for col in sample_cols if col in merged_df.columns]
        print(merged_df[available_cols].head())
"""
