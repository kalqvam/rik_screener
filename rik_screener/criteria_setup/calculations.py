import pandas as pd
import numpy as np
import os
import re

# Configuration
BASE_PATH = "/content/drive/MyDrive/Python/rik_screener"

def create_formula(formula_expr, data):
    print(f"Processing formula: {formula_expr}")

    pattern = r'"([^"]+)"|\'([^\']+)\''
    matches = re.findall(pattern, formula_expr)

    columns = [m[0] or m[1] for m in matches]

    # Print the columns extracted from the formula
    print(f"Columns referenced in formula: {columns}")

    # Create a namespace with columns and numpy functions
    namespace = {}

    # Add column data
    for col in columns:
        if col in data.columns:
            # Convert to numeric to ensure proper calculations
            values = pd.to_numeric(data[col], errors='coerce').values

            # Replace NaN values with 1
            values = np.nan_to_num(values, nan=1.0)

            namespace[col] = values
        else:
            raise ValueError(f"Column '{col}' not found in the data")

    # Add numpy functions for convenience
    namespace.update({
        'abs': np.abs,
        'min': np.minimum,
        'max': np.maximum,
        'sqrt': np.sqrt,
        'log': np.log,
        'log10': np.log10,
        'exp': np.exp,
        'round': np.round,
        'pow': np.power  # Add power function for CAGR calculations
    })

    # Replace quoted column names with namespace references
    for col in columns:
        # Replace both double and single quoted versions
        formula_expr = formula_expr.replace(f'"{col}"', f'namespace["{col}"]')
        formula_expr = formula_expr.replace(f"'{col}'", f'namespace["{col}"]')

    # Evaluate the formula
    try:
        result = eval(formula_expr)
        return result
    except Exception as e:
        raise ValueError(f"Error evaluating formula '{formula_expr}': {str(e)}")

def extract_quoted_columns(formula):
    """Extract column names from a formula string"""
    if formula:
        pattern = r'"([^"]+)"|\'([^\']+)\''
        matches = re.findall(pattern, formula)
        # Extract column names (either from first or second group depending on quote style)
        return [m[0] or m[1] for m in matches]
    return []

def calculate_ratios(
    input_file: str = "merged_companies_multi_year.csv",
    output_file: str = "companies_with_ratios.csv",
    years: list = None,
    financial_items: list = None,
    formulas: dict = None  # Dictionary of {formula_name: formula_expression}
) -> pd.DataFrame:
    # Set default years if none provided
    if years is None:
        years = [2023, 2022, 2021]  # Default years

    # Ensure years are sorted (most recent first)
    years = sorted(years, reverse=True)

    # Set default formulas if none provided
    if formulas is None:
        formulas = {
            f"EBITDA_Margin_{years[0]}": f'("Ärikasum (kahjum)_{years[0]}" + "Põhivarade kulum ja väärtuse langus_{years[0]}") / "Müügitulu_{years[0]}"',
            "Revenue_Growth": f'("Müügitulu_{years[0]}" - "Müügitulu_{years[1]}") / "Müügitulu_{years[1]}"',
        }

    # Extract all financial items needed for formulas
    all_formula_columns = []
    for formula in formulas.values():
        formula_cols = extract_quoted_columns(formula)
        print(f"Columns from formula: {formula_cols}")
        all_formula_columns.extend(formula_cols)

    # If financial_items is not provided, extract from formulas
    if financial_items is None:
        # Extract the base financial items (without year suffixes)
        financial_items = set()
        for col in all_formula_columns:
            # If the column has a year suffix, extract the base name
            if '_20' in col:  # Simple check for year suffix
                base_col = col.split('_20')[0]
                financial_items.add(base_col)
            else:
                financial_items.add(col)
        financial_items = list(financial_items)

    print(f"Financial items to retrieve: {financial_items}")

    # Load the merged companies data
    print(f"Loading merged companies from {input_file}...")
    input_path = os.path.join(BASE_PATH, input_file)

    if not os.path.exists(input_path):
        print(f"Error: Input file {input_path} not found")
        return None

    result = pd.read_csv(input_path)
    print(f"Loaded {len(result)} companies from merged data")
    print(f"Columns in merged data: {sorted(result.columns.tolist())}")

    # For each year, load the financial data and merge using the year-specific report_id
    for year in years:
        report_id_col = f"report_id_{year}"

        # Check if the report_id column exists for this year
        if report_id_col not in result.columns:
            print(f"Warning: {report_id_col} not found in merged data. Skipping year {year}.")
            continue

        # Load financial data for this year
        financial_file = os.path.join(BASE_PATH, f"financials_{year}.csv")
        if not os.path.exists(financial_file):
            print(f"Warning: Financial file for {year} not found")
            continue

        print(f"Reading financial data for {year}...")

        try:
            financial_data = pd.read_csv(
                financial_file,
                usecols=["report_id", "tabel", "elemendi_label", "vaartus"],
                encoding="utf-8",
                sep=";"
            )

            # Filter for requested financial items
            # First, create a clean version of elemendi_label by removing " Konsolideeritud" suffix if present
            financial_data['clean_elemendi_label'] = financial_data['elemendi_label'].str.replace(' Konsolideeritud$', '', regex=True)

            # Now filter using the clean labels
            financial_data = financial_data[financial_data["clean_elemendi_label"].isin(financial_items)]
            print(f"Found {len(financial_data)} financial records for year {year}")

            # Create flags for consolidated data:
            # 1. Check if "Konsolideeritud" appears in the "tabel" column
            # 2. For PDF reports, check if " Konsolideeritud" appears in the "elemendi_label" column
            financial_data['is_consolidated'] = (
                financial_data['tabel'].str.contains('Konsolideeritud', case=False, na=False) |
                financial_data['elemendi_label'].str.contains(' Konsolideeritud$', regex=True, na=False)
            )

            # Group by report_id and company to identify which have consolidated data
            consolidated_reports = financial_data.groupby('report_id')['is_consolidated'].any()
            financial_data['has_consolidated'] = financial_data['report_id'].map(consolidated_reports)

            # Keep only consolidated records for reports that have them, otherwise keep non-consolidated
            financial_data = financial_data[
                (~financial_data['has_consolidated']) |
                (financial_data['has_consolidated'] & financial_data['is_consolidated'])
            ]

            print(f"After consolidated filtering: {len(financial_data)} records")

            # Pivot to wide format using the clean labels
            financial_wide = financial_data.pivot_table(
                index="report_id",
                columns="clean_elemendi_label",
                values="vaartus",
                aggfunc='first'  # In case there are duplicates
            ).reset_index()

            # Add year suffix to financial columns
            financial_cols = financial_wide.columns.difference(['report_id'])
            rename_dict = {col: f"{col}_{year}" for col in financial_cols}
            financial_wide = financial_wide.rename(columns=rename_dict)

            # Add a flag indicating if the data is consolidated
            is_consolidated = financial_data.groupby('report_id')['is_consolidated'].any()
            financial_wide['is_consolidated'] = financial_wide['report_id'].map(is_consolidated)
            financial_wide = financial_wide.rename(columns={'is_consolidated': f'is_consolidated_{year}'})

            # Print financial columns after renaming
            print(f"Financial columns for {year} after renaming: {sorted([col for col in financial_wide.columns if col != 'report_id'])}")

            # Merge with the main dataframe
            result = pd.merge(
                result,
                financial_wide,
                left_on=report_id_col,
                right_on='report_id',
                how='left'
            )

            # Drop the duplicate report_id column from the merge
            if 'report_id' in result.columns:
                result = result.drop(columns=['report_id'])

            print(f"Merged financial data for {year}. Current columns: {len(result.columns)}")

        except Exception as e:
            print(f"Error processing financial data for {year}: {str(e)}")
            import traceback
            traceback.print_exc()

    # Convert all financial columns to numeric and handle NaN values
    print("Converting financial columns to numeric and replacing NaN values with 1...")

    # Find all financial columns with year suffixes
    all_financial_columns = []
    for item in financial_items:
        for year in years:
            col = f"{item}_{year}"
            if col in result.columns:
                all_financial_columns.append(col)

    for col in all_financial_columns:
        result[col] = pd.to_numeric(result[col], errors='coerce')
        # Count NaN values before replacement
        nan_count = result[col].isna().sum()
        if nan_count > 0:
            print(f"  - Column '{col}': {nan_count} NaN values replaced with 1")
        # Replace NaN with 1
        result[col] = result[col].fillna(1)

    # Print a few rows of financial data for debugging
    print("\nSample of financial data columns:")
    sample_cols = ['company_code'] + all_financial_columns[:3]  # First 3 financial columns
    print(result[sample_cols].head())

    # Calculate all formulas
    for formula_name, formula_expr in formulas.items():
        try:
            print(f"\nCalculating formula: {formula_name}")
            result[formula_name] = create_formula(formula_expr, result)
            print(f"Successfully calculated formula: {formula_name}")
        except Exception as e:
            print(f"Error calculating formula {formula_name}: {str(e)}")
            result[formula_name] = np.nan

    # Save the results to a CSV file
    output_path = os.path.join(BASE_PATH, output_file)
    result.to_csv(output_path, index=False, encoding="utf-8")

    print(f"Saved {len(result)} companies with ratios to {output_path}")
    return result
