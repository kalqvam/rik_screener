import pandas as pd
import numpy as np
import re

from ..utils import (
    get_config,
    safe_read_csv,
    safe_write_csv,
    convert_to_numeric,
    extract_quoted_columns,
    log_info,
    log_warning,
    log_error
)

def create_formula(formula_expr, data):
    log_info(f"Processing formula: {formula_expr}")

    columns = extract_quoted_columns(formula_expr)
    log_info(f"Columns referenced in formula: {columns}")

    namespace = {}

    for col in columns:
        if col in data.columns:
            values = pd.to_numeric(data[col], errors='coerce').values
            values = np.nan_to_num(values, nan=1.0)
            namespace[col] = values
        else:
            raise ValueError(f"Column '{col}' not found in the data")

    namespace.update({
        'abs': np.abs,
        'min': np.minimum,
        'max': np.maximum,
        'sqrt': np.sqrt,
        'log': np.log,
        'log10': np.log10,
        'exp': np.exp,
        'round': np.round,
        'pow': np.power
    })

    for col in columns:
        formula_expr = formula_expr.replace(f'"{col}"', f'namespace["{col}"]')
        formula_expr = formula_expr.replace(f"'{col}'", f'namespace["{col}"]')

    try:
        result = eval(formula_expr)
        return result
    except Exception as e:
        raise ValueError(f"Error evaluating formula '{formula_expr}': {str(e)}")


def calculate_ratios(
    input_file: str = "merged_companies_multi_year.csv",
    output_file: str = "companies_with_ratios.csv",
    years: list = None,
    financial_items: list = None,
    formulas: dict = None
) -> pd.DataFrame:
    config = get_config()
    
    if years is None:
        years = config.get_years()

    years = sorted(years, reverse=True)

    if formulas is None:
        formulas = {
            f"EBITDA_Margin_{years[0]}": f'("Ärikasum (kahjum)_{years[0]}" + "Põhivarade kulum ja väärtuse langus_{years[0]}") / "Müügitulu_{years[0]}"',
            "Revenue_Growth": f'("Müügitulu_{years[0]}" - "Müügitulu_{years[1]}") / "Müügitulu_{years[1]}"',
        }

    all_formula_columns = []
    for formula in formulas.values():
        formula_cols = extract_quoted_columns(formula)
        log_info(f"Columns from formula: {formula_cols}")
        all_formula_columns.extend(formula_cols)

    if financial_items is None:
        financial_items = set()
        for col in all_formula_columns:
            if '_20' in col:
                base_col = col.split('_20')[0]
                financial_items.add(base_col)
            else:
                financial_items.add(col)
        financial_items = list(financial_items)

    log_info(f"Financial items to retrieve: {financial_items}")

    log_info(f"Loading merged companies from {input_file}")
    result = safe_read_csv(input_file)
    if result is None:
        log_error(f"Failed to load input file {input_file}")
        return None

    log_info(f"Loaded {len(result)} companies from merged data")
    log_info(f"Columns in merged data: {sorted(result.columns.tolist())}")

    for year in years:
        report_id_col = f"report_id_{year}"

        if report_id_col not in result.columns:
            log_warning(f"{report_id_col} not found in merged data. Skipping year {year}")
            continue

        financial_file = f"financials_{year}.csv"
        financial_data = safe_read_csv(
            financial_file,
            usecols=["report_id", "tabel", "elemendi_label", "vaartus"]
        )
        
        if financial_data is None:
            log_warning(f"Financial file for {year} not found")
            continue

        log_info(f"Reading financial data for {year}")

        try:
            financial_data['clean_elemendi_label'] = financial_data['elemendi_label'].str.replace(' Konsolideeritud$', '', regex=True)
            financial_data = financial_data[financial_data["clean_elemendi_label"].isin(financial_items)]
            log_info(f"Found {len(financial_data)} financial records for year {year}")

            financial_data['is_consolidated'] = (
                financial_data['tabel'].str.contains('Konsolideeritud', case=False, na=False) |
                financial_data['elemendi_label'].str.contains(' Konsolideeritud$', regex=True, na=False)
            )

            consolidated_reports = financial_data.groupby('report_id')['is_consolidated'].any()
            financial_data['has_consolidated'] = financial_data['report_id'].map(consolidated_reports)

            financial_data = financial_data[
                (~financial_data['has_consolidated']) |
                (financial_data['has_consolidated'] & financial_data['is_consolidated'])
            ]

            log_info(f"After consolidated filtering: {len(financial_data)} records")

            financial_wide = financial_data.pivot_table(
                index="report_id",
                columns="clean_elemendi_label",
                values="vaartus",
                aggfunc='first'
            ).reset_index()

            financial_cols = financial_wide.columns.difference(['report_id'])
            rename_dict = {col: f"{col}_{year}" for col in financial_cols}
            financial_wide = financial_wide.rename(columns=rename_dict)

            is_consolidated = financial_data.groupby('report_id')['is_consolidated'].any()
            financial_wide['is_consolidated'] = financial_wide['report_id'].map(is_consolidated)
            financial_wide = financial_wide.rename(columns={'is_consolidated': f'is_consolidated_{year}'})

            log_info(f"Financial columns for {year} after renaming: {sorted([col for col in financial_wide.columns if col != 'report_id'])}")

            result = pd.merge(
                result,
                financial_wide,
                left_on=report_id_col,
                right_on='report_id',
                how='left'
            )

            if 'report_id' in result.columns:
                result = result.drop(columns=['report_id'])

            log_info(f"Merged financial data for {year}. Current columns: {len(result.columns)}")

        except Exception as e:
            log_error(f"Error processing financial data for {year}: {str(e)}")
            import traceback
            traceback.print_exc()

    log_info("Converting financial columns to numeric and replacing NaN values with 1")

    all_financial_columns = []
    for item in financial_items:
        for year in years:
            col = f"{item}_{year}"
            if col in result.columns:
                all_financial_columns.append(col)

    result = convert_to_numeric(result, all_financial_columns, fill_value=1)

    log_info("Sample of financial data columns:")
    sample_cols = ['company_code'] + all_financial_columns[:3]
    log_info(str(result[sample_cols].head()))

    for formula_name, formula_expr in formulas.items():
        try:
            log_info(f"Calculating formula: {formula_name}")
            result[formula_name] = create_formula(formula_expr, result)
            log_info(f"Successfully calculated formula: {formula_name}")
        except Exception as e:
            log_error(f"Error calculating formula {formula_name}: {str(e)}")
            result[formula_name] = np.nan

    if safe_write_csv(result, output_file):
        log_info(f"Saved {len(result)} companies with ratios to {output_file}")
    else:
        log_error(f"Failed to save results to {output_file}")

    return result
