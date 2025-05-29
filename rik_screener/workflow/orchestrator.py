import pandas as pd
from typing import Dict, Any, Optional

from .config_validator import validate_config
from ..utils import get_config, log_step, log_info, log_warning, cleanup_temp_files
from ..df_prep.multi_year_merger import merge_multiple_years
from ..criteria_setup.calculations import calculate_ratios
from ..criteria_setup.calculation_utils import get_standard_formulas
from ..add_info.industry_codes import add_industry_classifications
from ..add_info.company_age import add_company_age
from ..add_info.emtak_descriptions import add_emtak_descriptions
from ..add_info.shareholder_data import add_ownership_data
from ..post_processing.scoring import score_companies
from ..post_processing.filtering import filter_and_rank
from ..post_processing.company_names import add_company_names


def run_company_screening(config: Dict[str, Any] = None, **kwargs) -> pd.DataFrame:
    final_config = _merge_config_and_kwargs(config, kwargs)
    validate_config(final_config)
    
    years = final_config['years']
    skip_steps = final_config.get('skip_steps', [])
    cleanup_intermediates = final_config.get('cleanup_intermediates', True)
    output_file = final_config.get('output_file', f"screening_results_{years[-1]}_{years[0]}.csv")
    
    log_step("Starting Company Screening Workflow")
    log_info(f"Years: {years}")
    log_info(f"Skip steps: {skip_steps}")
    
    current_file = _merge_multi_year_data(final_config)
    current_file = _calculate_financial_ratios(final_config, current_file)
    current_file = _add_enrichment_data(final_config, current_file, skip_steps)
    current_file = _score_and_filter_companies(final_config, current_file)
    result_df = _finalize_results(final_config, current_file, output_file)
    
    if cleanup_intermediates:
        cleanup_temp_files(pattern="screening_temp_*.csv")
    
    log_info("Company screening workflow completed successfully")
    return result_df


def _merge_config_and_kwargs(config: Optional[Dict[str, Any]], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    final_config = config.copy() if config else {}
    final_config.update(kwargs)
    return final_config


def _merge_multi_year_data(config: Dict[str, Any]) -> str:
    log_step("Merging Multi-Year Data")
    
    years = config['years']
    legal_forms = config.get('legal_forms', ["AS", "OÜ"])
    temp_file = f"screening_temp_merged_{years[-1]}_{years[0]}.csv"
    
    merged_df = merge_multiple_years(
        years=years,
        legal_forms=legal_forms,
        output_file=temp_file,
        require_all_years=True
    )
    
    if merged_df is None or merged_df.empty:
        raise RuntimeError("Failed to merge multi-year data")
    
    return temp_file


def _calculate_financial_ratios(config: Dict[str, Any], input_file: str) -> str:
    log_step("Calculating Financial Ratios")
    
    years = config['years']
    temp_file = f"screening_temp_ratios_{years[-1]}_{years[0]}.csv"
    
    formulas = _build_formulas(config)
    financial_items = config.get('financial_items', get_config().get_default('financial_items'))
    
    ratios_df = calculate_ratios(
        input_file=input_file,
        output_file=temp_file,
        years=years,
        formulas=formulas,
        financial_items=financial_items,
        use_standard_formulas=False
    )
    
    if ratios_df is None or ratios_df.empty:
        raise RuntimeError("Failed to calculate financial ratios")
    
    return temp_file


def _build_formulas(config: Dict[str, Any]) -> Dict[str, str]:
    years = config['years']
    standard_formulas_config = config.get('standard_formulas', {})
    custom_formulas = config.get('custom_formulas', {})
    
    formulas = {}
    
    if standard_formulas_config:
        standard_formulas = _get_customized_standard_formulas(standard_formulas_config, years)
        formulas.update(standard_formulas)
    
    formulas.update(custom_formulas)
    
    return formulas


def _get_customized_standard_formulas(standard_config: Dict[str, Any], years: list) -> Dict[str, str]:
    from ..criteria_setup.calculation_utils.standard_formulas import (
        ebitda_margin, roe, roa, asset_turnover, employee_efficiency,
        cash_ratio, current_ratio, debt_to_equity, labour_ratio,
        revenue_growth, revenue_cagr
    )
    
    formulas = {}
    
    for formula_type, config_val in standard_config.items():
        if formula_type == 'revenue_growth':
            for from_year, to_year in config_val.get('year_pairs', []):
                name = f"revenue_growth_{from_year}_to_{to_year}"
                formulas[name] = revenue_growth(from_year, to_year)
        
        elif formula_type == 'revenue_cagr':
            start_year = config_val.get('start_year')
            end_year = config_val.get('end_year')
            name = f"revenue_cagr_{start_year}_to_{end_year}"
            formulas[name] = revenue_cagr(start_year, end_year)
        
        else:
            use_averages = config_val.get('use_averages', True)
            binary = 0 if use_averages else 1
            
            for year in config_val.get('years', []):
                if formula_type == 'ebitda_margin':
                    formulas[f"ebitda_margin_{year}"] = ebitda_margin(year)
                elif formula_type == 'roe':
                    suffix = "_single" if not use_averages else ""
                    formulas[f"roe{suffix}_{year}"] = roe(year, binary)
                elif formula_type == 'roa':
                    suffix = "_single" if not use_averages else ""
                    formulas[f"roa{suffix}_{year}"] = roa(year, binary)
                elif formula_type == 'asset_turnover':
                    suffix = "_single" if not use_averages else ""
                    formulas[f"asset_turnover{suffix}_{year}"] = asset_turnover(year, binary)
                elif formula_type == 'employee_efficiency':
                    suffix = "_single" if not use_averages else ""
                    formulas[f"employee_efficiency{suffix}_{year}"] = employee_efficiency(year, binary)
                elif formula_type == 'cash_ratio':
                    formulas[f"cash_ratio_{year}"] = cash_ratio(year)
                elif formula_type == 'current_ratio':
                    formulas[f"current_ratio_{year}"] = current_ratio(year)
                elif formula_type == 'debt_to_equity':
                    formulas[f"debt_to_equity_{year}"] = debt_to_equity(year)
                elif formula_type == 'labour_ratio':
                    formulas[f"labour_ratio_{year}"] = labour_ratio(year)
    
    return formulas


def _add_enrichment_data(config: Dict[str, Any], input_file: str, skip_steps: list) -> str:
    years = config['years']
    current_file = input_file
    
    if 'industry' not in skip_steps:
        log_step("Adding Industry Classifications")
        temp_file = f"screening_temp_industry_{years[-1]}_{years[0]}.csv"
        result_df = add_industry_classifications(
            input_file=current_file,
            output_file=temp_file,
            revenues_file="revenues.csv",
            years=years
        )
        if result_df is not None:
            current_file = temp_file
    
    if 'age' not in skip_steps:
        log_step("Adding Company Age")
        temp_file = f"screening_temp_age_{years[-1]}_{years[0]}.csv"
        result_df = add_company_age(
            input_file=current_file,
            output_file=temp_file,
            legal_data_file="legal_data.csv"
        )
        if result_df is not None:
            current_file = temp_file
    
    if 'emtak' not in skip_steps:
        log_step("Adding EMTAK Descriptions")
        temp_file = f"screening_temp_emtak_{years[-1]}_{years[0]}.csv"
        result_df = add_emtak_descriptions(
            input_file=current_file,
            output_file=temp_file,
            emtak_file="emtak_2008.csv",
            years=years,
            create_combined_columns=True
        )
        if result_df is not None:
            current_file = temp_file
    
    if 'ownership' not in skip_steps:
        log_step("Adding Ownership Data")
        temp_file = f"screening_temp_ownership_{years[-1]}_{years[0]}.csv"
        ownership_filters = config.get('ownership_filters')
        result_df = add_ownership_data(
            input_file=current_file,
            output_file=temp_file,
            shareholders_file="shareholders.json",
            top_percentages=3,
            top_names=3,
            filters=ownership_filters
        )
        if result_df is not None:
            current_file = temp_file
    
    return current_file


def _score_and_filter_companies(config: Dict[str, Any], input_file: str) -> str:
    years = config['years']
    
    scoring_config = config.get('scoring_config')
    if scoring_config:
        log_step("Scoring Companies")
        temp_file = f"screening_temp_scored_{years[-1]}_{years[0]}.csv"
        scored_df = score_companies(
            input_file=input_file,
            output_file=temp_file,
            scoring_config=scoring_config,
            score_column="score"
        )
        if scored_df is not None:
            input_file = temp_file
    
    financial_filters = config.get('financial_filters')
    sort_column = config.get('sort_column', 'score')
    top_n = config.get('top_n')
    
    if financial_filters or sort_column or top_n:
        log_step("Filtering and Ranking Companies")
        temp_file = f"screening_temp_filtered_{years[-1]}_{years[0]}.csv"
        filtered_df = filter_and_rank(
            input_file=input_file,
            output_file=temp_file,
            sort_column=sort_column,
            filters=financial_filters,
            ascending=False,
            top_n=top_n,
            export_columns=None
        )
        if filtered_df is not None:
            input_file = temp_file
    
    return input_file


def _finalize_results(config: Dict[str, Any], input_file: str, output_file: str) -> pd.DataFrame:
    log_step("Finalizing Results")
    
    final_df = add_company_names(
        input_file=input_file,
        output_file=output_file,
        legal_data_file="legal_data.csv"
    )
    
    if final_df is None:
        from ..utils import safe_read_csv
        final_df = safe_read_csv(input_file)
        if final_df is None:
            raise RuntimeError("Failed to load final results")
    
    export_columns = config.get('export_columns')
    if export_columns:
        missing_columns = [col for col in export_columns if col not in final_df.columns]
        if missing_columns:
            log_warning(f"Export columns not found (skipping): {missing_columns}")
        
        available_columns = [col for col in export_columns if col in final_df.columns]
        if available_columns:
            final_df = final_df[available_columns]
            log_info(f"Exported {len(available_columns)} columns as requested")
    
    log_info(f"Final results: {len(final_df)} companies")
    return final_df
