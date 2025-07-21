import pandas as pd
from typing import Dict, Any, Optional, Union

from .config_validator import validate_config
from ..utils import get_config, log_step, log_info, log_warning, cleanup_temp_files, safe_read_csv, reset_logger
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
    reset_logger()
    
    final_config = _merge_config_and_kwargs(config, kwargs)
    validate_config(final_config)
    
    use_dataframe_pipeline = final_config.get('use_dataframe_pipeline', False)
    years = final_config['years']
    output_file = final_config.get('output_file', f"screening_results_{years[-1]}_{years[0]}.csv")
    
    log_step("Starting Company Screening Workflow")
    log_info(f"Years: {years}")
    log_info(f"Pipeline mode: {'DataFrame' if use_dataframe_pipeline else 'CSV'}")
    
    if use_dataframe_pipeline:
        result_df = _run_dataframe_pipeline(final_config)
    else:
        result_df = _run_csv_pipeline(final_config)
    
    if final_config.get('save_final_output', True):
        from ..utils import safe_write_csv
        safe_write_csv(result_df, output_file, encoding='utf-8-sig')
        log_info(f"Final results saved to {output_file}")
    
    if final_config.get('cleanup_intermediates', True) and not use_dataframe_pipeline:
        cleanup_temp_files(pattern="screening_temp_*.csv")
    
    log_info("Company screening workflow completed successfully")
    return result_df


def _run_dataframe_pipeline(config: Dict[str, Any]) -> pd.DataFrame:
    log_info("Running DataFrame-based pipeline")
    
    current_df = _merge_multi_year_data_df(config)
    current_df = _calculate_financial_ratios_df(config, current_df)
    current_df = _add_enrichment_data_df(config, current_df)
    current_df = _score_and_filter_companies_df(config, current_df)
    result_df = _finalize_results_df(config, current_df)
    
    return result_df


def _run_csv_pipeline(config: Dict[str, Any]) -> pd.DataFrame:
    log_info("Running CSV-based pipeline")
    
    skip_steps = config.get('skip_steps', [])
    output_file = config.get('output_file', f"screening_results_{config['years'][-1]}_{config['years'][0]}.csv")
    
    current_file = _merge_multi_year_data(config)
    current_file = _calculate_financial_ratios(config, current_file)
    current_file = _add_enrichment_data(config, current_file, skip_steps)
    current_file = _score_and_filter_companies(config, current_file)
    result_df = _finalize_results(config, current_file, output_file)
    
    return result_df


def _merge_multi_year_data_df(config: Dict[str, Any]) -> pd.DataFrame:
    log_step("Merging Multi-Year Data")
    
    years = config['years']
    legal_forms = config.get('legal_forms', ["AS", "OÜ"])
    
    merged_df = merge_multiple_years(
        years=years,
        legal_forms=legal_forms,
        output_file=None,
        require_all_years=True,
        return_dataframe=True
    )
    
    if merged_df is None or merged_df.empty:
        raise RuntimeError("Failed to merge multi-year data")
    
    return merged_df


def _calculate_financial_ratios_df(config: Dict[str, Any], input_df: pd.DataFrame) -> pd.DataFrame:
    log_step("Calculating Financial Ratios")
    
    years = config['years']
    standard_formulas_config = config.get('standard_formulas', {})
    custom_formulas = config.get('custom_formulas', {})
    financial_items = config.get('financial_items', get_config().get_default('financial_items'))
    
    current_df = input_df
    
    if standard_formulas_config:
        log_info("Calculating standard formulas...")
        standard_formulas = _get_customized_standard_formulas(standard_formulas_config, years)
        
        current_df = calculate_ratios(
            input_data=current_df,
            years=years,
            formulas=standard_formulas,
            financial_items=financial_items,
            use_standard_formulas=False,
            return_dataframe=True
        )
        
        if current_df is None or current_df.empty:
            raise RuntimeError("Failed to calculate standard formulas")
    
    if custom_formulas:
        log_info("Calculating custom formulas...")
        
        from ..criteria_setup.calculation_utils import apply_formulas, validate_formulas
        
        valid_formulas, formula_errors = validate_formulas(custom_formulas, current_df)
        if formula_errors:
            log_warning(f"Found {len(formula_errors)} custom formula validation errors")
        
        current_df = apply_formulas(current_df, valid_formulas)
    
    if not standard_formulas_config and not custom_formulas:
        log_info("No formulas specified, merging financial data only...")
        current_df = calculate_ratios(
            input_data=current_df,
            years=years,
            formulas={},
            financial_items=financial_items,
            use_standard_formulas=False,
            return_dataframe=True
        )
        
        if current_df is None or current_df.empty:
            raise RuntimeError("Failed to merge financial data")
    
    return current_df


def _add_enrichment_data_df(config: Dict[str, Any], input_df: pd.DataFrame) -> pd.DataFrame:
    years = config['years']
    skip_steps = config.get('skip_steps', [])
    current_df = input_df
    
    if 'industry' not in skip_steps:
        log_step("Adding Industry Classifications")
        current_df = add_industry_classifications(
            input_data=current_df,
            revenues_file="revenues.csv",
            years=years,
            return_dataframe=True
        )
    
    if 'age' not in skip_steps:
        log_step("Adding Company Age")
        current_df = add_company_age(
            input_data=current_df,
            legal_data_file="legal_data.csv",
            return_dataframe=True
        )
    
    if 'emtak' not in skip_steps:
        log_step("Adding EMTAK Descriptions")
        current_df = add_emtak_descriptions(
            input_data=current_df,
            emtak_file="emtak_2025.csv",
            years=years,
            create_combined_columns=True,
            return_dataframe=True
        )
    
    if 'ownership' not in skip_steps:
        log_step("Adding Ownership Data")
        ownership_filters = config.get('ownership_filters')
        current_df = add_ownership_data(
            input_data=current_df,
            shareholders_file="shareholders.json",
            top_percentages=3,
            top_names=3,
            filters=ownership_filters,
            return_dataframe=True
        )
    
    return current_df


def _score_and_filter_companies_df(config: Dict[str, Any], input_df: pd.DataFrame) -> pd.DataFrame:
    current_df = input_df
    
    scoring_config = config.get('scoring_config')
    if scoring_config:
        log_step("Scoring Companies")
        current_df = score_companies(
            input_data=current_df,
            scoring_config=scoring_config,
            score_column="score",
            return_dataframe=True
        )
    
    financial_filters = config.get('financial_filters')
    sort_column = config.get('sort_column', 'score')
    top_n = config.get('top_n')
    
    if financial_filters or sort_column or top_n:
        log_step("Filtering and Ranking Companies")
        current_df = filter_and_rank(
            input_data=current_df,
            sort_column=sort_column,
            filters=financial_filters,
            ascending=False,
            top_n=top_n,
            export_columns=None,
            return_dataframe=True
        )
    
    return current_df


def _finalize_results_df(config: Dict[str, Any], input_df: pd.DataFrame) -> pd.DataFrame:
    log_step("Finalizing Results")
    
    final_df = add_company_names(
        input_data=input_df,
        legal_data_file="legal_data.csv",
        return_dataframe=True
    )
    
    if final_df is None:
        final_df = input_df
    
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
    temp_file_1 = f"screening_temp_standard_ratios_{years[-1]}_{years[0]}.csv"
    temp_file_2 = f"screening_temp_all_ratios_{years[-1]}_{years[0]}.csv"
    
    standard_formulas_config = config.get('standard_formulas', {})
    custom_formulas = config.get('custom_formulas', {})
    financial_items = config.get('financial_items', get_config().get_default('financial_items'))
    
    current_file = input_file
    
    if standard_formulas_config:
        log_info("Calculating standard formulas...")
        standard_formulas = _get_customized_standard_formulas(standard_formulas_config, years)
        
        ratios_df = calculate_ratios(
            input_file=current_file,
            output_file=temp_file_1,
            years=years,
            formulas=standard_formulas,
            financial_items=financial_items,
            use_standard_formulas=False
        )
        
        if ratios_df is None or ratios_df.empty:
            raise RuntimeError("Failed to calculate standard formulas")
        
        current_file = temp_file_1
    
    if custom_formulas:
        log_info("Calculating custom formulas...")

        current_df = safe_read_csv(current_file)
        if current_df is None:
            raise RuntimeError("Failed to load data for custom formula calculation")
        
        from ..criteria_setup.calculation_utils import apply_formulas, validate_formulas
        
        valid_formulas, formula_errors = validate_formulas(custom_formulas, current_df)
        if formula_errors:
            log_warning(f"Found {len(formula_errors)} custom formula validation errors")
        
        current_df = apply_formulas(current_df, valid_formulas)
        
        from ..utils import safe_write_csv
        if not safe_write_csv(current_df, temp_file_2):
            raise RuntimeError("Failed to save custom formula results")
        
        current_file = temp_file_2
    
    if not standard_formulas_config and not custom_formulas:
        log_info("No formulas specified, merging financial data only...")
        ratios_df = calculate_ratios(
            input_file=current_file,
            output_file=temp_file_1,
            years=years,
            formulas={},
            financial_items=financial_items,
            use_standard_formulas=False
        )
        
        if ratios_df is None or ratios_df.empty:
            raise RuntimeError("Failed to merge financial data")
        
        current_file = temp_file_1
    
    return current_file


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
            emtak_file="emtak_2025.csv",
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
