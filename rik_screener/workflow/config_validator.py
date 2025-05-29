from typing import Dict, List, Any, Set
import pandas as pd


def validate_config(config: Dict[str, Any]) -> None:
    if not isinstance(config, dict):
        raise ValueError("Config must be a dictionary")
    
    _validate_years(config.get('years'))
    _validate_legal_forms(config.get('legal_forms'))
    _validate_skip_steps(config.get('skip_steps'))
    _validate_pipeline_mode(config.get('use_dataframe_pipeline'))
    _validate_formulas(config)
    _validate_scoring_config(config.get('scoring_config'))
    _validate_financial_filters(config.get('financial_filters'))
    _validate_ownership_filters(config.get('ownership_filters'))


def _validate_years(years):
    if years is None:
        raise ValueError("Years must be specified")
    if not isinstance(years, list) or len(years) == 0:
        raise ValueError("Years must be a non-empty list")
    if not all(isinstance(y, int) and 2000 <= y <= 2030 for y in years):
        raise ValueError("Years must be integers between 2000 and 2030")


def _validate_legal_forms(legal_forms):
    if legal_forms is not None:
        if not isinstance(legal_forms, list):
            raise ValueError("Legal forms must be a list")
        valid_forms = ["AS", "OÜ"]
        if not all(form in valid_forms for form in legal_forms):
            raise ValueError(f"Legal forms must be from {valid_forms}")


def _validate_skip_steps(skip_steps):
    if skip_steps is not None:
        if not isinstance(skip_steps, list):
            raise ValueError("Skip steps must be a list")
        valid_steps = ["industry", "age", "emtak", "ownership"]
        invalid_steps = [step for step in skip_steps if step not in valid_steps]
        if invalid_steps:
            raise ValueError(f"Invalid skip steps: {invalid_steps}. Valid options: {valid_steps}")


def _validate_pipeline_mode(use_dataframe_pipeline):
    if use_dataframe_pipeline is not None:
        if not isinstance(use_dataframe_pipeline, bool):
            raise ValueError("use_dataframe_pipeline must be a boolean")


def _validate_formulas(config):
    standard_formulas = config.get('standard_formulas', {})
    custom_formulas = config.get('custom_formulas', {})
    
    if not isinstance(standard_formulas, dict):
        raise ValueError("Standard formulas must be a dictionary")
    if not isinstance(custom_formulas, dict):
        raise ValueError("Custom formulas must be a dictionary")
    
    _validate_standard_formulas(standard_formulas, config.get('years', []))
    _validate_custom_formulas(custom_formulas)
    
    standard_names = _get_generated_formula_names(standard_formulas, config.get('years', []))
    custom_names = set(custom_formulas.keys())
    
    overlapping = standard_names & custom_names
    if overlapping:
        raise ValueError(f"Overlapping formula names between standard and custom: {overlapping}")


def _validate_standard_formulas(standard_formulas, years):
    valid_formula_types = [
        'ebitda_margin', 'roe', 'roa', 'asset_turnover', 'employee_efficiency',
        'cash_ratio', 'current_ratio', 'debt_to_equity', 'labour_ratio', 
        'revenue_growth', 'revenue_cagr'
    ]
    
    for formula_type, config_val in standard_formulas.items():
        if formula_type not in valid_formula_types:
            raise ValueError(f"Invalid standard formula type: {formula_type}")
        
        if formula_type in ['revenue_growth']:
            if not isinstance(config_val, dict) or 'year_pairs' not in config_val:
                raise ValueError(f"{formula_type} must have 'year_pairs' specified")
            if not isinstance(config_val['year_pairs'], list):
                raise ValueError(f"{formula_type} year_pairs must be a list")
            for pair in config_val['year_pairs']:
                if not isinstance(pair, list) or len(pair) != 2:
                    raise ValueError(f"{formula_type} year_pairs must contain lists of 2 years each")
        
        elif formula_type in ['revenue_cagr']:
            if not isinstance(config_val, dict):
                raise ValueError(f"{formula_type} must be a dictionary")
            if 'start_year' not in config_val or 'end_year' not in config_val:
                raise ValueError(f"{formula_type} must have 'start_year' and 'end_year'")
        
        else:
            if not isinstance(config_val, dict) or 'years' not in config_val:
                raise ValueError(f"{formula_type} must have 'years' specified")
            if not isinstance(config_val['years'], list):
                raise ValueError(f"{formula_type} years must be a list")


def _validate_custom_formulas(custom_formulas):
    for name, formula in custom_formulas.items():
        if not isinstance(name, str) or not name:
            raise ValueError("Custom formula names must be non-empty strings")
        if not isinstance(formula, str) or not formula:
            raise ValueError("Custom formula expressions must be non-empty strings")


def _get_generated_formula_names(standard_formulas, years):
    names = set()
    
    for formula_type, config_val in standard_formulas.items():
        if formula_type == 'revenue_growth':
            for from_year, to_year in config_val.get('year_pairs', []):
                names.add(f"revenue_growth_{from_year}_to_{to_year}")
        elif formula_type == 'revenue_cagr':
            start_year = config_val.get('start_year')
            end_year = config_val.get('end_year')
            if start_year and end_year:
                names.add(f"revenue_cagr_{start_year}_to_{end_year}")
        else:
            use_averages = config_val.get('use_averages', True)
            for year in config_val.get('years', []):
                if formula_type in ['roe', 'roa', 'asset_turnover', 'employee_efficiency']:
                    suffix = "_single" if not use_averages else ""
                    names.add(f"{formula_type}{suffix}_{year}")
                else:
                    names.add(f"{formula_type}_{year}")
    
    return names


def _validate_scoring_config(scoring_config):
    if scoring_config is not None:
        from ..post_processing.scoring_config import validate_scoring_config
        errors = validate_scoring_config(scoring_config)
        if errors:
            raise ValueError(f"Scoring config validation errors: {errors}")


def _validate_financial_filters(financial_filters):
    if financial_filters is not None:
        if not isinstance(financial_filters, list):
            raise ValueError("Financial filters must be a list")
        for i, filter_dict in enumerate(financial_filters):
            if not isinstance(filter_dict, dict):
                raise ValueError(f"Filter {i} must be a dictionary")
            if 'column' not in filter_dict:
                raise ValueError(f"Filter {i} must have 'column' specified")


def _validate_ownership_filters(ownership_filters):
    if ownership_filters is not None:
        if not isinstance(ownership_filters, dict):
            raise ValueError("Ownership filters must be a dictionary")
