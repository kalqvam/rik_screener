import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional

from ..utils import (
    safe_read_csv,
    safe_write_csv,
    log_info,
    log_warning,
    log_error
)

from .scoring_config import validate_scoring_config


def score_companies(
    input_file: str = "companies_with_ownership.csv",
    output_file: str = "companies_with_scores.csv",
    scoring_config: Dict[str, Dict[str, Any]] = None,
    score_column: str = "score"
) -> pd.DataFrame:
    log_info(f"Loading companies data from {input_file}")
    
    companies_df = safe_read_csv(input_file)
    if companies_df is None:
        log_error(f"Failed to load input file {input_file}")
        return None
    
    log_info(f"Loaded {len(companies_df)} companies")
    
    if scoring_config is None:
        log_warning("No scoring configuration provided. Companies will have score of 0")
        companies_df[score_column] = 0
        return companies_df
    
    config_errors = validate_scoring_config(scoring_config)
    if config_errors:
        log_error(f"Invalid scoring configuration:")
        for error in config_errors:
            log_error(f"  - {error}")
        return None
    
    log_info(f"Applying scoring configuration with {len(scoring_config)} metrics")
    
    companies_df[score_column] = 0
    
    scoring_stats = {}
    
    for metric_name, metric_config in scoring_config.items():
        log_info(f"Processing metric: {metric_name}")
        
        if metric_name not in companies_df.columns:
            log_warning(f"Metric column '{metric_name}' not found in data. Skipping")
            continue
        
        thresholds = metric_config["thresholds"].copy()
        auto_sort = metric_config.get("auto_sort", True)
        
        if auto_sort:
            thresholds = _sort_thresholds(thresholds)
        
        metric_scores = _calculate_metric_scores(
            companies_df[metric_name], 
            thresholds, 
            metric_name
        )
        
        companies_df[score_column] += metric_scores
        
        scoring_stats[metric_name] = {
            "companies_scored": (metric_scores > 0).sum(),
            "total_companies": len(companies_df),
            "max_points_available": max([t["points"] for t in thresholds]),
            "avg_points_awarded": metric_scores.mean()
        }
    
    _log_scoring_summary(scoring_stats, companies_df[score_column])
    
    if safe_write_csv(companies_df, output_file):
        log_info(f"Saved {len(companies_df)} companies with scores to {output_file}")
    else:
        log_error(f"Failed to save results to {output_file}")
    
    return companies_df


def _sort_thresholds(thresholds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    min_thresholds = [t for t in thresholds if "min" in t]
    max_thresholds = [t for t in thresholds if "max" in t]
    
    min_thresholds.sort(key=lambda x: (-x["points"], -x["min"]))
    max_thresholds.sort(key=lambda x: (-x["points"], x["max"]))
    
    if min_thresholds and max_thresholds:
        log_warning("Metric has both min and max thresholds. This may cause unexpected behavior.")
        return min_thresholds + max_thresholds
    elif min_thresholds:
        return min_thresholds
    else:
        return max_thresholds


def _calculate_metric_scores(
    series: pd.Series, 
    thresholds: List[Dict[str, Any]], 
    metric_name: str
) -> pd.Series:
    scores = pd.Series(0, index=series.index)
    
    valid_mask = series.notna() & pd.to_numeric(series, errors='coerce').notna()
    valid_series = pd.to_numeric(series[valid_mask], errors='coerce')
    
    if valid_mask.sum() == 0:
        log_warning(f"No valid numeric values found for metric '{metric_name}'")
        return scores
    
    companies_with_data = valid_mask.sum()
    log_info(f"  {companies_with_data} companies have valid data for {metric_name}")
    
    for threshold in thresholds:
        points = threshold["points"]
        
        if "min" in threshold:
            condition = valid_series >= threshold["min"]
            new_scores = (condition & (scores[valid_mask] == 0))
            scores.loc[valid_series.index[new_scores]] = points
            
            log_info(f"    {new_scores.sum()} companies scored {points} points (>= {threshold['min']:.3f})")
            
        elif "max" in threshold:
            condition = valid_series <= threshold["max"]
            new_scores = (condition & (scores[valid_mask] == 0))
            scores.loc[valid_series.index[new_scores]] = points
            
            log_info(f"    {new_scores.sum()} companies scored {points} points (<= {threshold['max']:.3f})")
    
    return scores


def _log_scoring_summary(scoring_stats: Dict[str, Dict], total_scores: pd.Series):
    log_info("=== SCORING SUMMARY ===")
    
    total_companies = len(total_scores)
    companies_with_scores = (total_scores > 0).sum()
    max_possible_score = sum([stats["max_points_available"] for stats in scoring_stats.values()])
    
    log_info(f"Total companies: {total_companies}")
    log_info(f"Companies with scores > 0: {companies_with_scores}")
    log_info(f"Maximum possible score: {max_possible_score}")
    log_info(f"Average total score: {total_scores.mean():.2f}")
    log_info(f"Highest total score: {total_scores.max()}")
    
    for metric_name, stats in scoring_stats.items():
        coverage = (stats["companies_scored"] / stats["total_companies"]) * 100
        log_info(f"{metric_name}: {stats['companies_scored']}/{stats['total_companies']} companies scored ({coverage:.1f}%), avg: {stats['avg_points_awarded']:.2f}")
    
    if len(total_scores) > 0:
        unique_scores = sorted(total_scores.unique())
        log_info(f"Score distribution: {dict(total_scores.value_counts().sort_index())}")


def preview_scoring(
    data: pd.DataFrame,
    scoring_config: Dict[str, Dict[str, Any]],
    sample_size: int = 10
) -> pd.DataFrame:
    if len(data) == 0:
        log_warning("No data provided for preview")
        return pd.DataFrame()
    
    sample_data = data.head(sample_size).copy()
    
    sample_data["preview_score"] = 0
    
    for metric_name, metric_config in scoring_config.items():
        if metric_name not in sample_data.columns:
            continue
            
        thresholds = metric_config["thresholds"].copy()
        if metric_config.get("auto_sort", True):
            thresholds = _sort_thresholds(thresholds)
        
        metric_scores = _calculate_metric_scores(
            sample_data[metric_name], 
            thresholds, 
            metric_name
        )
        
        sample_data["preview_score"] += metric_scores
        sample_data[f"{metric_name}_points"] = metric_scores
    
    return sample_data


def get_scoring_metrics_from_data(data: pd.DataFrame) -> List[str]:
    potential_metrics = []
    
    ratio_patterns = [
        "margin", "ratio", "turnover", "growth", "roe", "roa", 
        "efficiency", "cagr", "debt", "equity", "current"
    ]
    
    for col in data.columns:
        col_lower = col.lower()
        
        if any(pattern in col_lower for pattern in ratio_patterns):
            if pd.api.types.is_numeric_dtype(data[col]) or data[col].dtype == 'object':
                try:
                    numeric_series = pd.to_numeric(data[col], errors='coerce')
                    if numeric_series.notna().sum() > 0:
                        potential_metrics.append(col)
                except:
                    continue
    
    return potential_metrics
