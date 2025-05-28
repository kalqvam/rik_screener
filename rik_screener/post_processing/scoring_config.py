from typing import Dict, List, Any


def get_default_scoring_config(years: List[int] = None) -> Dict[str, Dict[str, Any]]:
    if years is None:
        years = [2023]
    
    primary_year = max(years)
    
    config = {
        f"ebitda_margin_{primary_year}": {
            "thresholds": [
                {"min": 0.40, "points": 3},
                {"min": 0.20, "points": 2},
                {"min": 0.10, "points": 1}
            ],
            "auto_sort": True
        },
        f"roe_{primary_year}": {
            "thresholds": [
                {"min": 0.25, "points": 3},
                {"min": 0.15, "points": 2},
                {"min": 0.10, "points": 1}
            ],
            "auto_sort": True
        },
        f"asset_turnover_{primary_year}": {
            "thresholds": [
                {"min": 2.0, "points": 3},
                {"min": 1.0, "points": 2},
                {"min": 0.5, "points": 1}
            ],
            "auto_sort": True
        },
        f"debt_to_equity_{primary_year}": {
            "thresholds": [
                {"max": 0.3, "points": 3},
                {"max": 0.5, "points": 2},
                {"max": 0.8, "points": 1}
            ],
            "auto_sort": True
        },
        f"current_ratio_{primary_year}": {
            "thresholds": [
                {"min": 2.0, "points": 2},
                {"min": 1.2, "points": 1}
            ],
            "auto_sort": True
        }
    }
    
    if len(years) >= 2:
        years_sorted = sorted(years, reverse=True)
        from_year = years_sorted[1]
        to_year = years_sorted[0]
        
        config[f"revenue_growth_{from_year}_to_{to_year}"] = {
            "thresholds": [
                {"min": 0.30, "points": 3},
                {"min": 0.15, "points": 2},
                {"min": 0.05, "points": 1}
            ],
            "auto_sort": True
        }
    
    return config


def create_custom_scoring_config(
    metrics: Dict[str, List[Dict[str, Any]]],
    auto_sort_all: bool = True
) -> Dict[str, Dict[str, Any]]:
    config = {}
    
    for metric, thresholds in metrics.items():
        config[metric] = {
            "thresholds": thresholds,
            "auto_sort": auto_sort_all
        }
    
    return config


def validate_scoring_config(config: Dict[str, Dict[str, Any]]) -> List[str]:
    errors = []
    
    for metric_name, metric_config in config.items():
        if not isinstance(metric_config, dict):
            errors.append(f"Metric '{metric_name}' config must be a dictionary")
            continue
        
        if "thresholds" not in metric_config:
            errors.append(f"Metric '{metric_name}' missing 'thresholds' key")
            continue
        
        thresholds = metric_config["thresholds"]
        if not isinstance(thresholds, list) or len(thresholds) == 0:
            errors.append(f"Metric '{metric_name}' thresholds must be a non-empty list")
            continue
        
        for i, threshold in enumerate(thresholds):
            if not isinstance(threshold, dict):
                errors.append(f"Metric '{metric_name}' threshold {i} must be a dictionary")
                continue
            
            if "points" not in threshold:
                errors.append(f"Metric '{metric_name}' threshold {i} missing 'points' key")
                continue
            
            has_min = "min" in threshold
            has_max = "max" in threshold
            
            if not has_min and not has_max:
                errors.append(f"Metric '{metric_name}' threshold {i} must have either 'min' or 'max' key")
            elif has_min and has_max:
                errors.append(f"Metric '{metric_name}' threshold {i} cannot have both 'min' and 'max' keys")
            
            try:
                points = float(threshold["points"])
                if points < 0:
                    errors.append(f"Metric '{metric_name}' threshold {i} points must be non-negative")
            except (ValueError, TypeError):
                errors.append(f"Metric '{metric_name}' threshold {i} points must be a number")
    
    return errors
