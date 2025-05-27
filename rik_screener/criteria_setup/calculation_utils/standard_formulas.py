from typing import Dict, List


def get_standard_formulas(years: List[int] = None) -> Dict[str, str]:
    if years is None:
        years = [2023, 2022, 2021]
    
    years = sorted(years, reverse=True)
    
    return {}
