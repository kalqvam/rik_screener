from typing import Dict, List


def get_standard_formulas(years: List[int] = None) -> Dict[str, str]:
    if years is None:
        years = [2023, 2022, 2021]
    
    years = sorted(years, reverse=True)
    
    formulas = {}
    
    for year in years:
        formulas.update({
            f"ebitda_margin_{year}": ebitda_margin(year),
            f"roe_{year}": roe(year),
            f"roe_single_{year}": roe(year, binary=1),
            f"roa_{year}": roa(year),
            f"roa_single_{year}": roa(year, binary=1),
            f"asset_turnover_{year}": asset_turnover(year),
            f"asset_turnover_single_{year}": asset_turnover(year, binary=1),
            f"employee_efficiency_{year}": employee_efficiency(year),
            f"employee_efficiency_single_{year}": employee_efficiency(year, binary=1),
            f"cash_ratio_{year}": cash_ratio(year),
            f"current_ratio_{year}": current_ratio(year),
            f"debt_to_equity_{year}": debt_to_equity(year),
            f"labour_ratio_{year}": labour_ratio(year),
        })
    
    if len(years) >= 2:
        for i in range(len(years)-1):
            to_year = years[i]
            from_year = years[i+1]
            formulas[f"revenue_growth_{from_year}_to_{to_year}"] = revenue_growth(from_year, to_year)
    
    if len(years) >= 3:
        start_year = years[-1]
        end_year = years[0]
        num_years = len(years) - 1
        formulas[f"revenue_cagr_{start_year}_to_{end_year}"] = revenue_cagr(start_year, end_year)
    
    return formulas


def ebitda_margin(year: int) -> str:
    return f'("Ärikasum (kahjum)_{year}" + abs("Põhivarade kulum ja väärtuse langus_{year}")) / "Müügitulu_{year}"'


def revenue_growth(from_year: int, to_year: int) -> str:
    return f'(("Müügitulu_{to_year}" / "Müügitulu_{from_year}") - 1)'


def revenue_cagr(start_year: int, end_year: int) -> str:
    years_diff = end_year - start_year
    return f'(pow(("Müügitulu_{end_year}" / "Müügitulu_{start_year}"), 1/{years_diff}) - 1)'


def asset_turnover(year: int, binary: int = 0) -> str:
    if binary == 1:
        return f'"Müügitulu_{year}" / "Varad_{year}"'
    else:
        prev_year = year - 1
        return f'"Müügitulu_{year}" / (("Varad_{year}" + "Varad_{prev_year}") / 2)'


def roe(year: int, binary: int = 0) -> str:
    if binary == 1:
        return f'"Aruandeaasta kasum (kahjum)_{year}" / "Omakapital_{year}"'
    else:
        prev_year = year - 1
        return f'"Aruandeaasta kasum (kahjum)_{year}" / (("Omakapital_{year}" + "Omakapital_{prev_year}") / 2)'


def roa(year: int, binary: int = 0) -> str:
    if binary == 1:
        return f'"Aruandeaasta kasum (kahjum)_{year}" / "Varad_{year}"'
    else:
        prev_year = year - 1
        return f'"Aruandeaasta kasum (kahjum)_{year}" / (("Varad_{year}" + "Varad_{prev_year}") / 2)'


def employee_efficiency(year: int, binary: int = 0) -> str:
    if binary == 1:
        return f'"Müügitulu_{year}" / "Töötajate keskmine arv taandatud täistööajale_{year}"'
    else:
        prev_year = year - 1
        return f'"Müügitulu_{year}" / (("Töötajate keskmine arv taandatud täistööajale_{year}" + "Töötajate keskmine arv taandatud täistööajale_{prev_year}") / 2)'


def cash_ratio(year: int) -> str:
    return f'"Raha_{year}" / "Lühiajalised kohustised_{year}"'


def current_ratio(year: int) -> str:
    return f'"Käibevarad_{year}" / "Lühiajalised kohustised_{year}"'


def debt_to_equity(year: int) -> str:
    return f'("Lühiajalised kohustised_{year}" + "Pikaajalised kohustised_{year}") / "Omakapital_{year}"'


def labour_ratio(year: int) -> str:
    return f'-("Tööjõukulud_{year}") / "Müügitulu_{year}"'


def get_available_formulas() -> List[str]:
    return [
        'ebitda_margin',
        'revenue_growth', 
        'revenue_cagr',
        'asset_turnover',
        'roe',
        'roa', 
        'employee_efficiency',
        'cash_ratio',
        'current_ratio',
        'debt_to_equity',
        'labour_ratio'
    ]


def build_custom_formula_set(
    years: List[int],
    include_growth: bool = True,
    include_efficiency: bool = True,
    include_liquidity: bool = True,
    include_leverage: bool = True,
    use_averages: bool = True
) -> Dict[str, str]:
    formulas = {}
    years = sorted(years, reverse=True)
    binary = 0 if use_averages else 1
    suffix = "" if use_averages else "_single"
    
    for year in years:
        formulas[f"ebitda_margin_{year}"] = ebitda_margin(year)
    
    if include_efficiency:
        for year in years:
            formulas.update({
                f"roe{suffix}_{year}": roe(year, binary),
                f"roa{suffix}_{year}": roa(year, binary),
                f"asset_turnover{suffix}_{year}": asset_turnover(year, binary),
                f"employee_efficiency{suffix}_{year}": employee_efficiency(year, binary),
            })
    
    if include_liquidity:
        for year in years:
            formulas.update({
                f"cash_ratio_{year}": cash_ratio(year),
                f"current_ratio_{year}": current_ratio(year),
            })
    
    if include_leverage:
        for year in years:
            formulas.update({
                f"debt_to_equity_{year}": debt_to_equity(year),
                f"labour_ratio_{year}": labour_ratio(year),
            })
    
    if include_growth and len(years) >= 2:
        for i in range(len(years)-1):
            to_year = years[i]
            from_year = years[i+1]
            formulas[f"revenue_growth_{from_year}_to_{to_year}"] = revenue_growth(from_year, to_year)
        
        if len(years) >= 3:
            start_year = years[-1]
            end_year = years[0]
            formulas[f"revenue_cagr_{start_year}_to_{end_year}"] = revenue_cagr(start_year, end_year)
    
    return formulas
