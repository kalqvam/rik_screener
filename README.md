# RIK Screener

A Python package for screening and analyzing Estonian companies using RIK (Estonian Business Register) data.

## Overview

RIK Screener processes Estonian company financial data to identify investment opportunities through automated screening, ratio calculations, and scoring. It combines financial metrics, industry classifications, ownership data, and company demographics into a comprehensive analysis pipeline.

## Features

- **Multi-year Financial Analysis**: Process and compare company data across multiple years
- **Automated Ratio Calculations**: 15+ built-in financial ratios (EBITDA margin, ROE, ROA, debt ratios, etc.)
- **Custom Formula Engine**: Create and apply custom financial formulas
- **Company Scoring System**: Configurable scoring based on financial performance thresholds
- **Industry Classification**: Automatic EMTAK code mapping and industry descriptions
- **Ownership Analysis**: Shareholder structure and concentration analysis
- **Flexible Filtering**: Multi-criteria filtering and ranking capabilities

## Quick Start

### Installation

```python
pip install rik-screener
```

### Basic Usage

```python
import rik_screener

# Simple screening workflow
config = {
    'years': [2023, 2022, 2021],
    'legal_forms': ['AS', 'OÜ'],
    'standard_formulas': {
        'ebitda_margin': {'years': [2023]},
        'roe': {'years': [2023]},
        'debt_to_equity': {'years': [2023]}
    },
    'scoring_config': {
        'ebitda_margin_2023': {
            'thresholds': [
                {'min': 0.2, 'points': 3},
                {'min': 0.1, 'points': 2},
                {'min': 0.05, 'points': 1}
            ]
        }
    },
    'top_n': 100
}

# Run complete screening workflow
results = rik_screener.run_company_screening(config)
```

### Individual Components

```python
# Step-by-step approach
from rik_screener import (
    merge_multiple_years, 
    calculate_ratios, 
    score_companies,
    filter_and_rank
)

# 1. Merge multi-year data
merged_data = merge_multiple_years(
    years=[2023, 2022, 2021],
    legal_forms=['AS', 'OÜ']
)

# 2. Calculate financial ratios
ratios_data = calculate_ratios(
    input_data=merged_data,
    use_standard_formulas=True
)

# 3. Score companies
scored_data = score_companies(
    input_data=ratios_data,
    scoring_config=scoring_config
)

# 4. Filter and rank
final_results = filter_and_rank(
    input_data=scored_data,
    sort_column='score',
    top_n=50
)
```

## Data Requirements
Necessary files are available from official Estonian Business Registry: https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine

Place these CSV files in your data directory:
- `general_data.csv` - Aruannete üldandmed
- `financials_YYYY.csv` - Põhinäitajad yyyy. aasta aruannetest
- `revenues.csv` - EMTAK müügitulu jaotus  
- `legal_data.csv` - Ettevõtja rekvisiidid: Lihtandmed
- `shareholders.json` - Ettevõtja rekvisiidid: Osanikud
- `emtak_2008.csv` - mapping file, can be scraped or copied from https://ariregister.rik.ee/est/emtak_search

## Configuration

### Standard Formulas Available
- `ebitda_margin` - EBITDA margin calculation
- `roe` / `roa` - Return on equity/assets (with averaging options)
- `asset_turnover` - Asset efficiency ratios
- `current_ratio` - Liquidity ratios
- `debt_to_equity` - Leverage ratios
- `revenue_growth` - Growth calculations
- `revenue_cagr` - Compound annual growth rate

### Custom Formulas
```python
custom_formulas = {
    'custom_efficiency': '"Müügitulu_2023" / "Varad_2023"',
    'margin_stability': 'abs("EBITDA_Margin_2023" - "EBITDA_Margin_2022")'
}
```

### Scoring System
```python
scoring_config = {
    'roe_2023': {
        'thresholds': [
            {'min': 0.25, 'points': 5},  # Excellent
            {'min': 0.15, 'points': 3},  # Good
            {'min': 0.05, 'points': 1}   # Acceptable
        ]
    }
}
```

## Environment Setup

### Google Colab
```python
rik_screener.setup_environment()  # Mounts Google Drive
rik_screener.set_base_path('/content/drive/MyDrive/your_data_folder')
```

### Local Environment
```python
import os
os.environ['RIK_SCREENER_PATH'] = '/path/to/your/data'
```

## License

MIT License - see LICENSE file for details.

## Requirements

- Python 3.8+
- pandas >= 1.3.0
- numpy >= 1.20.0
