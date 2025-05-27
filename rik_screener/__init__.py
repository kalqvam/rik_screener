import pandas as pd
import numpy as np
import os
import json
import re
from typing import List, Dict, Optional
from datetime import datetime

# Make BASE_PATH configurable instead of hard-coded
DEFAULT_BASE_PATH = "/content/drive/MyDrive/Python/rik_screener"
BASE_PATH = os.getenv('RIK_SCREENER_PATH', DEFAULT_BASE_PATH)

# Use relative imports to avoid circular import issues
from .data_preparation import filter_companies
from .multi_year_merger import merge_multiple_years
from .calculations import calculate_ratios, create_formula, extract_quoted_columns
from .industry_codes import add_industry_classifications
from .shareholder_data import add_ownership_data
from .filtering import filter_and_rank

__version__ = "1.0.0"
__author__ = "kalqvam"

__all__ = [
    'filter_companies',
    'merge_multiple_years',
    'calculate_ratios',
    'create_formula',
    'extract_quoted_columns',
    'add_industry_classifications',
    'add_ownership_data',
    'filter_and_rank',
    'BASE_PATH',
    'setup_environment',
    'get_timestamp',
    'validate_base_path',
    'set_base_path'
]

def set_base_path(path: str):
    """Set the base path for data files."""
    global BASE_PATH
    BASE_PATH = path
    # Update BASE_PATH in all modules
    from . import data_preparation
    from . import multi_year_merger
    from . import calculations
    from . import industry_codes
    from . import shareholder_data
    from . import filtering
    
    data_preparation.BASE_PATH = path
    multi_year_merger.BASE_PATH = path
    calculations.BASE_PATH = path
    industry_codes.BASE_PATH = path
    shareholder_data.BASE_PATH = path
    filtering.BASE_PATH = path

def setup_environment():
    """Setup the environment (mount Google Drive if in Colab)."""
    try:
        from google.colab import drive
        drive.mount('/content/drive')
        print("Google Drive mounted successfully")
        return True
    except ImportError:
        print("Running outside of Google Colab")
        return False

def get_timestamp():
    """Generate a timestamp string for file naming."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def validate_base_path():
    """Validate that the base path exists and is accessible."""
    if not os.path.exists(BASE_PATH):
        print(f"Warning: Base path {BASE_PATH} does not exist")
        return False
    return True

print(f"RIK Screener v{__version__} initialized")
print(f"Base path: {BASE_PATH}")
print("Use set_base_path() to change the data directory if needed")
