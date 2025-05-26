"""
RIK Screener Package - Central Import Hub
==========================================

This module provides a centralized import structure for the RIK Screener project.
All functions and classes are imported here to avoid circular dependencies and
provide a clean interface for the main scripts.

Usage:
    from rik_screener import *
    # or
    import rik_screener as rik
"""

# Core dependencies
import pandas as pd
import numpy as np
import os
import json
import re
from typing import List, Dict, Optional
from datetime import datetime

# Configuration
BASE_PATH = "/content/drive/MyDrive/Python/rik_screener"

# Import all functions from individual modules
from .data_preparation import filter_companies
from .multi_year_merger import merge_multiple_years
from .calculations import calculate_ratios, create_formula, extract_quoted_columns
from .industry_codes import add_industry_classifications
from .shareholder_data import add_ownership_data
from .filtering import filter_and_rank
from .emtak_assignment import replace_industry_codes, run_tool

# Version info
__version__ = "1.0.0"
__author__ = "RIK Screener Team"

# Export all functions
__all__ = [
    # Data preparation
    'filter_companies',
    
    # Multi-year operations
    'merge_multiple_years',
    
    # Calculations
    'calculate_ratios',
    'create_formula',
    'extract_quoted_columns',
    
    # Industry classification
    'add_industry_classifications',
    'replace_industry_codes',
    'run_tool',
    
    # Shareholder data
    'add_ownership_data',
    
    # Filtering and ranking
    'filter_and_rank',
    
    # Configuration
    'BASE_PATH'
]

def setup_environment():
    """
    Setup the Google Colab environment if needed.
    Call this function at the beginning of your analysis.
    """
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

# Initialize package
print(f"RIK Screener v{__version__} initialized")
print(f"Base path: {BASE_PATH}")
