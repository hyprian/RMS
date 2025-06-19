# RMS/data_ingestion/utils.py
import pandas as pd
import re

def clean_numeric_value(value):
    """
    Cleans a string value to make it suitable for numeric conversion.
    Removes currency symbols, commas, and handles potential errors.
    """
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s_value = str(value).strip()
    # Remove currency symbols (₹, $, etc.) and commas
    s_value = re.sub(r'[^\d.-]', '', s_value) # Keep digits, decimal, and minus sign
    
    try:
        return float(s_value) if s_value else 0.0
    except ValueError:
        return 0.0

def clean_integer_value(value):
    """
    Cleans a string value to make it suitable for integer conversion.
    """
    if pd.isna(value):
        return 0
    if isinstance(value, (int, float)): # if float, convert to int
        return int(value)

    s_value = str(value).strip()
    s_value = re.sub(r'[^\d-]', '', s_value) # Keep digits and minus sign for integers

    try:
        return int(s_value) if s_value else 0
    except ValueError:
        return 0