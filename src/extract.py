"""
Extract module
Reads data from CSV files
"""

import pandas as pd

def load_claims_csv(filepath='data/raw/claims.csv'):
    """Load claims CSV file"""
    try:
        df = pd.read_csv(filepath)
        return df
    except Exception as e:
        raise Exception(f"Error loading claims CSV: {e}")

def load_providers_csv(filepath='data/raw/providers.csv'):
    """Load providers CSV file"""
    try:
        df = pd.read_csv(filepath)
        return df
    except Exception as e:
        raise Exception(f"Error loading providers CSV: {e}")

def load_insurers_csv(filepath='data/raw/insurers.csv'):
    """Load insurers CSV file"""
    try:
        df = pd.read_csv(filepath)
        return df
    except Exception as e:
        raise Exception(f"Error loading insurers CSV: {e}")

def load_all_data():
    """Load all data files"""
    claims_df = load_claims_csv()
    providers_df = load_providers_csv()
    insurers_df = load_insurers_csv()
    
    return claims_df, providers_df, insurers_df