"""
Transform module
Cleans and validates data
"""

import pandas as pd
import numpy as np

def transform_claims(claims_df):
    """
    Transform and clean claims data:
    - Handle missing values
    - Remove duplicates
    - Validate amounts
    - Fix date inconsistencies
    - Standardize formats
    """
    original_count = len(claims_df)
    
    # Remove complete duplicates
    claims_df = claims_df.drop_duplicates()
    
    # Fill missing claim amounts with median
    median_amount = claims_df['claim_amount'].median()
    claims_df['claim_amount'].fillna(median_amount, inplace=True)
    
    # Remove invalid amounts (zero or negative)
    claims_df = claims_df[claims_df['claim_amount'] > 0]
    
    # Fix date inconsistencies (claimed before service)
    mask = claims_df['date_claimed'] < claims_df['date_of_service']
    if mask.any():
        claims_df.loc[mask, ['date_claimed', 'date_of_service']] = \
            claims_df.loc[mask, ['date_of_service', 'date_claimed']].values
    
    # Convert date columns to datetime
    claims_df['date_of_service'] = pd.to_datetime(claims_df['date_of_service'])
    claims_df['date_claimed'] = pd.to_datetime(claims_df['date_claimed'])
    
    # Standardize status values
    claims_df['status'] = claims_df['status'].str.strip().str.capitalize()
    
    # Fill missing locations
    claims_df['location'].fillna('Unknown', inplace=True)
    
    # Ensure numeric columns are properly typed
    claims_df['claim_amount'] = pd.to_numeric(claims_df['claim_amount'], errors='coerce')
    claims_df['approved_amount'] = pd.to_numeric(claims_df['approved_amount'], errors='coerce')
    claims_df['processing_days'] = pd.to_numeric(claims_df['processing_days'], errors='coerce')
    
    return claims_df

def transform_providers(providers_df):
    """Transform providers data"""
    providers_df = providers_df.drop_duplicates()
    
    # Standardize text fields
    providers_df['provider_name'] = providers_df['provider_name'].str.strip()
    providers_df['location'] = providers_df['location'].str.strip()
    providers_df['provider_type'] = providers_df['provider_type'].str.strip()
    
    return providers_df

def transform_insurers(insurers_df):
    """Transform insurers data"""
    insurers_df = insurers_df.drop_duplicates()
    
    # Standardize text fields
    insurers_df['insurer_name'] = insurers_df['insurer_name'].str.strip()
    insurers_df['headquarters'] = insurers_df['headquarters'].str.strip()
    
    return insurers_df

def transform_all_data(claims_df, providers_df, insurers_df):
    """Transform all datasets"""
    claims_df = transform_claims(claims_df)
    providers_df = transform_providers(providers_df)
    insurers_df = transform_insurers(insurers_df)
    
    return claims_df, providers_df, insurers_df