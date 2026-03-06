"""
TASK 1: Generate Synthetic Healthcare Claims Data
This script creates realistic healthcare claims data for testing the ETL pipeline.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

np.random.seed(42)
random.seed(42)

# Generate providers (hospitals/clinics)
providers_data = {
    'provider_id': ['P101', 'P102', 'P103', 'P104', 'P105', 'P106', 'P107', 'P108', 'P109', 'P110'],
    'provider_name': [
        'Kenyatta National Hospital',
        'Nairobi Hospital',
        'Aga Khan University Hospital',
        'MP Shah Hospital',
        'Coptic Hospital',
        'Kisumu County Hospital',
        'Nakuru Provincial Hospital',
        'Mombasa County Hospital',
        'Eldoret Medical Centre',
        'Kericho Chest Hospital'
    ],
    'location': [
        'Nairobi', 'Nairobi', 'Nairobi', 'Nairobi', 'Nairobi',
        'Kisumu', 'Nakuru', 'Mombasa', 'Eldoret', 'Kericho'
    ],
    'provider_type': ['Hospital'] * 10,
    'established_year': [1999, 1990, 1980, 1972, 1960, 2001, 1998, 2000, 1995, 1987]
}

providers_df = pd.DataFrame(providers_data)

# Generate insurers (insurance companies)
insurers_data = {
    'insurer_id': ['I001', 'I002', 'I003', 'I004', 'I005'],
    'insurer_name': [
        'NHIF',
        'Britam',
        'CIC Insurance',
        'Old Mutual Kenya',
        'AAR Kenya'
    ],
    'headquarters': ['Nairobi'] * 5,
    'year_established': [1966, 1985, 1996, 1988, 1991]
}

insurers_df = pd.DataFrame(insurers_data)

# Diagnosis codes (real ICD-10 codes)
diagnosis_codes = {
    'J45.9': 'Asthma, unspecified',
    'E11.9': 'Type 2 diabetes mellitus',
    'I10.0': 'Essential hypertension',
    'K80.0': 'Calculus of gallbladder',
    'R01.1': 'Cardiac murmur',
    'M79.3': 'Panniculitis, unspecified',
    'Z12.11': 'Screening for malignant neoplasm of colon',
    'F41.9': 'Anxiety disorder, unspecified',
    'N18.3': 'Chronic kidney disease stage 3',
    'A15.0': 'Tuberculosis of lung'
}

# Generate claims data
num_claims = 5000
num_members = 500

claims_data = {
    'claim_id': [f'CLM{str(i).zfill(6)}' for i in range(1, num_claims + 1)],
    'member_id': [f'M{np.random.randint(1000, 1000 + num_members)}' for _ in range(num_claims)],
    'provider_id': [random.choice(providers_data['provider_id']) for _ in range(num_claims)],
    'claim_amount': np.random.uniform(1000, 50000, num_claims).round(2),
    'date_of_service': [
        datetime(2024, 1, 1) + timedelta(days=np.random.randint(0, 90))
        for _ in range(num_claims)
    ],
    'diagnosis_code': [random.choice(list(diagnosis_codes.keys())) for _ in range(num_claims)],
    'approved_amount': np.random.uniform(1000, 50000, num_claims).round(2),
    'status': [random.choice(['Approved', 'Pending', 'Rejected']) for _ in range(num_claims)],
    'insurance_plan_id': [f'PLAN_{random.choice(["A", "B", "C"])}' for _ in range(num_claims)],
    'insurer_id': [random.choice(insurers_data['insurer_id']) for _ in range(num_claims)]
}

claims_df = pd.DataFrame(claims_data)

# Add date_claimed (always after date_of_service)
claims_df['date_claimed'] = claims_df['date_of_service'] + pd.to_timedelta(
    np.random.randint(1, 7, num_claims), unit='D'
)

# Add location based on provider
claims_df = claims_df.merge(
    providers_df[['provider_id', 'location']], 
    on='provider_id', 
    how='left'
)

# Calculate processing days
claims_df['processing_days'] = (
    claims_df['date_claimed'] - claims_df['date_of_service']
).dt.days + np.random.randint(1, 5, num_claims)

# Add intentional data quality issues
# Duplicate claims
duplicate_indices = np.random.choice(len(claims_df), size=20, replace=False)
for idx in duplicate_indices:
    duplicate_row = claims_df.iloc[idx].copy()
    claims_df = pd.concat([claims_df, pd.DataFrame([duplicate_row])], ignore_index=True)

# Missing values in claim_amount
missing_amount_indices = np.random.choice(len(claims_df), size=50, replace=False)
claims_df.loc[missing_amount_indices, 'claim_amount'] = np.nan

# Outliers (very high claims)
outlier_indices = np.random.choice(len(claims_df), size=30, replace=False)
claims_df.loc[outlier_indices, 'claim_amount'] = np.random.uniform(100000, 500000, 30).round(2)

# Date inconsistencies (claimed before service)
date_error_indices = np.random.choice(len(claims_df), size=15, replace=False)
claims_df.loc[date_error_indices, 'date_claimed'] = claims_df.loc[date_error_indices, 'date_of_service'] - timedelta(days=2)

# Save to CSV files
os.makedirs('data/raw', exist_ok=True)

claims_df.to_csv('data/raw/claims.csv', index=False)
providers_df.to_csv('data/raw/providers.csv', index=False)
insurers_df.to_csv('data/raw/insurers.csv', index=False)