"""
Feature Engineering Module
Creates machine learning-ready features from claims data
"""

import pandas as pd
import numpy as np
import os

def engineer_features(claims_df, providers_df):
    """Create features for machine learning models"""
    
    df = claims_df.copy()
    
    # Temporal features
    df['date_of_service'] = pd.to_datetime(df['date_of_service'])
    df['date_claimed'] = pd.to_datetime(df['date_claimed'])
    df['days_to_claim'] = (df['date_claimed'] - df['date_of_service']).dt.days
    df['service_month'] = df['date_of_service'].dt.month
    df['service_day_of_week'] = df['date_of_service'].dt.dayofweek
    df['is_weekend'] = (df['service_day_of_week'] >= 5).astype(int)
    
    # Claim amount features
    claim_mean = df['claim_amount'].mean()
    claim_std = df['claim_amount'].std()
    claim_q95 = df['claim_amount'].quantile(0.95)
    
    df['claim_amount_normalized'] = (df['claim_amount'] - claim_mean) / claim_std
    df['is_high_value'] = (df['claim_amount'] > claim_q95).astype(int)
    df['is_low_value'] = (df['claim_amount'] < df['claim_amount'].quantile(0.25)).astype(int)
    df['approval_difference'] = df['claim_amount'] - df['approved_amount']
    df['approval_ratio'] = df['approved_amount'] / df['claim_amount']
    df['approval_ratio'] = df['approval_ratio'].fillna(1.0)
    df['is_approved'] = (df['status'] == 'Approved').astype(int)
    df['is_rejected'] = (df['status'] == 'Rejected').astype(int)
    
    # Provider features
    provider_stats = df.groupby('provider_id').agg({
        'is_approved': 'mean',
        'claim_id': 'count'
    }).rename(columns={
        'is_approved': 'provider_approval_rate',
        'claim_id': 'provider_total_claims'
    })
    
    df = df.merge(provider_stats, on='provider_id', how='left')
    
    providers_copy = providers_df[['provider_id', 'established_year']].copy()
    df = df.merge(providers_copy, on='provider_id', how='left')
    df['provider_experience_years'] = 2024 - df['established_year']
    df['provider_experience_years'] = df['provider_experience_years'].fillna(0)
    
    # Member features
    member_stats = df.groupby('member_id').agg({
        'claim_id': 'count',
        'is_approved': 'mean',
        'claim_amount': 'mean'
    }).rename(columns={
        'claim_id': 'member_total_claims',
        'is_approved': 'member_approval_rate',
        'claim_amount': 'member_avg_claim'
    })
    
    df = df.merge(member_stats, on='member_id', how='left')
    df['is_repeat_member'] = (df['member_total_claims'] > 1).astype(int)
    df['member_risk_score'] = 1 - df['member_approval_rate']
    df['member_risk_score'] = df['member_risk_score'].fillna(0.5)
    
    # Diagnosis features
    diagnosis_mapping = {code: idx for idx, code in enumerate(df['diagnosis_code'].unique())}
    df['diagnosis_encoded'] = df['diagnosis_code'].map(diagnosis_mapping)
    
    diagnosis_stats = df.groupby('diagnosis_code').agg({
        'is_approved': 'mean',
        'claim_amount': 'mean'
    }).rename(columns={
        'is_approved': 'diagnosis_approval_rate',
        'claim_amount': 'diagnosis_avg_amount'
    })
    
    df = df.merge(diagnosis_stats, on='diagnosis_code', how='left')
    
    # Derived features
    df['risk_score'] = (
        (1 - df['provider_approval_rate']) * 0.3 +
        df['member_risk_score'] * 0.3 +
        (1 - df['diagnosis_approval_rate']) * 0.2 +
        (df['claim_amount_normalized'] > 1).astype(int) * 0.2
    )
    
    df['potential_fraud'] = (
        (df['approval_difference'] > claim_q95) &
        (df['provider_approval_rate'] < 0.5) &
        (df['is_high_value'] == 1)
    ).astype(int)
    
    df['is_complex_claim'] = (
        (df['days_to_claim'] > 7) |
        (df['approval_ratio'] < 0.8) |
        (df['is_high_value'] == 1)
    ).astype(int)
    
    # Data cleanup
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        df[col] = df[col].fillna(df[col].mean())
    
    columns_to_drop = ['date_of_service', 'date_claimed', 'diagnosis_code', 'established_year', 'service_day_of_week', 'status']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # Save datasets
    os.makedirs('data/processed', exist_ok=True)
    
    df.to_csv('data/processed/ml_ready_claims_full.csv', index=False)
    
    fraud_cols = [col for col in [
        'claim_id', 'member_id', 'provider_id',
        'claim_amount_normalized', 'is_high_value', 'is_low_value',
        'days_to_claim', 'is_weekend', 'service_month',
        'provider_approval_rate', 'provider_experience_years',
        'member_approval_rate', 'member_total_claims', 'is_repeat_member',
        'diagnosis_approval_rate', 'approval_ratio',
        'risk_score', 'potential_fraud', 'is_complex_claim',
        'is_approved'
    ] if col in df.columns]
    
    df[fraud_cols].to_csv('data/processed/ml_fraud_detection.csv', index=False)
    
    approval_cols = [col for col in [
        'claim_id', 'member_id', 'provider_id',
        'is_high_value', 'is_low_value',
        'days_to_claim', 'is_weekend', 'service_month',
        'provider_approval_rate', 'provider_experience_years',
        'member_approval_rate', 'member_total_claims',
        'diagnosis_approval_rate',
        'risk_score', 'potential_fraud',
        'approved_amount'
    ] if col in df.columns]
    
    df[approval_cols].to_csv('data/processed/ml_approval_prediction.csv', index=False)
    
    return df

if __name__ == "__main__":
    claims_df = pd.read_csv('data/raw/claims.csv')
    providers_df = pd.read_csv('data/raw/providers.csv')
    
    ml_data = engineer_features(claims_df, providers_df)