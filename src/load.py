"""
Load module
Creates database schema and loads data into PostgreSQL
"""

import pandas as pd
from database import DatabaseConnection

def create_schema(db_conn):
    """Create database tables"""
    try:
        cursor = db_conn.execute_query("""
            CREATE TABLE IF NOT EXISTS providers (
                provider_id VARCHAR(20) PRIMARY KEY,
                provider_name VARCHAR(255) NOT NULL,
                location VARCHAR(100),
                provider_type VARCHAR(50),
                established_year INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        db_conn.conn.commit()
        
        cursor = db_conn.execute_query("""
            CREATE TABLE IF NOT EXISTS insurers (
                insurer_id VARCHAR(20) PRIMARY KEY,
                insurer_name VARCHAR(255) NOT NULL,
                headquarters VARCHAR(100),
                year_established INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        db_conn.conn.commit()
        
        cursor = db_conn.execute_query("""
            CREATE TABLE IF NOT EXISTS claims (
                claim_id VARCHAR(20) PRIMARY KEY,
                member_id VARCHAR(20) NOT NULL,
                provider_id VARCHAR(20),
                claim_amount DECIMAL(12, 2),
                date_of_service DATE,
                date_claimed DATE,
                diagnosis_code VARCHAR(20),
                approved_amount DECIMAL(12, 2),
                status VARCHAR(20),
                insurance_plan_id VARCHAR(20),
                insurer_id VARCHAR(20),
                location VARCHAR(100),
                processing_days INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
                FOREIGN KEY (insurer_id) REFERENCES insurers(insurer_id)
            );
        """)
        db_conn.conn.commit()
        
    except Exception as e:
        raise Exception(f"Error creating schema: {e}")

def load_providers(db_conn, providers_df):
    """Load providers data into database"""
    try:
        for idx, row in providers_df.iterrows():
            cursor = db_conn.execute_query("""
                INSERT INTO providers (provider_id, provider_name, location, provider_type, established_year)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (provider_id) DO UPDATE SET
                    provider_name = EXCLUDED.provider_name,
                    location = EXCLUDED.location,
                    provider_type = EXCLUDED.provider_type,
                    established_year = EXCLUDED.established_year
            """, (
                row['provider_id'],
                row['provider_name'],
                row['location'],
                row['provider_type'],
                int(row['established_year']) if pd.notna(row['established_year']) else None
            ))
        
        db_conn.conn.commit()
    except Exception as e:
        db_conn.conn.rollback()
        raise Exception(f"Error loading providers: {e}")

def load_insurers(db_conn, insurers_df):
    """Load insurers data into database"""
    try:
        for idx, row in insurers_df.iterrows():
            cursor = db_conn.execute_query("""
                INSERT INTO insurers (insurer_id, insurer_name, headquarters, year_established)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (insurer_id) DO UPDATE SET
                    insurer_name = EXCLUDED.insurer_name,
                    headquarters = EXCLUDED.headquarters,
                    year_established = EXCLUDED.year_established
            """, (
                row['insurer_id'],
                row['insurer_name'],
                row['headquarters'],
                int(row['year_established']) if pd.notna(row['year_established']) else None
            ))
        
        db_conn.conn.commit()
    except Exception as e:
        db_conn.conn.rollback()
        raise Exception(f"Error loading insurers: {e}")

def load_claims(db_conn, claims_df):
    """Load claims data into database"""
    try:
        batch_size = 500
        
        for start_idx in range(0, len(claims_df), batch_size):
            end_idx = min(start_idx + batch_size, len(claims_df))
            batch = claims_df.iloc[start_idx:end_idx]
            
            for idx, row in batch.iterrows():
                cursor = db_conn.execute_query("""
                    INSERT INTO claims (
                        claim_id, member_id, provider_id, claim_amount, 
                        date_of_service, date_claimed, diagnosis_code, 
                        approved_amount, status, insurance_plan_id, 
                        insurer_id, location, processing_days
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (claim_id) DO NOTHING
                """, (
                    row['claim_id'],
                    row['member_id'],
                    row['provider_id'],
                    float(row['claim_amount']) if pd.notna(row['claim_amount']) else None,
                    row['date_of_service'],
                    row['date_claimed'],
                    row['diagnosis_code'],
                    float(row['approved_amount']) if pd.notna(row['approved_amount']) else None,
                    row['status'],
                    row['insurance_plan_id'],
                    row['insurer_id'],
                    row['location'],
                    int(row['processing_days']) if pd.notna(row['processing_days']) else None
                ))
            
            db_conn.conn.commit()
        
    except Exception as e:
        db_conn.conn.rollback()
        raise Exception(f"Error loading claims: {e}")

def load_all_data(db_conn, claims_df, providers_df, insurers_df):
    """Load all data into database"""
    create_schema(db_conn)
    load_providers(db_conn, providers_df)
    load_insurers(db_conn, insurers_df)
    load_claims(db_conn, claims_df)