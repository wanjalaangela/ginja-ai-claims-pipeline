"""
ETL Pipeline Orchestrator
Coordinates extraction, transformation, and loading
"""

from extract import load_all_data as extract_data
from transform import transform_all_data
from load import load_all_data as load_data
from database import DatabaseConnection

def run_etl_pipeline():
    """Run complete ETL pipeline"""
    
    try:
        # Extract data from CSV files
        claims_df, providers_df, insurers_df = extract_data()
        
        # Transform and clean data
        claims_df, providers_df, insurers_df = transform_all_data(
            claims_df, providers_df, insurers_df
        )
        
        # Setup database connection
        db_conn = DatabaseConnection()
        db_conn.create_database_if_not_exists()
        db_conn.connect()
        
        # Load data into database
        load_data(db_conn, claims_df, providers_df, insurers_df)
        
        # Close connection
        db_conn.close()
        
        return True
        
    except Exception as e:
        raise Exception(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    try:
        success = run_etl_pipeline()
        exit(0 if success else 1)
    except Exception as e:
        exit(1)