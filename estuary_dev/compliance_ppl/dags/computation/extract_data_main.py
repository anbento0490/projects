#!/usr/bin/env python3
"""
Extract Data Main Task
Generates mock PII data and stores it in DuckDB for compliance processing.
"""
import os, sys

current_directory = os.path.dirname(os.getcwd()) 
sys.path.append(current_directory)

from assets.auxiliary_fnc import generate_sample_data, write_to_duckdb

def extract_data(num_records = 1000, db_path=None):
    """
    Main function to extract (generate) sample data and store in DuckDB.
    """
    # DEFAULT PATH IN CONTAINER: /opt/airflow/data
    db_path = '/opt/airflow/data/compliance_data_prod.duckdb'

    print(f"Starting DATA EXTRACTION task...\n")
    print(f"Generating {num_records} sample records with PII data...")
    print(f"Database path: {db_path}")
    
    # GENERATING MOCK DATA
    df = generate_sample_data(num_records)
    
    print(f"Generated {len(df)} records")
    print(f"Columns: {', '.join(df.columns)}")
    
    # WRITING TO DUCKDB
    print(f"Writing data to trx_pii_data table in {db_path}...")
    row_count = write_to_duckdb(df, 'trx_pii_data', db_path)
    
    print(f"Successfully wrote {row_count} records to trx_pii_data table")
    print("Data extraction task completed successfully!")
    
    return df

if __name__ == "__main__":
    # WHEN RUN DIRECTLY FROM CLI (NOT CALLED VIA Airflow)
    extract_data(num_records=1000)
