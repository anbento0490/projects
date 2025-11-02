#!/usr/bin/env python3
"""
Mask PII Main Task
Reads PII data from DuckDB, applies masking for GDPR compliance, and stores masked data.
"""
import os, sys

current_directory = os.path.dirname(os.getcwd()) 
sys.path.append(current_directory)

from assets.auxiliary_fnc import read_from_duckdb, write_to_duckdb, mask_pii, table_exists

def mask_pii_data(db_path=None):
    """
    Main function to read PII data, mask it, and store the result.
    """
    # DEFAULT PATH IN CONTAINER: /opt/airflow/data
    db_path = '/opt/airflow/data/compliance_data_prod.duckdb'

    print(f"Starting PII MASKING task...\n")
    print(f"Database path: {db_path}")
    
    # CHECK IF SOURCE TABLE EXISTS
    if not table_exists('trx_pii_data', db_path):
        raise ValueError("Source table 'trx_pii_data' does not exist. Run extract_data_main.py first.")
    
    # READING DATA FROM DUCKDB
    print(f"Reading data from trx_pii_data table in {db_path}...")
    df_pii = read_from_duckdb('trx_pii_data', db_path)
    print(f"Read {len(df_pii)} records")
    
    # APPLYING PII MASKING
    print("Applying GDPR-compliant PII masking (SHA256 hashing)...\n")
    df_masked = mask_pii(df_pii)
    
    print("PII fields masked:")
    print("  - full_name: hashed")
    print("  - email: hashed")
    
    # WRITING MASKED DATA TO DUCKDB
    print(f"Writing masked data to trx_clear_data table in {db_path}...")
    row_count = write_to_duckdb(df_masked, 'trx_clear_data', db_path)
    
    print(f"Successfully wrote {row_count} masked records to trx_clear_data table")
    print("PII masking task completed successfully!\n\n")
    
    return df_masked

if __name__ == "__main__":
    # WHEN RUN DIRECTLY FROM CLI (NOT CALLED VIA Airflow)
    mask_pii_data()
