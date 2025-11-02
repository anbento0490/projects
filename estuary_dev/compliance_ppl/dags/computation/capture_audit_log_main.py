#!/usr/bin/env python3
"""
Capture Audit Log Main Task
Reads masked data from DuckDB, captures audit metadata for SOX compliance, and stores audit log.
"""

import os, sys

current_directory = os.path.dirname(os.getcwd()) 
sys.path.append(current_directory)

from assets.auxiliary_fnc import read_from_duckdb, write_to_duckdb, capture_audit_log, table_exists

def capture_audit_log_data(db_path=None, operation_type='mask_pii', user_id='af_service_user'):
    """
    Main function to read masked data, capture audit log, and store it.
    """
    # DEFAULT PATH IN CONTAINER: /opt/airflow/data
    db_path = '/opt/airflow/data/compliance_data_prod.duckdb'

    print(f"Starting AUDIT LOG CAPTURE task...")
    print(f"Database path: {db_path}")
    
    # CHECK IF SOURCE TABLE EXISTS
    if not table_exists('trx_clear_data', db_path):
        raise ValueError("Source table 'trx_clear_data' does not exist. Run mask_pii_main.py first.")
    
    # READING MASKED DATA FROM DUCKDB
    print(f"Reading data from trx_clear_data table in {db_path}...")
    df_masked = read_from_duckdb('trx_clear_data', db_path)
    print(f"Read {len(df_masked)} records")
    
    # CAPTURING AUDIT LOG
    print(f"Capturing SOX-compliant audit log for operation: {operation_type}...")
    df_audit = capture_audit_log(
        df_masked, 
        operation_type=operation_type, 
        user_id=user_id,
        include_checksums=True
    )
    
    print(f"Generated {len(df_audit)} audit log entries\n")
    print("Audit log includes:")
    print("  - Unique audit IDs")
    print("  - Batch tracking")
    print("  - Timestamps (ISO format + Unix)")
    print("  - Row-level checksums for integrity verification")
    print("  - Column access metadata")
    
    # WRITING AUDIT LOG TO DUCKDB
    print(f"Writing audit log to trx_audit_log table in {db_path}...")
    row_count = write_to_duckdb(df_audit, 'trx_audit_log', db_path)
    
    print(f"Successfully wrote {row_count} audit records to trx_audit_log table")
    print("Audit log capture task completed successfully!\n\n")
    
    return df_audit


if __name__ == "__main__":
    # WHEN RUN DIRECTLY FROM CLI (NOT CALLED VIA Airflow)
    capture_audit_log_data()
