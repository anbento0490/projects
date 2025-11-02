import random, hashlib, datetime, duckdb, pandas as pd
from faker import Faker

### MAIN FUNCTIONS 
def generate_sample_data(num_records=1000):
    """Generate sample dataframe with realistic PII data using Faker package"""
    fake = Faker()
    
    # NAMES ARE GENERATED FIRST, THEN EMAILS ARE BASED ON NAMES
    names = [fake.name() for _ in range(num_records)]
    emails = [f"{name.lower().replace(' ', '.')}@{fake.free_email_domain()}" for name in names]
    
    data = {
        'id': [fake.unique.random_int(min=10000, max=99999) for _ in range(num_records)],
        'full_name': names,
        'email': emails,
        'signup_date': pd.date_range(start='2025-01-01', periods=num_records, freq='D'),
        'transaction_amount': [round(random.uniform(5, 500), 2) for _ in range(num_records)]
    }
    
    return pd.DataFrame(data)


def mask_pii(df):
    """Mask PII data using SHA256 hashing for GDPR compliance"""
    
    def hash_val(val):
        return hashlib.sha256(val.encode('utf-8')).hexdigest()
    
    df_masked = df.copy()
    df_masked['full_name'] = df_masked['full_name'].apply(hash_val)
    df_masked['email'] = df_masked['email'].apply(hash_val)
    return df_masked


def capture_audit_log(df, operation_type='update', user_id=None, include_checksums=True):
    """
    Capture comprehensive audit log with metadata for SOX compliance.
    operation_type can get following values (e.g., 'create', 'update', 'delete', 'read')
    """
    now = datetime.datetime.now(datetime.UTC)
    batch_id = hashlib.sha256(f"{now.isoformat()}{len(df)}".encode()).hexdigest()[:16]
    
    audit_data = {
        'audit_id': [f"AUD-{batch_id}-{i:06d}" for i in range(len(df))],
        'record_id': df['id'].values,
        'operation_time': now.isoformat(),
        'operation_type': operation_type,
        'user_id': user_id or 'system',
        'batch_id': batch_id,
        'record_count': len(df),
        'timestamp_utc': now.timestamp()
    }
    # ADD ROW-LEVEL CHECKSUMS FOR ADDITIONAL DATA INTEGRITY VERIFICATION
    if include_checksums:
        checksums = df.apply(
            lambda row: hashlib.sha256(
                ''.join(str(v) for v in row.values).encode()
            ).hexdigest()[:16], 
            axis=1
        )
        audit_data['row_checksum'] = checksums.values
    
    # ADDING COLUMN-LEVEL METADATA
    audit_data['columns_accessed'] = ','.join(df.columns)
    
    audit_df = pd.DataFrame(audit_data)
    return audit_df


### DUCKDB AUXILIARY FUNCTIONS
def get_duckdb_connection(db_path):
    """Create and return a DuckDB connection"""
    return duckdb.connect(db_path)


def write_to_duckdb(df, table_name, db_path):
    """Write a DataFrame to DuckDB table"""
    conn = get_duckdb_connection(db_path)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    conn.close()
    return row_count


def read_from_duckdb(table_name, db_path):
    """Read a table from DuckDB into a DataFrame"""
    conn = get_duckdb_connection(db_path)
    df = conn.execute(f"SELECT * FROM {table_name}").df()
    conn.close()
    return df


def table_exists(table_name, db_path='compliance_data.duckdb'):
    """Check if a table exists in DuckDB"""
    conn = get_duckdb_connection(db_path)
    result = conn.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    ).fetchone()[0]
    conn.close()
    return result > 0
