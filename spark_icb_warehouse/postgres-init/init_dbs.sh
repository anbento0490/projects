#!/bin/bash
set -e

# Create the additional databases
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE "iceberg_warehouse_pg";
    GRANT ALL PRIVILEGES ON DATABASE "iceberg_warehouse_pg" TO postgres;
EOSQL

# List all databases to confirm creation.
echo "Listing all databases:"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -c '\l'