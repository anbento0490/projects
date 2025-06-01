#!/bin/bash
set -e  

echo "################################################################################"
echo "Waiting for PostgreSQL to start..."
until psql -U "$POSTGRES_USER" -d postgres -c "SELECT 1;" &> /dev/null; do
  echo "PostgreSQL is not ready yet. Retrying in 5 seconds..."
  sleep 5
done
echo "PostgreSQL is ready!"

# CREATE ANALYTICS_DB
echo "Ensuring securities_master database exists..."
psql -U "$POSTGRES_USER" -tc "SELECT 1 FROM pg_database WHERE datname = 'securities_master'" | grep -q 1 || psql -U "$POSTGRES_USER" -c "CREATE DATABASE securities_master;"

echo "Database securities_master is ready!"

# SWITCH TO ANALYTICS_DB & CREATE SCHEMA + TABLE
echo "Switching to securities_master and setting up nasdaq_100 schema and intraday_historical table..."
psql -U "$POSTGRES_USER" -d securities_master <<EOSQL
  -- Ensure we are inside securities_master
  SELECT current_database();

  -- Create the trades schema inside securities_master
  CREATE SCHEMA IF NOT EXISTS nasdaq_100;

  -- Verify that the schema is inside securities_master
  SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'nasdaq_100';

  -- Create the table nasdaq_100 trades schema
  CREATE TABLE nasdaq_100.intraday_historical (
        ticker VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        datetime TIMESTAMP NOT NULL,
        open NUMERIC(10, 4) NOT NULL,
        high NUMERIC(10, 4) NOT NULL,
        low NUMERIC(10, 4) NOT NULL,
        close NUMERIC(10, 4) NOT NULL,
        volume INTEGER NOT NULL,
        inserted_at TIMESTAMP NOT NULL
    );

  -- Verify table creation
  SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'nasdaq_100' AND table_name = 'intraday_historical';
EOSQL

echo "Database initialization complete!"
echo "################################################################################"
