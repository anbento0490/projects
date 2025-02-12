#!/bin/bash
set -e  

echo "################################################################################"
echo "Waiting for PostgreSQL to start..."
until psql -U "$POSTGRES_USER" -d postgres -c "SELECT 1;" &> /dev/null; do
  echo "PostgreSQL is not ready yet. Retrying in 2 seconds..."
  sleep 2
done

echo "PostgreSQL is ready!"

# CREATE ANALYTICS_DB
echo "Ensuring analytics_db database exists..."
psql -U "$POSTGRES_USER" -tc "SELECT 1 FROM pg_database WHERE datname = 'analytics_db'" | grep -q 1 || psql -U "$POSTGRES_USER" -c "CREATE DATABASE analytics_db;"

# SWITCH TO ANALYTICS_DB & CREATE SCHEMA + TABLE
echo "Switching to analytics_db and setting up trades schema and bronze_layer table..."
psql -U "$POSTGRES_USER" -d analytics_db <<EOSQL
  -- Ensure we are inside analytics_db
  SELECT current_database();

  -- Create the trades schema inside analytics_db
  CREATE SCHEMA IF NOT EXISTS trades;

  -- Verify that the schema is inside analytics_db
  SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'trades';

  -- Create the table inside trades schema
  CREATE TABLE IF NOT EXISTS trades.bronze_layer (
      TRADE_ID VARCHAR,
      PORTFOLIO_ID INT,
      BUSINESS_DATE VARCHAR,
      PNL DECIMAL(35,5)
  );

  -- Verify table creation
  SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'trades' AND table_name = 'bronze_layer';
EOSQL

echo "Database initialization complete!"
echo "################################################################################"
