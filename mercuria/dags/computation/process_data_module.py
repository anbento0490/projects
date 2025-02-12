import psycopg2
from sqlalchemy import create_engine
import polars as pl
from datetime import datetime
import numpy as np

import logging
logging.basicConfig(level=logging.INFO)

class DataProcessing:
    def __init__(self, uri):
        self._uri = uri

    def read_from_db(self, src_tbl):
        """Reads data from a DB and outputs a Polars DF"""

        logging.info(f"Reading data source from DB as a Polars DF...")
        conn = create_engine(self._uri)
        query = f"select * from {src_tbl}"
        df = pl.read_database(query=query, connection=conn.connect())

        logging.info(f"Data successfully read from DB! DF has shape {df.shape}")
        return df

    def write_to_db(self, df, tgt_tbl):
        """Writes a Polars DF to a target DB table"""

        logging.info(f"Writing DF to DB target table...")
        df.write_database(table_name=tgt_tbl,
                          if_table_exists= "replace",
                          connection=self._uri) 
        logging.info(f"Data successfully written to PostgreDB {tgt_tbl} table.")

        return True

    def compute_var(self, sorted_pnl, confidence_level):
        """Computes VaR At a given confidence level."""

        index = int(np.floor((1 - confidence_level) * len(sorted_pnl)))
        return sorted_pnl[index] if index < len(sorted_pnl) else None

    def process_bronze_layer(self):
        """Processes datasets in the bronze layer, performing a raw data 
        ingestion from a source to a dedicated target table in a PG database
        """
        logging.info("Processing Bronze Layer...")
        df_bronze = pl.read_csv("data_engineer_sample_data.csv")

        # 1. Reading From CSV
        logging.info(f"Reading data source as a Polars DF...")
        df_bronze = df_bronze.rename({
                    "TradeID": "trade_id",
                    "PortfolioId": "portfolio_id",
                    "businessDate": "business_date",
                    "PnL": "pnl"
                }).with_columns([
                    pl.col("trade_id").cast(pl.Utf8),  # String type
                    pl.col("portfolio_id").cast(pl.Int64),  # BigInt
                    pl.col("business_date").cast(pl.Utf8),  # Text
                    pl.col("pnl").cast(pl.Decimal(35, 2))  # Decimal(35,5)
                ])

        logging.info(f"Data successfully parsed! DF has shape {df_bronze.shape}")
    
        # 2. Writing To PG tables named trades.bronze_layer
        tgt_tbl = "trades.bronze_layer"
        DataProcessing.write_to_db(self, df_bronze, tgt_tbl)

    def process_silver_layer(self):
        """Processes datasets in the silver layer, by reading from the 
        bronze layer target table and applying transformation/cleaning on
        specific fields.
        """
        src_tbl = "trades.bronze_layer"
        tgt_tbl = "trades.silver_layer"

        logging.info("Processing Silver Layer...")
        
        df_silver = DataProcessing.read_from_db(self, src_tbl)

        logging.info(f"Performing transformations...")
        # 1. Converting date to standard format (timestamp)
        df_silver = df_silver.with_columns(pl.col("business_date").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M",strict=False)
            .fill_null(pl.col("business_date").str.strptime(pl.Datetime, format="%m/%d/%Y",strict=False))
            .fill_null(pl.col("business_date").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S",strict=False))
            .fill_null(pl.col("business_date").str.strptime(pl.Datetime, format="%d-%m-%Y",strict=False))
            .fill_null(pl.col("business_date").str.strptime(pl.Datetime, format="%d-%m-%Y %H:%M:%S",strict=False))
            .alias("business_ts"))
        
        # 2. Converting timestamp into date 
        df_silver = df_silver.with_columns(pl.col("business_ts").dt.date().alias("business_date"))
        
        # 3. Adding processed_at field
        df_silver = df_silver.with_columns(
            pl.lit(datetime.now()).alias("processed_at")
        )

        # 4. Converting Date-time fields back to strings to avoid format issues while writing to PG
        df_silver = df_silver.with_columns([pl.col("business_ts").dt.strftime("%Y-%m-%d %H:%M:%S").alias("business_ts"),
                                            pl.col("business_date").dt.strftime("%Y-%m-%d").alias("business_date"),
                                            pl.col("processed_at").dt.strftime("%Y-%m-%d %H:%M:%S").alias("processed_at"),
                                            pl.col("pnl").cast(pl.Float64)])
        
        # 5. Re-ordering columns
        df_silver = df_silver.select([
                            "trade_id",
                            "portfolio_id",
                            "business_ts",
                            "business_date",
                            "pnl",
                            "processed_at"])

        # 6. Writing To PG tables named trades.silver_layer
        DataProcessing.write_to_db(self, df_silver, tgt_tbl)

    def process_gold_layer(self):
        """Processes datasets in the gold layer, by calculating VaR
        using clean data previously prepared as part of silver layer.
        """
        src_tbl = "trades.silver_layer"
        tgt_tbl = "trades.gold_layer"

        logging.info("Processing Gold Layer...")
        
        df = DataProcessing.read_from_db(self, src_tbl)

        print(df.head)

        logging.info(f"Performing VaR Calculations...")
        # Step 1: Compute VaR Across All Trades
        df_agg = (df.group_by("business_date").agg(pl.sum("pnl").alias("total_pnl")).sort("business_date"))
        sorted_pnl = df_agg.sort("total_pnl")["total_pnl"].to_numpy()

        var_95_all = DataProcessing.compute_var(self, sorted_pnl, 0.95)
        logging.info(f"VaR across ALL TRADES at 95% Confidence Level was found to be {var_95_all}.")

        # Step 2: Compute VaR per trade
        df_trade_agg = df.group_by("trade_id").agg(pl.sum("pnl").alias("trade_total_pnl"))
        sorted_trade_pnl = df_trade_agg.sort("trade_total_pnl")["trade_total_pnl"].to_numpy()

        var_95_trade = DataProcessing.compute_var(self, sorted_trade_pnl, 0.95)
        var_975_trade = DataProcessing.compute_var(self, sorted_trade_pnl, 0.975)
        logging.info(f"VaR p/trade at 95% Confidence Level was found to be {var_95_trade}.")
        logging.info(f"VaR p/trade at 97.5% Confidence Level was found to be {var_975_trade}.")

        # Step 3: Compute VaR per book (portfolio_id)
        df_book_agg = df.group_by("portfolio_id").agg(pl.sum("pnl").alias("book_total_pnl"))
        sorted_book_pnl = df_book_agg.sort("book_total_pnl")["book_total_pnl"].to_numpy()

        var_99_book = DataProcessing.compute_var(self, sorted_book_pnl, 0.99)
        logging.info(f"VaR p/book (portfolio_id) at 99% Confidence Level was found to be {var_99_book}.")

        # Step 4: Create a df_gold to store results
        df_gold = pl.DataFrame({
            "VaR Type": [
                "VaR 95% per trade",
                "VaR 97.5% per trade",
                "VaR 95% across all trades",
                "VaR 99% per book"
            ],
            "Value": [
                var_95_trade,
                var_975_trade,
                var_95_all,
                var_99_book
            ]})
        
        # Step 5: Writing To PG tables named trades.gold_layer
        DataProcessing.write_to_db(self, df_gold, tgt_tbl)
