
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)


def main(spark):

    icb_catalog_name = 'hadoop_catalog'

    spark.sql(f"""CREATE OR REPLACE TABLE {icb_catalog_name}.TEST_SCHEMA.TEST_TABLE_EMR_S3_HDP (
             FIELD_1 BIGINT,
             FIELD_2 varchar(50),
             FIELD_3 DATE,
             FIELD_4 DOUBLE,
             FIELD_5 TIMESTAMP
             )
             USING iceberg
             """)
    
    spark.sql(f'SHOW TABLES IN {icb_catalog_name}.TEST_SCHEMA').show(truncate=False)
  
if __name__ == "__main__":
    #####
    logging.info('Creating SPARK SESSION...\n')

    spark = SparkSession.builder.appName('perform_cold_storage').getOrCreate()

    logging.info('SPARK SESSION created!\n')

    main(spark)

    logging.info('Main APPLICATION was executed!\n')
