import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def process_data_and_write_to_s3(s3_input, s3_output):

    print(f'Reading data from {s3_input} directory and creating Temp view')
    spark.read.parquet(s3_input).createOrReplaceTempView("balances")

    print('Creating DF with only balances for Company_A')
    df_filtered = spark.sql("select * from balances where COMPANY_CODE = 'Company_A'")

    print(f'Writing DF to {s3_output} directory...')
    df_filtered.write.mode("overwrite").parquet(s3_output)

    print('Data written to target folder...')

    count = df_filtered.count()

    print(f'DF rows count is {count}')

    print('PROCESS COMPLETED!')

if __name__ == "__main__":

    print('Start process...')

    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_input", type=str, help="Input path for reading DataFrame")
    parser.add_argument("--s3_output", type=str, help="Output path for writing DataFrame")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Write Spark DF To Parquet Via EMR").getOrCreate()
    process_data_and_write_to_s3(s3_input = args.s3_input, s3_output = args.s3_output)