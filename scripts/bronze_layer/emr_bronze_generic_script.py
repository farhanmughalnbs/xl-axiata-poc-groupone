from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col
from datetime import datetime
import sys


if __name__ == "__main__":

    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("xl-axiata-poc-bronze-job") \
        .getOrCreate()
        
    # Access command-line arguments
    arguments = sys.argv[1:]

    # Parse command-line arguments
    source_bucket = arguments[arguments.index("--source_bucket") + 1] if "--source_bucket" in arguments else None
    target_bucket = arguments[arguments.index("--target_bucket") + 1] if "--target_bucket" in arguments else None

    # Read CSV files from the public S3 bucket
    df = spark.read.csv(source_bucket, header=True, inferSchema=True)

    # Convert the "DATE" column to a date type
    df = df.withColumn("date_column", col("DATE").cast("date"))

    # Extract year, month, and day from the date_column
    df = df.withColumn("year", date_format(col("date_column"), "yyyy"))
    df = df.withColumn("month", date_format(col("date_column"), "MM"))
    df = df.withColumn("day", date_format(col("date_column"), "dd"))

    df = df.repartition("year", "month", "day")
    # Write the DataFrame to the new S3 bucket in Parquet format with partitioning
    df.write.partitionBy("year", "month", "day").parquet(target_bucket, mode="append")

    # Stop the Spark session
    spark.stop()
