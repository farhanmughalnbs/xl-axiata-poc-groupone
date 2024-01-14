from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, max, date_format
import sys

if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("xl-axiata-poc-gold-job2") \
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()

    # Access command-line arguments
    arguments = sys.argv[1:]

    # Parse command-line arguments
    output_s3_path = arguments[arguments.index("--target_bucket") + 1] if "--target_bucket" in arguments else None

    silver_2020 = spark.sql("select * from xlaxiata_poc_groupone.silver_2020")
    silver_2021 = spark.sql("select * from xlaxiata_poc_groupone.silver_2021")
    silver_2022 = spark.sql("select * from xlaxiata_poc_groupone.silver_2022")

    # Union the data from all three years
    combined_data = silver_2020.union(silver_2021).union(silver_2022)

    # Extract month and year from the date column
    combined_data = combined_data \
        .withColumn("month", month("date_column")) \
        .withColumn("year", year("date_column")) \
        .withColumn("month_name", date_format("date_column", "MMMM"))

    # Calculate the maximum recorded temperature for each month and year
    max_temperature_trends = combined_data.groupBy("month_name", "year") \
        .agg(max("TEMP").alias("max_temperature"))

    # Pivot the data to have years as columns
    max_temperature_trends_pivoted = max_temperature_trends.groupBy("month_name") \
        .pivot("year").agg(max("max_temperature").alias("max_temperature"))

    # Show the results
    max_temperature_trends_pivoted.show()
    print("Max temperature trends schema")
    max_temperature_trends_pivoted.printSchema()


    max_temperature_trends_pivoted.write.parquet(output_s3_path, mode="overwrite")

    # Stop the Spark session
    spark.stop()
