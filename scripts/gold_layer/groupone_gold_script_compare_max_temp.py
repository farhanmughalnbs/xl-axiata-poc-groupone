from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, max, date_format
import sys

if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("xl-axiata-poc-gold-job") \
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

    print(silver_2020.show(), "Silver 2020 data frame")
    print(silver_2021.show(), "Silver 2021 data frame")
    print(silver_2022.show(), "Silver 2022 data frame")

   
    # Union the data from all three years
    combined_data = silver_2020.union(silver_2021).union(silver_2022)

    print(combined_data.show(), "Combined data frame")


    combined_data = combined_data \
        .withColumn("month", month("date_column")) \
        .withColumn("year", year("date_column")) \
        .withColumn("month_name", date_format("date_column", "MMMM"))

    # Calculate the maximum recorded temperature for each month
    max_temperature_per_month = combined_data.groupBy("year", "month_name") \
        .agg(max("TEMP").alias("max_temperature"))

    # Show the results
    max_temperature_per_month.show()
    max_temperature_per_month.printSchema()

    max_temperature_per_month.write.parquet(output_s3_path, mode="overwrite")

    # Stop the Spark session
    spark.stop()
