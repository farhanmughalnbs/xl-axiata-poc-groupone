from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Row
import os

os.environ['SPARK_VERSION'] = "3.2"

import pydeequ
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.suggestions import *


spark = SparkSession.builder \
    .appName("xl-axiata-poc-silver-job") \
    .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
    .getOrCreate()

spark_version = spark.sparkContext.version
print(f"Running Spark Version: {str(spark_version)}")
print(f"Running Spark Version: {str(os.environ['SPARK_VERSION'])}")

# Access command-line arguments
arguments = sys.argv[1:]

# Parse command-line arguments
input_s3_path = arguments[arguments.index("--source_bucket") + 1] if "--source_bucket" in arguments else None
output_s3_path = arguments[arguments.index("--target_bucket") + 1] if "--target_bucket" in arguments else None
table_name = arguments[arguments.index("--table_name") + 1] if "--table_name" in arguments else None

df = spark.read.parquet(input_s3_path)

df = df.withColumn("id", monotonically_increasing_id())

select_columns = ["id", "TEMP", "WDSP", "GUST", "MAX", "MIN", "NAME", "date_column", "year", "month", "day"]
df = df.select(*select_columns)

df = df.repartition("year", "month", "day")

############### DataQuality ########################

# Running Data Analyzer #
print("Performing Analysis on Dataframe")
analysisResult = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("id")) \
                    .addAnalyzer(Completeness("TEMP")) \
                    .addAnalyzer(Completeness("WDSP")) \
                    .addAnalyzer(Completeness("GUST")) \
                    .addAnalyzer(Completeness("MAX")) \
                    .addAnalyzer(Completeness("MIN")) \
                    .addAnalyzer(Completeness("NAME")) \
                    .addAnalyzer(Completeness("date_column")) \
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
print("Printing Analysis Results")
analysisResult_df.show(100)


# Profiling read data #
print("Performing Profiling on Dataframe")
result = ColumnProfilerRunner(spark) \
    .onData(df) \
    .run()

print("Printing Profiling Results")
for col, profile in result.profiles.items():
    print(profile)


# Running Suggestion runner #
print("Performing Suggestions for Data")
suggestionResult = ConstraintSuggestionRunner(spark) \
             .onData(df) \
             .addConstraintRule(DEFAULT()) \
             .run()

# Constraint Suggestions in JSON format
print("Printing Suggestion Results")
print(suggestionResult)
########################################################

hudiOptions = {
  'hoodie.table.name': 'silver_2022',
  'hoodie.datasource.write.recordkey.field': 'id',
  'hoodie.datasource.write.partitionpath.field': 'year,month,day',
  'hoodie.datasource.write.precombine.field': 'year,month,day',
}

print("Resultant Dataframe Schema")
df.printSchema()

df.show(10)

print("Writing Silver Data")
df.write.partitionBy("year", "month", "day").format('org.apache.hudi').option('hoodie.datasource.write.operation', 'insert').options(**hudiOptions).mode('append').save(output_s3_path)
print("Silver Data write completed")

spark.stop()
