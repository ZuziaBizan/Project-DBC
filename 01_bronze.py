from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# W Databricks pobierze istniejącą sesję, a w IDE usunie podkreślenia błędów składni
spark = SparkSession.builder.appName("CERT_Tranco_Bronze").getOrCreate()

#Set base folder path and target database schema
BASE_PATH = "/Volumes/workspace/default/raw_data"
TARGET_SCHEMA = "workspace.default"

# 1. Read 2 Tranco csv files (no headers)
df_tranco_bronze = (
    spark.read
    .option("header","false")
    .schema("rank INT, domain STRING")
    .csv(f"{BASE_PATH}/tranco/*.csv")
    # Add load time and source for tracking
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file",F.col("_metadata.file_path"))
)