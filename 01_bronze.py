from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import data from tranco csv
df_tranco = spark.read.csv(
    "/Volumes/workspace/default/raw_data/tranco/*.csv",
    header=False,
    schema="rank INT, domain STRING")

#adding columns
df_tranco_bronze= df_tranco.withColumn(
    "_ingested_at",F.current_timestamp()
    ).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )

df_tranco_bronze.write.mode("overwrite").saveAsTable("workspace.default.tranco_bronze")