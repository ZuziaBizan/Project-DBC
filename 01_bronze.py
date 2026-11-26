from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import data from tranco csv
df_tranco = spark.read.csv(
    "/Volumes/workspace/default/raw_data/tranco/*.csv",
    header=False,
    schema="rank INT, domain STRING")

# Adding columns
df_tranco_bronze= df_tranco.withColumn(
    "_ingested_at",F.current_timestamp()
    ).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )

df_tranco_bronze.write.mode("overwrite").saveAsTable("workspace.default.tranco_bronze")
# Import data from CERT domains.json
df_cert = spark.read.option("multiLine", True).json(
    "/Volumes/workspace/default/raw_data/domains.json"
)

df_cert_bronze = df_cert.withColumn(
    "_ingested_at", F.current_timestamp()
).withColumn(
    "_source_file", F.col("_metadata.file_path")
)

df_cert_bronze.write.mode("overwrite").saveAsTable("workspace.default.cert_active_bronze")


# Import data from IANA TLDs txt
df_tlds = spark.read.text(
    "/Volumes/workspace/default/raw_data/tlds-alpha-by-domain.txt"
)

df_tlds_bronze = df_tlds.withColumn(
    "_ingested_at", F.current_timestamp()
).withColumn(
    "_source_file", F.col("_metadata.file_path")
)

df_tlds_bronze.write.mode("overwrite").saveAsTable("workspace.default.tlds_bronze")