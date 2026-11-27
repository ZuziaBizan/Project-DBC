from pyspark.sql.functions import col, lower, trim, split, element_at, when, lit

# ==================================================
# 3. Gold Layer
# ==================================================

# 1. Load Silver tables and the brands dictionary CSV
df_cert = spark.table("workspace.default.cert_active_silver")
df_tlds = spark.table("workspace.default.tlds_silver")
df_tranco = spark.table("workspace.default.tranco_silver")

# File path
df_brands = (
    spark.read.option("header", True)
    .csv("/Volumes/workspace/default/raw_data/brands.csv")
    .withColumn("brand_clean", lower(trim(col("brand"))))
    .withColumn("official_domain_clean", lower(trim(col("official_domain"))))
)

# 2. Extract the domain TLD (e.g., "shop", "pl", "cyou") and check if it exists in IANA TLDs
df_cert_tld = (
    df_cert
    .withColumn("tld", element_at(split(col("domain_clean"), "\\."), -1))
    .join(
        df_tlds.select(lower(trim(col("tld"))).alias("tld_iana")).dropDuplicates(),
        col("tld") == col("tld_iana"),
        how="left",
    )
    .withColumn("is_valid_tld", col("tld_iana").isNotNull())
)

# 3. Match brand names against domain_clean
# crossJoin pairs every domain with every brand, and filter keeps only matches
df_matches = (
    df_cert_tld.alias("c")
    .crossJoin(df_brands.alias("b"))
    .filter(col("c.domain_clean").contains(col("b.brand_clean")))
    .withColumn("is_official", col("c.domain_clean") == col("b.official_domain_clean"))
    .withColumn("is_suspicious", (~col("is_official")) & col("c.is_active"))
)

# 4. Join Tranco popularity rank (if the domain appears in Tranco)
df_tranco_small = (
    df_tranco
    .select(
        lower(trim(col("domain"))).alias("tranco_domain"),
        col("rank").alias("tranco_rank"),
    )
    .dropDuplicates(["tranco_domain"])
)

df_gold = (
    df_matches
    .join(
        df_tranco_small,
        col("c.domain_clean") == col("tranco_domain"),
        how="left",
    )
    .select(
        col("b.id").alias("brand_id"),
        col("b.brand_clean").alias("brand"),
        col("b.category"),
        col("c.DomainAddress"),
        col("c.domain_clean"),
        col("c.tld"),
        col("c.is_valid_tld"),
        col("c.is_active"),
        col("is_official"),
        col("is_suspicious"),
        col("tranco_rank"),
    )
)

# 5. Save the results in Gold Delta table
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.brand_impersonation_gold")
)