from pyspark.sql.functions import *

# ==========================================
# 1. TLDs Silver (IANA list)
# ==========================================

# Read raw TLDs table from Bronze
df_tlds = spark.table('workspace.default.tlds_bronze')

# Remove null, empty, and comment lines starting with '#'
df_tlds = df_tlds.filter(
    (col('value').isNotNull()) &
    (trim(col('value')) != '') &
    (~trim(col('value')).startswith('#'))
)

# Normalize TLD to lowercase without extra spaces and drop duplicates
df_tlds = df_tlds.withColumn('tld', lower(trim(col('value'))))\
                 .select('tld', '_ingested_at', '_source_file')\
                 .dropDuplicates(['tld'])

# Save cleaned TLDs table to Silver
df_tlds.write.format('delta')\
       .mode('overwrite')\
       .saveAsTable('workspace.default.tlds_silver')


# ==========================================
# 2. Tranco Silver (Domain popularity)
# ==========================================

# Read raw Tranco table from Bronze
df_tranco = spark.table('workspace.default.tranco_bronze')

# Clean domain name (lowercase and trim) and remove empty rows
# Note: if your column is named '_c1' instead of 'domain', change 'domain' below
df_tranco = df_tranco.withColumn('domain_clean', lower(trim(col('domain'))))\
                     .filter((col('domain_clean').isNotNull()) & (col('domain_clean') != ''))\
                     .dropDuplicates(['domain_clean'])

# Save cleaned Tranco table to Silver
df_tranco.write.format('delta')\
         .mode('overwrite')\
         .saveAsTable('workspace.default.tranco_silver')


# ==========================================
# 3. CERT Active Silver (Domains from JSON)
# ==========================================

# Read raw CERT domains table from Bronze
df_cert = spark.table('workspace.default.cert_active_bronze')

# Clean domain address (lowercase, trim, remove leading 'www.') and filter empty values
df_cert = df_cert.withColumn(
    'domain_clean',
    regexp_replace(lower(trim(col('DomainAddress'))), '^www\\.', '')
).filter((col('domain_clean').isNotNull()) & (col('domain_clean') != ''))

# Cast date strings to timestamp and add an active flag
df_cert = df_cert.withColumn('insert_ts', to_timestamp(col('InsertDate')))\
                 .withColumn('delete_ts', to_timestamp(col('DeleteDate')))\
                 .withColumn('is_active', when(col('delete_ts').isNull(), lit(True)).otherwise(lit(False)))

# Remove duplicate rows by position ID
df_cert = df_cert.dropDuplicates(['RegisterPositionId'])

# Save cleaned CERT table to Silver
df_cert.write.format('delta')\
       .mode('overwrite')\
       .saveAsTable('workspace.default.cert_active_silver')