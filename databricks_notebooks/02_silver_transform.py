# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Transform - BACI Semiconductor Trade Data
# MAGIC Transforms raw Bronze data into a clean, analysis-ready Silver layer.
# MAGIC All transformations documented below - Bronze source data is never modified.
# MAGIC
# MAGIC **Input:** /Volumes/main/semiconductors/raw_baci/ (79M rows, all products)<br>
# MAGIC **Output:** /Volumes/main/semiconductors/silver_trade/ (Parquet, semiconductors only)<br>
# MAGIC **Source:** CEPII BACI HS17 V202601 - see source_registry.csv SRC001<br><br>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Loads raw Bronze data into Spark DataFrame
# MAGIC - Filters 79M rows down to semiconductor HS codes 8541 and 8542 only
# MAGIC - Renames single-letter BACI columns to readable names
# MAGIC - Adds semiconductor_category column (Discrete Semiconductors / Integrated Circuits)
# MAGIC - Drops quantity column (2.2M nulls, not used in analysis)
# MAGIC - Casts data types correctly
# MAGIC - Writes clean output to Parquet - Silver layer
# MAGIC - Validates output row count and schema before closing

# COMMAND ----------

# =============================================================
# 02_silver_transform.ipynb
# Semiconductor Supply Chain Analysis
# Author: Ray Garza | Austin TX | March 2026
# Purpose: Transform Bronze BACI data → Silver layer
#          Filter to semiconductors, rename, clean, write Parquet
# =============================================================

from pyspark.sql.functions import col, when, lit
import pyspark.sql.functions as F

# Paths
BRONZE_PATH = "/Volumes/main/semiconductors/raw_baci/"
SILVER_PATH = "/Volumes/main/semiconductors/silver_trade/"

print(f"Spark version: {spark.version}")
print(f"Bronze source: {BRONZE_PATH}")
print(f"Silver destination: {SILVER_PATH}")
print("Silver transform initialized.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Load Bronze Data
# MAGIC Re-load the raw BACI data from the Bronze volume.
# MAGIC Same read operation as bronze_ingest - no transformations yet.
# MAGIC 79M rows expected.

# COMMAND ----------

# Load all 7 CSVs from Bronze volume
# Same operation as bronze_ingest — confirming data is still intact
df_bronze = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(BRONZE_PATH)
)

# Quick confirmation — lazy, no scan yet
print(f"Schema: {df_bronze.columns}")
print(f"Bronze data loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Filter to Semiconductor HS Codes
# MAGIC Reduces 79M rows to semiconductor-only trade flows.
# MAGIC
# MAGIC HS 8541 = discrete semiconductors (diodes, transistors, LEDs)<br>
# MAGIC HS 8542 = integrated circuits (the chips)
# MAGIC
# MAGIC Filter logic: keep rows where first 4 digits of HS code match 8541 or 8542.
# MAGIC This is the core transformation that makes the dataset analysis-ready.

# COMMAND ----------

# Filter to semiconductor HS codes only
# BACI k column contains 6-digit HS codes as integers
# Cast to string, check first 4 digits against our target chapters

df_semicon = df_bronze.filter(
    col("k").cast("string").startswith("8541") |
    col("k").cast("string").startswith("8542")
)

# Count — this triggers a full Spark scan
semicon_count = df_semicon.count()
reduction_pct = (1 - semicon_count / 79258376) * 100

print(f"Total rows after filter: {semicon_count:,}")
print(f"Reduction: {reduction_pct:.1f}% of rows removed")
print(f"Semiconductor share of all global trade rows: {100-reduction_pct:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename Columns to Readable Names
# MAGIC BACI uses single-letter column names for compact storage.<br>
# MAGIC Renaming to descriptive names for readability downstream.<br><br>
# MAGIC Mapping:<br>
# MAGIC t→year,<br> i→exporter_code,<br> j→importer_code,<br> 
# MAGIC          k→hs6_code,<br> v→trade_value_usd_thousands,<br> q→quantity_mt <br><br>
# MAGIC Note: trade_value is in thousands of USD in raw BACI data.
# MAGIC We multiply by 1000 to convert to actual USD.

# COMMAND ----------

# Rename columns and convert trade value to actual USD
# BACI v column is in thousands of USD — multiply by 1000
from pyspark.sql.functions import col, round as spark_round

df_renamed = (df_semicon
    .withColumnRenamed("t", "year")
    .withColumnRenamed("i", "exporter_code")
    .withColumnRenamed("j", "importer_code")
    .withColumnRenamed("k", "hs6_code")
    .withColumn("trade_value_usd", 
                spark_round(col("v").cast("double") * 1000, 2))
    .withColumnRenamed("q", "quantity_mt")
    .drop("v")  # drop original v column, replaced by trade_value_usd
)

print(f"Columns after rename: {df_renamed.columns}")
df_renamed.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add Semiconductor Category
# MAGIC
# MAGIC
# MAGIC Maps hs6_code to human-readable category.<br>
# MAGIC 8541xx = Discrete Semiconductors<br>
# MAGIC 8542xx = Integrated Circuits<br><br>
# MAGIC This column makes Tableau filtering intuitive for non-technical viewers.

# COMMAND ----------

# Add semiconductor_category based on first 4 digits of hs6_code
from pyspark.sql.functions import when

df_categorized = df_renamed.withColumn(
    "semiconductor_category",
    when(col("hs6_code").cast("string").startswith("8541"), 
         "Discrete Semiconductors")
    .when(col("hs6_code").cast("string").startswith("8542"), 
         "Integrated Circuits")
    .otherwise("Other")
)

# Verify category distribution
print("=== Category Distribution ===")
df_categorized.groupBy("semiconductor_category").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Drop Quantity Column and Clean Nulls
# MAGIC quantity_mt has 2.2M nulls across the full dataset - not needed for analysis.
# MAGIC
# MAGIC Our story is told through trade_value_usd, not physical weight.<br>
# MAGIC Also drop any rows where trade_value_usd is null or zero - not meaningful.

# COMMAND ----------

# Drop quantity column — not used in analysis
# Drop rows where trade_value_usd is null or zero
df_clean = (df_categorized
    .drop("quantity_mt")
    .filter(col("trade_value_usd").isNotNull())
    .filter(col("trade_value_usd") > 0)
)

# Final row count
clean_count = df_clean.count()
print(f"Rows after cleaning: {clean_count:,}")
print(f"Columns: {df_clean.columns}")
print(f"\nSample:")
df_clean.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Ad Hoc Analysis
# MAGIC Quick exploration before writing Silver output.
# MAGIC
# MAGIC Answering: which countries dominate semiconductor exports? 

# COMMAND ----------

# Quick look — top 10 exporting countries by total trade value
# exporter_code 158 = Taiwan, 156 = China, 842 = USA, 410 = South Korea
print("=== Top 10 Exporters by Total Trade Value (2018-2024) ===")
(df_clean
    .groupBy("exporter_code")
    .agg(F.sum("trade_value_usd").alias("total_export_usd"))
    .orderBy(col("total_export_usd").desc())
    .show(10)
)

# Split by category
print("=== Trade Value by Semiconductor Category ===")
(df_clean
    .groupBy("semiconductor_category")
    .agg(F.sum("trade_value_usd").alias("total_usd"))
    .show()
)

# COMMAND ----------

# Compare top countries side by side
print("=== Top 5 exporters — 2024 only ===")
(df_clean
    .filter(col("year") == 2024)
    .groupBy("exporter_code")
    .agg(F.sum("trade_value_usd").alias("total_usd"))
    .orderBy(col("total_usd").desc())
    .show(10)
)

# Check if 158 (Taiwan) exists at all
print("=== Does Taiwan (158) appear? ===")
taiwan_count = df_clean.filter(col("exporter_code") == 158).count()
print(f"Rows with exporter_code 158: {taiwan_count:,}")

# COMMAND ----------

# Map BACI country codes to ISO3 and country names
# Source: BACI documentation + UN M49 standard
# Note: 490 = Taiwan (BACI classification for "Other Asia NES")

country_map = {
    490: ("TWN", "Taiwan"),
    156: ("CHN", "China"),
    410: ("KOR", "South Korea"),
    458: ("MYS", "Malaysia"),
    702: ("SGP", "Singapore"),
    392: ("JPN", "Japan"),
    842: ("USA", "United States"),
    704: ("VNM", "Vietnam"),
    608: ("PHL", "Philippines"),
    276: ("DEU", "Germany"),
    528: ("NLD", "Netherlands"),
    764: ("THA", "Thailand"),
    356: ("IND", "India"),
    348: ("HUN", "Hungary"),
    203: ("CZE", "Czech Republic"),
    372: ("IRL", "Ireland"),
    826: ("GBR", "United Kingdom"),
    250: ("FRA", "France"),
    724: ("ESP", "Spain"),
    380: ("ITA", "Italy"),
    36:  ("AUS", "Australia"),
    124: ("CAN", "Canada"),
    76:  ("BRA", "Brazil"),
    484: ("MEX", "Mexico"),
    682: ("SAU", "Saudi Arabia"),
    360: ("IDN", "Indonesia"),
    116: ("KHM", "Cambodia"),
    764: ("THA", "Thailand"),
    40:  ("AUT", "Austria"),
    56:  ("BEL", "Belgium"),
}

# Create a Spark DataFrame from the mapping
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("country_code", IntegerType()),
    StructField("iso3", StringType()),
    StructField("country_name", StringType())
])

country_rows = [(k, v[0], v[1]) for k, v in country_map.items()]
df_countries = spark.createDataFrame(country_rows, schema)

print(f"Country lookup entries: {df_countries.count()}")
df_countries.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Join Country Names
# MAGIC Joins country code lookup to trade data for both exporter and importer.<br>
# MAGIC Replaces numeric codes with ISO3 and full country names.<br>
# MAGIC Countries not in the lookup table kept as-is with null names - these are smaller trading partners, acceptable for this analysis.

# COMMAND ----------

# Join country names for exporters
df_with_exporter = df_clean.join(
    df_countries.select(
        col("country_code").alias("exporter_code"),
        col("iso3").alias("exporter_iso3"),
        col("country_name").alias("exporter_name")
    ),
    on="exporter_code",
    how="left"
)

# Join country names for importers
df_silver = df_with_exporter.join(
    df_countries.select(
        col("country_code").alias("importer_code"),
        col("iso3").alias("importer_iso3"),
        col("country_name").alias("importer_name")
    ),
    on="importer_code",
    how="left"
)

print(f"Columns: {df_silver.columns}")
print(f"\nSample with country names:")
df_silver.filter(col("exporter_name") == "Taiwan").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Final Schema Check Before Writing
# MAGIC Verify row count, schema, and data quality one last time. <br>
# MAGIC This is our last checkpoint before writing the Silver Parquet output.

# COMMAND ----------

# Final checks before writing Silver output
print("=== FINAL SILVER LAYER CHECK ===")
print(f"Row count: {df_silver.count():,}")
print(f"Columns: {df_silver.columns}")
print(f"Schema:")
df_silver.printSchema()

# Check named exporters vs unnamed
named = df_silver.filter(col("exporter_name").isNotNull()).count()
unnamed = df_silver.filter(col("exporter_name").isNull()).count()
print(f"\nExporters with names: {named:,}")
print(f"Exporters without names (smaller countries): {unnamed:,}")

# Top 5 exporters with names - sanity check
print("\n=== Top 5 Named Exporters (all years) ===")
(df_silver
    .filter(col("exporter_name").isNotNull())
    .groupBy("exporter_name")
    .agg(F.sum("trade_value_usd").alias("total_usd"))
    .orderBy(col("total_usd").desc())
    .show(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9 - Write Silver Layer to Parquet
# MAGIC Writes clean semiconductor trade data to Unity Catalog volume.<br>
# MAGIC Partitioned by year for efficient querying - Tableau and Snowflake<br>
# MAGIC can read a single year without scanning all 366K rows.<br>
# MAGIC Output format: Parquet (columnar, compressed, schema-preserved)<br>
# MAGIC This is the Silver layer - clean but not yet modeled.<br>
# MAGIC Modeling happens in Snowflake (Gold layer).

# COMMAND ----------

# Create the silver_trade volume if it doesn't exist
spark.sql("CREATE VOLUME IF NOT EXISTS main.semiconductors.silver_trade")
print("Volume created.")

# COMMAND ----------

# Write Silver layer to Parquet partitioned by year
# Partitioning by year means queries like "show me 2024 data"
# only scan 1/7 of the file - much faster for Tableau/Snowflake

(df_silver
    .write
    .mode("overwrite")
    .partitionBy("year")
    .parquet(SILVER_PATH)
)

print("Silver layer written successfully.")
print(f"Location: {SILVER_PATH}")
print(f"Partitioned by: year")
print(f"Rows written: 366,637")
print(f"Format: Parquet")
print(f"\nNext step: 03_silver_validate")