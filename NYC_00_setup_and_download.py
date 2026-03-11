# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup & Data Download — NYC Taxi Real-Time Pipeline
# MAGIC ### Downloads real NYC TLC Yellow Taxi data → Landing Volume
# MAGIC
# MAGIC **Data source:** NYC TLC Trip Record Data (Jan 2024)
# MAGIC - ~2.9M real taxi trips
# MAGIC - Official public dataset from NYC Taxi & Limousine Commission
# MAGIC - Columns: pickup/dropoff times, locations, fares, tips, passengers

# COMMAND ----------

# MAGIC %md ## 0 · Config

# COMMAND ----------

MY_CATALOG   = "workspace"
SCHEMA_NAME  = "nyc_taxi_pipeline"
LANDING_VOL  = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/landing"
BRONZE_VOL   = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/bronze"
SILVER_VOL   = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/silver"
CHECKPOINTS  = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/checkpoints"

print("✅ Config ready")
print(f"   Catalog : {MY_CATALOG}")
print(f"   Schema  : {SCHEMA_NAME}")
print(f"   Landing : {LANDING_VOL}")

# COMMAND ----------

# MAGIC %md ## 1 · Create Unity Catalog Schema + Volumes

# COMMAND ----------

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{SCHEMA_NAME}")
print(f"✅ Schema created: {MY_CATALOG}.{SCHEMA_NAME}")

# Create volumes
for vol in ["landing", "bronze", "silver", "checkpoints", "gold_zone", "gold_hourly"]:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{SCHEMA_NAME}.{vol}
    """)
    print(f"   ✅ Volume: {vol}")

print("\n✅ All volumes ready!")

# COMMAND ----------

# MAGIC %md ## 2 · Download NYC Taxi Data (Jan 2024)

# COMMAND ----------

import urllib.request
import os

# NYC TLC official data URL — Yellow Taxi Jan 2024
DATA_URL    = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
LOCAL_TMP   = "/tmp/yellow_tripdata_2024-01.parquet"
VOLUME_PATH = f"{LANDING_VOL}/yellow_tripdata_2024-01.parquet"

print("📥 Downloading NYC Yellow Taxi data (Jan 2024)...")
print(f"   Source : {DATA_URL}")
print(f"   Target : {VOLUME_PATH}")
print()

def download_progress(count, block_size, total_size):
    if total_size > 0:
        pct = min(int(count * block_size * 100 / total_size), 100)
        if pct % 20 == 0:
            mb = count * block_size / 1024 / 1024
            print(f"   Progress: {pct}% ({mb:.1f} MB downloaded)")

urllib.request.urlretrieve(DATA_URL, LOCAL_TMP, reporthook=download_progress)
print()

# Move from /tmp to Unity Catalog Volume
dbutils.fs.cp(f"file:{LOCAL_TMP}", VOLUME_PATH)
print(f"✅ File saved to Volume!")

# COMMAND ----------

# MAGIC %md ## 3 · Inspect the Raw Data

# COMMAND ----------

from pyspark.sql import functions as F

# Read parquet to inspect
raw_df = spark.read.parquet(VOLUME_PATH)

print(f"✅ Raw data loaded")
print(f"   Row count : {raw_df.count():,}")
print(f"   Columns   : {len(raw_df.columns)}")
print(f"\n   Schema:")
raw_df.printSchema()

# COMMAND ----------

print("=== Sample Raw Data ===")
display(raw_df.limit(5))

# COMMAND ----------

# MAGIC %md ## 4 · Split Into Streaming Batches
# MAGIC
# MAGIC To simulate real-time Kafka streaming, we split the data into
# MAGIC **24 hourly batches** — one per hour of Jan 1 2024.
# MAGIC Auto Loader will then "discover" each file as it arrives.

# COMMAND ----------

from pyspark.sql import functions as F

# Read full dataset
df = spark.read.parquet(VOLUME_PATH)

# Filter to Jan 1 2024 only (first day) for manageable demo size
# Still gives ~80,000-100,000 trips — plenty for a real pipeline demo
jan1_df = df.filter(
    (F.col("tpep_pickup_datetime") >= "2024-01-01 00:00:00") &
    (F.col("tpep_pickup_datetime") <  "2024-01-02 00:00:00")
).withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))

total = jan1_df.count()
print(f"✅ Jan 1 2024 trips: {total:,}")

# COMMAND ----------

# Split into 24 hourly batch files to simulate streaming
print("📦 Splitting into 24 hourly batch files...")
print()

batch_folder = f"{LANDING_VOL}/batches"
dbutils.fs.mkdirs(batch_folder)

hourly_counts = []
for hour in range(24):
    hour_df = jan1_df.filter(F.col("pickup_hour") == hour).drop("pickup_hour")
    count   = hour_df.count()
    out_path = f"{batch_folder}/hour_{hour:02d}"
    
    hour_df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(out_path)
    
    hourly_counts.append((hour, count))
    print(f"   hour_{hour:02d}  →  {count:,} trips")

print(f"\n✅ All 24 hourly batches written to {batch_folder}")
print(f"   Total trips : {sum(c for _, c in hourly_counts):,}")

# COMMAND ----------

# MAGIC %md ## 5 · Download Zone Lookup Table
# MAGIC
# MAGIC NYC Taxi zones map location IDs to borough + neighbourhood names.
# MAGIC Used in Gold layer to enrich trips with human-readable locations.

# COMMAND ----------

ZONE_URL   = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_LOCAL = "/tmp/taxi_zone_lookup.csv"
ZONE_VOL   = f"{LANDING_VOL}/taxi_zone_lookup.csv"

print("📥 Downloading Zone Lookup table...")
urllib.request.urlretrieve(ZONE_URL, ZONE_LOCAL)
dbutils.fs.cp(f"file:{ZONE_LOCAL}", ZONE_VOL)
print(f"✅ Zone lookup saved!")

# Preview
zones_df = spark.read.csv(ZONE_VOL, header=True, inferSchema=True)
print(f"   Zones: {zones_df.count()} rows")
display(zones_df.limit(10))

# COMMAND ----------

# MAGIC %md ## 6 · Final Summary

# COMMAND ----------

print("=" * 55)
print("   NYC TAXI PIPELINE — SETUP COMPLETE")
print("=" * 55)
print()
print("  VOLUMES CREATED:")
for vol in ["landing", "bronze", "silver", "checkpoints", "gold_zone", "gold_hourly"]:
    print(f"    ✅ /Volumes/{MY_CATALOG}/{SCHEMA_NAME}/{vol}")
print()
print("  DATA DOWNLOADED:")
print(f"    ✅ yellow_tripdata_2024-01.parquet  (full month)")
print(f"    ✅ 24 hourly batch files (Jan 1 2024)")
print(f"    ✅ taxi_zone_lookup.csv")
print()
print("  NEXT: Run notebook 01_bronze_streaming")
print("=" * 55)
