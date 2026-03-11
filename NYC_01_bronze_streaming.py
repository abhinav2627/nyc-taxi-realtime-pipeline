# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Layer — Auto Loader Streaming Ingestion
# MAGIC ### Simulates Kafka consumer: Auto Loader watches landing zone → streams to Bronze Delta
# MAGIC
# MAGIC **Auto Loader** is Databricks' production file streaming tool — it:
# MAGIC - Watches a directory for new files continuously
# MAGIC - Tracks which files have been processed (exactly-once guarantee)
# MAGIC - Scales to billions of files
# MAGIC - Equivalent to a Kafka consumer in file-based architectures

# COMMAND ----------

# MAGIC %md ## 0 · Config

# COMMAND ----------

MY_CATALOG    = "workspace"
SCHEMA_NAME   = "nyc_taxi_pipeline"
LANDING_BATCH = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/landing/batches"
BRONZE_TABLE  = f"{MY_CATALOG}.{SCHEMA_NAME}.bronze_trips"
BRONZE_CKPT   = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/checkpoints/bronze"

spark.sql(f"USE {MY_CATALOG}.{SCHEMA_NAME}")
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("✅ Config ready")
print(f"   Watching  : {LANDING_BATCH}")
print(f"   Writing to: {BRONZE_TABLE}")
print(f"   Checkpoint: {BRONZE_CKPT}")

# COMMAND ----------

# MAGIC %md ## 1 · Drop Old Table + Clear Checkpoint

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
dbutils.fs.rm(BRONZE_CKPT, recurse=True)
print("✅ Clean slate ready")

# COMMAND ----------

# MAGIC %md ## 2 · Define Raw Schema
# MAGIC
# MAGIC Explicit schema = faster Auto Loader (no schema inference on every batch)

# COMMAND ----------

raw_schema = StructType([
    StructField("VendorID",                LongType(),   True),
    StructField("tpep_pickup_datetime",    TimestampType(), True),
    StructField("tpep_dropoff_datetime",   TimestampType(), True),
    StructField("passenger_count",         DoubleType(), True),
    StructField("trip_distance",           DoubleType(), True),
    StructField("RatecodeID",              DoubleType(), True),
    StructField("store_and_fwd_flag",      StringType(), True),
    StructField("PULocationID",            LongType(),   True),
    StructField("DOLocationID",            LongType(),   True),
    StructField("payment_type",            LongType(),   True),
    StructField("fare_amount",             DoubleType(), True),
    StructField("extra",                   DoubleType(), True),
    StructField("mta_tax",                 DoubleType(), True),
    StructField("tip_amount",              DoubleType(), True),
    StructField("tolls_amount",            DoubleType(), True),
    StructField("improvement_surcharge",   DoubleType(), True),
    StructField("total_amount",            DoubleType(), True),
    StructField("congestion_surcharge",    DoubleType(), True),
    StructField("Airport_fee",             DoubleType(), True),
])

print(f"✅ Schema defined — {len(raw_schema)} columns")

# COMMAND ----------

# MAGIC %md ## 3 · Auto Loader Stream — Simulated Kafka Consumer
# MAGIC
# MAGIC Auto Loader reads new parquet files as they land,
# MAGIC exactly like a Kafka consumer reading new messages off a topic.

# COMMAND ----------

# Build the Auto Loader stream
raw_stream = (
    spark.readStream
         .format("cloudFiles")                      # ← Auto Loader format
         .option("cloudFiles.format", "parquet")    # ← file format inside
         .option("cloudFiles.schemaLocation", f"{BRONZE_CKPT}/schema")
         .schema(raw_schema)
         .load(LANDING_BATCH)
)

# Add Kafka-style metadata columns
bronze_stream = (
    raw_stream
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file",      F.col("_metadata.file_path"))
    .withColumn("_kafka_offset_sim", F.monotonically_increasing_id())  # simulated Kafka offset
    .withColumn("_batch_hour",
        F.regexp_extract(F.col("_metadata.file_path"), r"hour_(\d+)", 1)
         .cast("int"))
)

print(f"✅ Auto Loader stream defined")
print(f"   isStreaming: {raw_stream.isStreaming}")

# COMMAND ----------

# MAGIC %md ## 4 · Write Bronze Stream → Delta Table

# COMMAND ----------

bronze_writer = (
    bronze_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CKPT)
    .trigger(availableNow=True)       # process all available files now, then stop
    .toTable(BRONZE_TABLE)
)

bronze_writer.awaitTermination()
print("✅ Bronze stream complete!")

# COMMAND ----------

# MAGIC %md ## 5 · Validate Bronze Table

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
count     = bronze_df.count()

print(f"✅ Bronze table validated!")
print(f"   Table     : {BRONZE_TABLE}")
print(f"   Row count : {count:,}")
print(f"   Columns   : {len(bronze_df.columns)}")

# COMMAND ----------

print("=== Bronze Sample ===")
display(
    bronze_df.select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "fare_amount", "total_amount",
        "_ingest_timestamp", "_batch_hour"
    ).limit(10)
)

# COMMAND ----------

# Hourly distribution — shows streaming batches were processed
print("=== Trips per Batch Hour ===")
display(
    bronze_df
    .groupBy("_batch_hour")
    .count()
    .orderBy("_batch_hour")
)

# COMMAND ----------

# MAGIC %md ## 6 · Stream Stats

# COMMAND ----------

print("=" * 50)
print("   BRONZE LAYER SUMMARY")
print("=" * 50)
print(f"   Total trips ingested : {count:,}")
print(f"   Columns              : {len(bronze_df.columns)}")
print(f"   Kafka-style metadata : _ingest_timestamp, _source_file, _kafka_offset_sim, _batch_hour")
print(f"   Storage format       : Delta Lake (ACID)")
print("=" * 50)
print("\n🎉 Bronze complete! Proceed to 02_silver_streaming")
