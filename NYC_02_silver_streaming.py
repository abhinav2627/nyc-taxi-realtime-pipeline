# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver Layer — Streaming Cleaning + Watermarking
# MAGIC ### Bronze stream → Cleaned, typed, anomaly-flagged Silver Delta table

# COMMAND ----------

# MAGIC %md ## 0 · Config

# COMMAND ----------

MY_CATALOG       = "workspace"
SCHEMA_NAME      = "nyc_taxi_pipeline"
BRONZE_TABLE     = f"{MY_CATALOG}.{SCHEMA_NAME}.bronze_trips"
SILVER_TABLE     = f"{MY_CATALOG}.{SCHEMA_NAME}.silver_trips"
SILVER_CKPT      = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/checkpoints/silver"

spark.sql(f"USE {MY_CATALOG}.{SCHEMA_NAME}")
from pyspark.sql import functions as F

print("✅ Config ready")
print(f"   Reading   : {BRONZE_TABLE}")
print(f"   Writing to: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md ## 1 · Drop Old Silver Table + Clear Checkpoint

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
dbutils.fs.rm(SILVER_CKPT, recurse=True)
print("✅ Clean slate ready")

# COMMAND ----------

# MAGIC %md ## 2 · Read Bronze as Stream

# COMMAND ----------

bronze_stream = (
    spark.readStream
         .format("delta")
         .table(BRONZE_TABLE)
)

print(f"✅ Bronze stream ready | isStreaming: {bronze_stream.isStreaming}")

# COMMAND ----------

# MAGIC %md ## 3 · Build Silver Stream
# MAGIC
# MAGIC All transformations applied inline — avoids Column callable conflicts.
# MAGIC Watermark handles late-arriving events (10 min tolerance).

# COMMAND ----------

silver_stream = (
    bronze_stream

    # ── Watermark for late data ─────────────────────────────────────────────
    .withWatermark("tpep_pickup_datetime", "10 minutes")

    # ── Hard DQ filter — drop truly invalid rows ────────────────────────────
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    .filter(
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60.0 < 1440   # < 24 hours
    )
    .filter(F.col("total_amount") < 1000)

    # ── Trip duration (minutes) ─────────────────────────────────────────────
    .withColumn("trip_duration_mins",
        F.round(
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60.0, 2
        )
    )

    # ── Speed (mph) ─────────────────────────────────────────────────────────
    .withColumn("speed_mph",
        F.when(
            (F.col("trip_duration_mins") > 0) & (F.col("trip_distance") > 0),
            F.round(F.col("trip_distance") / (F.col("trip_duration_mins") / 60.0), 2)
        ).otherwise(None)
    )

    # ── Tip percentage ───────────────────────────────────────────────────────
    .withColumn("tip_pct",
        F.when(F.col("fare_amount") > 0,
            F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 1)
        ).otherwise(0.0)
    )

    # ── Payment type decoded ─────────────────────────────────────────────────
    .withColumn("payment_type_desc",
        F.when(F.col("payment_type") == 1, "credit_card")
         .when(F.col("payment_type") == 2, "cash")
         .when(F.col("payment_type") == 3, "no_charge")
         .when(F.col("payment_type") == 4, "dispute")
         .otherwise("unknown")
    )

    # ── Time dimensions ──────────────────────────────────────────────────────
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("time_of_day",
        F.when((F.col("pickup_hour") >= 6)  & (F.col("pickup_hour") < 12), "morning")
         .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 17), "afternoon")
         .when((F.col("pickup_hour") >= 17) & (F.col("pickup_hour") < 21), "evening")
         .otherwise("night")
    )

    # ── Fare per mile ────────────────────────────────────────────────────────
    .withColumn("fare_per_mile",
        F.when(F.col("trip_distance") > 0,
            F.round(F.col("fare_amount") / F.col("trip_distance"), 2)
        ).otherwise(None)
    )

    # ── Anomaly flags ────────────────────────────────────────────────────────
    .withColumn("anomaly_zero_fare",
        (F.col("fare_amount") == 0) & (F.col("trip_distance") > 0)
    )
    .withColumn("anomaly_speed",
        F.col("speed_mph") > 120
    )
    .withColumn("anomaly_long_trip",
        F.col("trip_duration_mins") > 300
    )
    .withColumn("anomaly_negative_amount",
        (F.col("fare_amount") < 0) |
        (F.col("tip_amount")  < 0) |
        (F.col("total_amount") < 0)
    )
    .withColumn("anomaly_no_passengers",
        (F.col("passenger_count") == 0) | F.col("passenger_count").isNull()
    )
    .withColumn("is_anomaly",
        F.col("anomaly_zero_fare")       |
        F.col("anomaly_speed")           |
        F.col("anomaly_long_trip")       |
        F.col("anomaly_negative_amount") |
        F.col("anomaly_no_passengers")
    )

    # ── Silver metadata ──────────────────────────────────────────────────────
    .withColumn("_silver_timestamp", F.current_timestamp())
)

print("✅ Silver stream defined")

# COMMAND ----------

# MAGIC %md ## 4 · Write Silver Stream → Delta Table

# COMMAND ----------

silver_writer = (
    silver_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CKPT)
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

silver_writer.awaitTermination()
print("✅ Silver stream complete!")

# COMMAND ----------

# MAGIC %md ## 5 · Validate Silver

# COMMAND ----------

silver_df     = spark.table(SILVER_TABLE)
silver_count  = silver_df.count()
anomaly_count = silver_df.filter(F.col("is_anomaly") == True).count()
bronze_count  = spark.table(BRONZE_TABLE).count()

print(f"✅ Silver table validated!")
print(f"   Bronze in      : {bronze_count:,}")
print(f"   Silver out     : {silver_count:,}")
print(f"   DQ filtered    : {bronze_count - silver_count:,}")
print(f"   Anomalies found: {anomaly_count:,}  ({anomaly_count/silver_count*100:.1f}%)")

# COMMAND ----------

print("=== Silver Sample ===")
display(
    silver_df.select(
        "tpep_pickup_datetime", "trip_distance", "trip_duration_mins",
        "speed_mph", "fare_amount", "tip_pct", "payment_type_desc",
        "time_of_day", "is_anomaly"
    ).limit(10)
)

# COMMAND ----------

print("=== Anomaly Breakdown ===")
display(
    silver_df.select(
        F.sum(F.col("anomaly_zero_fare").cast("int")).alias("zero_fare"),
        F.sum(F.col("anomaly_speed").cast("int")).alias("impossible_speed"),
        F.sum(F.col("anomaly_long_trip").cast("int")).alias("long_trip_5h"),
        F.sum(F.col("anomaly_negative_amount").cast("int")).alias("negative_amount"),
        F.sum(F.col("anomaly_no_passengers").cast("int")).alias("no_passengers"),
        F.sum(F.col("is_anomaly").cast("int")).alias("total_anomalies"),
    )
)

# COMMAND ----------

print("=== Trips by Time of Day ===")
display(
    silver_df
    .groupBy("time_of_day")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_pct"), 1).alias("avg_tip_pct")
    )
    .orderBy(F.desc("trip_count"))
)

# COMMAND ----------

# MAGIC %md ## 6 · Final Summary

# COMMAND ----------

print("=" * 55)
print("   SILVER LAYER SUMMARY")
print("=" * 55)
print(f"   Bronze trips in   : {bronze_count:,}")
print(f"   Silver trips out  : {silver_count:,}")
print(f"   DQ filtered out   : {bronze_count - silver_count:,}")
print(f"   Anomalies flagged : {anomaly_count:,}")
print(f"   Watermark applied : 10 minutes (late data tolerance)")
print(f"   Anomaly rate      : {anomaly_count/silver_count*100:.1f}%")
print("=" * 55)
print("\n🎉 Silver complete! Proceed to 03_gold_aggregations")
