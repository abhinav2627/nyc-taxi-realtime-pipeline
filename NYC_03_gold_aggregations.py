# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold Layer — Zone KPIs + Hourly Revenue Aggregations
# MAGIC ### Silver → Two Gold tables with zone-level and hourly business metrics
# MAGIC
# MAGIC **Two Gold tables:**
# MAGIC - `gold_zone_kpis`   → Revenue, trips, avg fare per pickup zone (enriched with borough names)
# MAGIC - `gold_hourly_kpis` → Hourly revenue trend + payment breakdown across 24 hours

# COMMAND ----------

# MAGIC %md ## 0 · Config

# COMMAND ----------

MY_CATALOG   = "workspace"
SCHEMA_NAME  = "nyc_taxi_pipeline"
SILVER_TABLE = f"{MY_CATALOG}.{SCHEMA_NAME}.silver_trips"
GOLD_ZONE    = f"{MY_CATALOG}.{SCHEMA_NAME}.gold_zone_kpis"
GOLD_HOURLY  = f"{MY_CATALOG}.{SCHEMA_NAME}.gold_hourly_kpis"

spark.sql(f"USE {MY_CATALOG}.{SCHEMA_NAME}")
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("✅ Config ready")
print(f"   Silver   : {SILVER_TABLE}")
print(f"   Gold 1   : {GOLD_ZONE}")
print(f"   Gold 2   : {GOLD_HOURLY}")

# COMMAND ----------

# MAGIC %md ## 1 · Drop Old Gold Tables

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {GOLD_ZONE}")
spark.sql(f"DROP TABLE IF EXISTS {GOLD_HOURLY}")
print("✅ Old Gold tables dropped")

# COMMAND ----------

# MAGIC %md ## 2 · Load Silver + Zone Lookup

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
silver_count = silver_df.count()
print(f"✅ Silver loaded : {silver_count:,} rows")

# Load zone lookup from landing volume
ZONE_PATH = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/landing/taxi_zone_lookup.csv"

# Find the actual CSV file inside the folder written by spark
import os
try:
    # Try direct path first
    zones_raw = spark.read.csv(ZONE_PATH, header=True, inferSchema=True)
    zones_raw.count()
    zone_source = ZONE_PATH
except:
    # Fall back to folder path (written as partitioned folder)
    zone_folder = ZONE_PATH.replace(".csv", "")
    zones_raw = spark.read.csv(zone_folder, header=True, inferSchema=True)
    zone_source = zone_folder

zones_df = zones_raw.select(
    F.col("LocationID").cast("long").alias("location_id"),
    F.col("Borough").alias("borough"),
    F.col("Zone").alias("zone_name"),
    F.col("service_zone")
)

print(f"✅ Zone lookup loaded : {zones_df.count()} zones from {zone_source}")
display(zones_df.limit(5))

# COMMAND ----------

# MAGIC %md ## 3 · Gold Table 1 — Zone-Level KPIs
# MAGIC
# MAGIC **Grain:** one row per pickup zone
# MAGIC
# MAGIC Enriched with borough and zone name from the NYC TLC lookup table.
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | total_trips | All trips from this zone |
# MAGIC | total_revenue_usd | Revenue from non-anomaly trips |
# MAGIC | avg_fare_usd | Average fare amount |
# MAGIC | avg_distance_miles | Average trip distance |
# MAGIC | avg_duration_mins | Average trip duration |
# MAGIC | avg_tip_pct | Average tip percentage |
# MAGIC | avg_speed_mph | Average speed |
# MAGIC | anomaly_count | Flagged trips from this zone |
# MAGIC | credit_card_pct | % paying by credit card |
# MAGIC | revenue_rank_overall | Rank by total revenue |

# COMMAND ----------

zone_agg = (
    silver_df
    .filter(F.col("PULocationID").isNotNull())
    .groupBy("PULocationID")
    .agg(
        F.count("*")
            .alias("total_trips"),
        F.round(
            F.sum(F.when(F.col("is_anomaly") == False, F.col("total_amount")).otherwise(0)), 2)
            .alias("total_revenue_usd"),
        F.round(F.avg("fare_amount"), 2)
            .alias("avg_fare_usd"),
        F.round(F.avg("trip_distance"), 2)
            .alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_mins"), 1)
            .alias("avg_duration_mins"),
        F.round(F.avg("tip_pct"), 1)
            .alias("avg_tip_pct"),
        F.round(F.avg("speed_mph"), 1)
            .alias("avg_speed_mph"),
        F.sum(F.col("is_anomaly").cast("int"))
            .alias("anomaly_count"),
        F.round(
            F.sum(F.when(F.col("payment_type_desc") == "credit_card", 1).otherwise(0)) * 100.0
            / F.count("*"), 1)
            .alias("credit_card_pct"),
    )
    .withColumnRenamed("PULocationID", "location_id")
)

# Enrich with zone names via join
zone_kpis = (
    zone_agg
    .join(zones_df, on="location_id", how="left")
    .withColumn("revenue_rank_overall",
        F.rank().over(Window.orderBy(F.desc("total_revenue_usd")))
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
)

# Write Gold Zone
zone_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_ZONE)

zone_count = spark.table(GOLD_ZONE).count()
print(f"✅ Gold Zone KPIs written!")
print(f"   Table     : {GOLD_ZONE}")
print(f"   Row count : {zone_count:,} zones")

# COMMAND ----------

print("=== Top 15 Pickup Zones by Revenue ===")
display(
    spark.table(GOLD_ZONE)
         .select(
             "revenue_rank_overall", "zone_name", "borough",
             "total_trips", "total_revenue_usd",
             "avg_fare_usd", "avg_tip_pct"
         )
         .orderBy("revenue_rank_overall")
         .limit(15)
)

# COMMAND ----------

# MAGIC %md ## 4 · Gold Table 2 — Hourly KPIs
# MAGIC
# MAGIC **Grain:** one row per pickup hour (0–23)
# MAGIC
# MAGIC Shows the full revenue and demand curve across 24 hours of Jan 1 2024.
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | total_trips | Trips that hour |
# MAGIC | total_revenue_usd | Revenue from clean trips |
# MAGIC | avg_fare_usd | Average fare |
# MAGIC | avg_tip_pct | Average tip % |
# MAGIC | credit_card_trips | Credit card payments |
# MAGIC | cash_trips | Cash payments |
# MAGIC | avg_speed_mph | Average speed (congestion indicator) |
# MAGIC | peak_hour | Top 3 revenue hours flagged |

# COMMAND ----------

hourly_kpis = (
    silver_df
    .filter(F.col("pickup_hour").isNotNull())
    .groupBy("pickup_hour", "time_of_day")
    .agg(
        F.count("*")
            .alias("total_trips"),
        F.round(
            F.sum(F.when(F.col("is_anomaly") == False, F.col("total_amount")).otherwise(0)), 2)
            .alias("total_revenue_usd"),
        F.round(F.avg("fare_amount"), 2)
            .alias("avg_fare_usd"),
        F.round(F.avg("trip_distance"), 2)
            .alias("avg_distance_miles"),
        F.round(F.avg("tip_pct"), 1)
            .alias("avg_tip_pct"),
        F.round(F.avg("trip_duration_mins"), 1)
            .alias("avg_duration_mins"),
        F.sum(F.when(F.col("payment_type_desc") == "credit_card", 1).otherwise(0))
            .alias("credit_card_trips"),
        F.sum(F.when(F.col("payment_type_desc") == "cash", 1).otherwise(0))
            .alias("cash_trips"),
        F.sum(F.col("is_anomaly").cast("int"))
            .alias("anomaly_count"),
        F.round(F.avg("speed_mph"), 1)
            .alias("avg_speed_mph"),
    )
    .withColumn("revenue_rank",
        F.rank().over(Window.orderBy(F.desc("total_revenue_usd")))
    )
    .withColumn("peak_hour", F.col("revenue_rank") <= 3)
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy("pickup_hour")
)

# Write Gold Hourly
hourly_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_HOURLY)

hourly_count = spark.table(GOLD_HOURLY).count()
print(f"✅ Gold Hourly KPIs written!")
print(f"   Table     : {GOLD_HOURLY}")
print(f"   Row count : {hourly_count:,} hours")

# COMMAND ----------

print("=== Hourly Revenue + Trip Demand (full 24h) ===")
display(spark.table(GOLD_HOURLY))

# COMMAND ----------

# MAGIC %md ## 5 · Final Pipeline Report

# COMMAND ----------

bronze_count  = spark.table(f"{MY_CATALOG}.{SCHEMA_NAME}.bronze_trips").count()
silver_count  = spark.table(SILVER_TABLE).count()
zone_count    = spark.table(GOLD_ZONE).count()
hourly_count  = spark.table(GOLD_HOURLY).count()
anomaly_count = silver_df.filter(F.col("is_anomaly") == True).count()

total_rev  = spark.table(GOLD_ZONE).agg(F.sum("total_revenue_usd")).collect()[0][0]
peak_hour  = spark.table(GOLD_HOURLY).orderBy("revenue_rank").first()
top_zone   = spark.table(GOLD_ZONE).orderBy("revenue_rank_overall").first()

print("=" * 62)
print("   NYC TAXI REAL-TIME PIPELINE — FINAL REPORT")
print("=" * 62)
print()
print("  PIPELINE SUMMARY")
print(f"    Bronze trips ingested  : {bronze_count:,}")
print(f"    Silver trips (clean)   : {silver_count:,}")
print(f"    DQ filtered out        : {bronze_count - silver_count:,}")
print(f"    Anomalies flagged      : {anomaly_count:,}  ({anomaly_count/silver_count*100:.1f}%)")
print()
print("  BUSINESS KPIs  (NYC Yellow Taxi — Jan 1 2024)")
print(f"    Total Revenue          : ${total_rev:>12,.2f}")
print(f"    Total Clean Trips      : {silver_count:>12,}")
print(f"    Peak Revenue Hour      : {peak_hour['pickup_hour']:02d}:00  (${peak_hour['total_revenue_usd']:,.2f})")
print(f"    Top Pickup Zone        : {top_zone['zone_name']}  ({top_zone['borough']})")
print()
print("  TABLES CREATED:")
print(f"    ✅ bronze_trips         ({bronze_count:,} rows)")
print(f"    ✅ silver_trips         ({silver_count:,} rows)")
print(f"    ✅ gold_zone_kpis       ({zone_count:,} zones)")
print(f"    ✅ gold_hourly_kpis     ({hourly_count:,} hours)")
print()
print("  STREAMING FEATURES DEMONSTRATED:")
print("    ✅ Auto Loader (cloudFiles)  — simulated Kafka consumer")
print("    ✅ Watermarking              — 10 min late data tolerance")
print("    ✅ TIMESTAMP_NTZ casting     — Free Edition compatibility")
print("    ✅ Inline stream transforms  — anomaly detection per row")
print("    ✅ Delta Lake writes         — ACID streaming guarantee")
print("=" * 62)
print()
print("🎉 Pipeline complete! Bronze → Silver → Gold all healthy.")
