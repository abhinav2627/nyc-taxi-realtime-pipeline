# 🚕 NYC Taxi Real-Time Pipeline
### End-to-End Streaming Data Engineering Project on Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│              NYC TAXI REAL-TIME PIPELINE                              │
│         Streaming Medallion Architecture on Databricks                │
└──────────────────────────────────────────────────────────────────────┘

  [NYC TLC Public Dataset — Jan 1 2024]
  81,013 real Yellow Taxi trips
        │
        ▼  Auto Loader (cloudFiles) — Simulated Kafka Consumer
        │  Watches landing volume for new hourly batch files
        │  Exactly-once processing guarantee
  ┌─────────────────────────────────────┐
  │           BRONZE LAYER              │
  │      bronze_trips                   │
  │  • Raw trips, no transforms         │
  │  • + _ingest_timestamp              │
  │  • + _source_file (metadata)        │
  │  • + _batch_hour (Kafka partition)  │
  │  • 81,013 rows                      │
  └──────────────┬──────────────────────┘
                 │
                 ▼  Watermark (10 min) + Inline Transforms + Anomaly Detection
  ┌─────────────────────────────────────┐
  │           SILVER LAYER              │
  │      silver_trips                   │
  │  • Cleaned & typed                  │
  │  • Trip duration, speed, tip %      │
  │  • Time-of-day categorisation       │
  │  • 5 anomaly flags per row          │
  │  • 80,983 rows (99.9% pass DQ)      │
  └──────────────┬──────────────────────┘
                 │
                 ▼  Batch Aggregations + Window Functions
  ┌──────────────────────────┐    ┌─────────────────────────────┐
  │  GOLD — Zone KPIs        │    │  GOLD — Hourly KPIs         │
  │  gold_zone_kpis          │    │  gold_hourly_kpis           │
  │  • Revenue per zone      │    │  • 24-hour revenue curve    │
  │  • Avg fare, speed, tip  │    │  • Peak hour detection      │
  │  • Borough enrichment    │    │  • Payment breakdown        │
  │  • 233 zones             │    │  • 24 rows                  │
  └──────────────────────────┘    └─────────────────────────────┘
```

---

## 📊 Results

| Metric | Value |
|---|---|
| Total Trips Processed | 81,013 |
| Clean Trips (Silver) | 80,983 (99.9%) |
| Anomalies Flagged | 12,561 (15.5%) |
| Total Revenue (Jan 1) | $2,100,007.42 |
| Peak Revenue Hour | 01:00 AM ($147,626) |
| Top Pickup Zone | JFK Airport — Queens |
| NYC Zones Covered | 233 of 265 |
| Gold Tables Built | 2 |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Databricks Free Edition** | Serverless compute + notebooks |
| **Auto Loader (cloudFiles)** | Streaming file ingestion — Kafka simulation |
| **Delta Lake** | ACID streaming writes + time travel |
| **PySpark Structured Streaming** | Watermarking + stateless transforms |
| **Unity Catalog** | Data governance — Volumes + managed tables |
| **Python 3.10** | All notebook logic |

---

## 📁 Project Structure

```
nyc-taxi-realtime-pipeline/
│
├── notebooks/
│   ├── 00_setup_and_download.py    # Download real NYC TLC data + split into 24 hourly batches
│   ├── 01_bronze_streaming.py      # Auto Loader → Bronze Delta table
│   ├── 02_silver_streaming.py      # Watermark + transforms + anomaly detection → Silver
│   └── 03_gold_aggregations.py     # Zone KPIs + Hourly revenue → Gold tables
│
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
- Databricks Free Edition account ([sign up here](https://www.databricks.com/try-databricks))
- Unity Catalog enabled (default on Free Edition)
- Internet access from your Databricks workspace (for data download)

### Steps

**1. Clone this repo**
```bash
git clone https://github.com/abhinav2627/nyc-taxi-realtime-pipeline.git
```

**2. Import notebooks to Databricks**
- Go to Databricks Workspace → your folder
- Click ⋮ → Import → File
- Upload each `.py` file from the `notebooks/` folder

**3. Update catalog name in each notebook**
```python
MY_CATALOG  = "workspace"   # ← replace with your catalog name
SCHEMA_NAME = "nyc_taxi_pipeline"
```

**4. Run notebooks in order**
```
00 → 01 → 02 → 03
```
Connect each notebook to **Serverless** compute before running.

---

## 🔍 Streaming Features Demonstrated

### Auto Loader (Simulated Kafka Consumer)
The pipeline splits 81K trips into **24 hourly batch files**, then uses Auto Loader (`cloudFiles` format) to discover and process each file as it arrives — exactly like a Kafka consumer reading messages off a topic partition.

```python
spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format", "parquet")
     .option("cloudFiles.schemaLocation", checkpoint_path)
     .schema(inferred_schema)
     .load(landing_batch_path)
```

### Watermarking for Late Data
Spark's watermark tells the engine to wait up to **10 minutes** for late-arriving events before closing a time window — essential for accurate streaming aggregations in real production pipelines.

```python
bronze_stream.withWatermark("tpep_pickup_datetime", "10 minutes")
```

### Real-Time Anomaly Detection
5 anomaly flags applied to every row on the stream:

| Flag | Condition |
|---|---|
| `anomaly_zero_fare` | fare = $0 but distance > 0 miles |
| `anomaly_speed` | speed > 120 mph (physically impossible in NYC) |
| `anomaly_long_trip` | trip duration > 5 hours |
| `anomaly_negative_amount` | any negative fare/tip/total |
| `anomaly_no_passengers` | passenger count = 0 or null |

---

## 📈 Gold Layer KPIs

### `gold_zone_kpis` (Grain: pickup zone)
- Total trips + revenue
- Avg fare, distance, duration, speed
- Avg tip percentage
- Credit card payment %
- Anomaly count
- Revenue rank across all 233 zones
- Borough + zone name (joined from NYC TLC lookup)

### `gold_hourly_kpis` (Grain: hour 0–23)
- Total trips per hour
- Total revenue (clean trips only)
- Avg fare + tip %
- Credit card vs cash breakdown
- Avg speed (congestion proxy)
- Peak hour flag (top 3 revenue hours)

---

## 💡 Key Learnings

- **Auto Loader** provides exactly-once file ingestion with automatic schema evolution — far more robust than manual `spark.readStream.format("parquet")`
- **Watermarking** is critical for streaming aggregations — without it, late data silently corrupts window results
- **Inline transforms** on streams are safer than chaining `.transform()` functions — avoids Column object conflicts
- **TIMESTAMP_NTZ** (no timezone) columns from modern Parquet files must be cast to `TIMESTAMP` before watermarking on Databricks Serverless
- **JFK Airport** generates more revenue than Midtown Manhattan on an average day — late-night airport runs dominate

---

## 🗺️ Portfolio Roadmap

This is **Project 2** of a 6-month data engineering portfolio series:

- ✅ Project 1 — [Financial Transactions Lakehouse](https://github.com/abhinav2627/financial-transactions-lakehouse) (Databricks + Delta Lake)
- ✅ Project 2 — NYC Taxi Real-Time Pipeline (Auto Loader + Streaming + Watermarking)
- 🔜 Project 3 — COVID Data Warehouse (dbt + Snowflake + Airflow)
- 🔜 Project 4 — E-Commerce ETL Pipeline (Python + PostgreSQL)
- 🔜 Project 5 — Weather API Data Lake (AWS S3 + Lambda + Glue)

---

## 👤 Author

**Abhinav Mandal**
- LinkedIn: [linkedin.com/in/YOUR_HANDLE](https://linkedin.com)
- GitHub: [github.com/abhinav2627](https://github.com/abhinav2627)

---

## 📄 License

MIT License — feel free to use this project as a template for your own portfolio.
