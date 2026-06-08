# E-Commerce Clickstream Lakehouse

A production-grade **Data Lakehouse** pipeline built on the Medallion Architecture (Bronze → Silver → Gold), processing e-commerce clickstream data using Apache Spark, Delta Lake, MinIO, Hive Metastore, Apache Airflow, and Trino — fully containerised with Docker Compose.

![Architecture Diagram]<img src="assets/Blank diagram.png" width="800"/>

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Pipeline Design](#pipeline-design)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
- [Orchestration](#orchestration)
- [Query Engine](#query-engine)
- [Getting Started](#getting-started)
- [Service Endpoints](#service-endpoints)
- [Environment Variables](#environment-variables)

---

## Overview

This project simulates a real-world **e-commerce analytics platform** that ingests raw clickstream events (page views, cart additions, purchases) and transforms them into business-ready datasets for BI consumption.

| Dimension | Detail |
|---|---|
| Data period | November 2019 — 5 weekly batches (W44–W48) |
| Data volume | ~5 million clickstream events |
| Batch cadence | Weekly (`schedule=None`, trigger manually or via Airflow) |
| Storage format | Delta Lake (ACID, time-travel, schema evolution) |
| Object storage | MinIO (S3-compatible) |
| Catalog | Hive Metastore |
| Query layer | Trino (federated SQL) |

---

## Architecture

```
Raw Data (CSV)
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│                    DATA LAKEHOUSE (MinIO + Delta Lake)   │
│                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐  │
│  │  BRONZE  │───▶│  SILVER  │───▶│       GOLD       │  │
│  │          │    │          │    │                  │  │
│  │ Raw JSON │    │ Cleaned  │    │ sales_trend      │  │
│  │ envelope │    │ typed    │    │ brand_preference │  │
│  │          │    │ deduplied│    │ cohort_retention │  │
│  │          │    │          │    │ rfm_segmentation │  │
│  └──────────┘    └──────────┘    └──────────────────┘  │
│                                                         │
│              PySpark ETL  ◀──  Hive Metastore (catalog) │
└─────────────────────────────────────────────────────────┘
     │                                       │
     │ Apache Airflow (orchestration)        │ Trino (query engine)
     │                                       │
     ▼                                       ▼
DAG: clickstream_lakehouse            Power BI / Tableau / SQL clients
```

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Compute Engine | Apache Spark (Standalone Cluster) | 3.5.8 |
| Table Format | Delta Lake | 3.2.0 |
| Object Storage | MinIO (S3-compatible) | Latest |
| Metadata Catalog | Apache Hive Metastore | 3.x |
| Orchestration | Apache Airflow | 3.0.2 |
| Query Engine | Trino | 438 |
| Metadata DB | PostgreSQL | 15 |
| Containerisation | Docker Compose | - |
| Language | Python / PySpark | 3.12 / 3.5.8 |

---

## Project Structure

```
click_stream_lakehouse/
│
├── data/                              # Raw source files (mounted read-only)
│   ├── 01-log-tracking.csv            # Clickstream events (view/cart/purchase)
│   └── 02-purchase-behavior.csv       # Enriched purchase data with cohort columns
│
├── ingestion/
│   ├── bronze/
│   │   ├── ingest_log_tracking.py     # Bronze ingestion — log tracking
│   │   └── ingest_purchase_behavior.py# Bronze ingestion — purchase behavior
│   ├── silver/
│   │   ├── clean_log_tracking.py      # Silver cleaning — log tracking
│   │   └── clean_purchase_behavior.py # Silver cleaning — purchase behavior
│   └── gold/
│       ├── gold_sales_trend.py        # Weekly sales aggregation
│       ├── gold_brand_preferences.py  # Brand performance by week
│       ├── gold_cohort_retention.py   # User cohort retention matrix
│       └── gold_rfm_segmentation.py   # RFM customer segmentation
│
├── dags/
│   └── clickstream_pipeline.py        # Airflow DAG (5 weekly TaskGroups)
│
├── docker/
│   ├── spark/
│   │   ├── Dockerfile                 # Spark image + Delta/Hadoop JARs
│   │   └── spark-defaults.conf        # Spark config (Delta, S3A, Hive, History)
│   ├── airflow/
│   │   └── Dockerfile                 # Airflow image + Java + PySpark + Delta JARs
│   ├── hive/
│   │   ├── Dockerfile                 # Hive image + S3A JARs
│   │   └── hive-site.xml              # Hive config (PostgreSQL metastore, S3A)
│   ├── trino/etc/
│   │   ├── config.properties
│   │   ├── jvm.config
│   │   ├── node.properties
│   │   └── catalog/delta.properties   # Trino Delta connector → Hive Metastore
│   └── postgres/
│       └── init.sql                   # Creates separate Airflow DB user
│
├── docker-compose.yml                 # Full stack definition
├── .env                               # Secrets & config (not committed)
├── .env.example                       # Template for .env
└── README.md
```

---

## Data Sources

### `01-log-tracking.csv` — Clickstream Events
Raw e-commerce interaction log from an online store.

| Column | Type | Description |
|---|---|---|
| `event_time` | Timestamp | When the event occurred |
| `event_type` | String | `view` / `cart` / `purchase` / `remove_from_cart` |
| `product_id` | Integer | Product identifier |
| `category_id` | String | Category identifier |
| `category_code` | String | Human-readable category path (e.g. `electronics.smartphone`) |
| `brand` | String | Product brand |
| `price` | Double | Product price (USD) |
| `user_id` | Integer | User identifier |
| `user_session` | String | Session UUID |

### `02-purchase-behavior.csv` — Enriched Purchase Data
Purchase events enriched with cohort analysis columns pre-computed upstream.

| Column | Type | Description |
|---|---|---|
| `user_id` | Integer | User identifier |
| `event_time` | Timestamp | Purchase timestamp |
| `event_type` | String | Always `purchase` |
| `product_id` | Integer | Product identifier |
| `price` | Double | Purchase price |
| `event_date` | Date | Date of event |
| `first_event_date` | Date | User's first ever event date |
| `start_of_week` | Date | Monday of the event week |
| `week_number` | Integer | ISO week number |
| `cohort_index_week` | String | Week label of user's cohort entry |
| `week_after` | Integer | Weeks since user first appeared (0, 1, 2, …) |

---

## Pipeline Design

### Bronze Layer

**Purpose:** Land raw data as-is into Delta Lake with a standardised envelope schema. No transformations — just source fidelity.

**Schema (both tables):**

| Column | Type | Description |
|---|---|---|
| `datasource` | String | Source name (`log_tracking` / `purchase_behavior`) |
| `ingesttime` | Timestamp | When the record was ingested |
| `content` | String | Full original row serialised as JSON string |

**Write mode:** `append` — each weekly batch adds new rows without overwriting history.

**Storage paths:**
- `s3a://lakehouse/bronze/log_tracking`
- `s3a://lakehouse/bronze/purchase_behavior`

---

### Silver Layer

**Purpose:** Parse, type-cast, clean, and deduplicate the raw JSON content from Bronze. Registers tables in Hive Metastore for downstream SQL access.

**Transformations applied:**
- `from_json()` to unpack `content` column with explicit `StructType` schema
- Filter by `event_time` within the weekly batch window
- `fillna()` — replace nulls: `brand → "unknown"`, `category_code → "unknown"`
- `filter(price > 0)` — remove invalid price records
- `dropDuplicates()` — remove exact duplicate rows

**Write mode:** `append` — weekly incremental loads.

**Hive tables registered:**
- `silver.log_tracking` → `s3a://lakehouse/silver/log_tracking`
- `silver.purchase_behavior` → `s3a://lakehouse/silver/purchase_behavior`

---

### Gold Layer

Business-level aggregation tables, directly queryable via Trino or BI tools.

#### `gold.sales_trend`

Weekly sales performance by event type.

| Column | Description |
|---|---|
| `week_start` | First day of the batch week |
| `event_date` | Calendar date |
| `event_type` | `view` / `cart` / `purchase` |
| `event_count` | Total events |
| `unique_users` | Distinct users |
| `revenue` | Total revenue (purchases only, rounded to 2dp) |

**Write mode:** `append`

---

#### `gold.brand_preferences`

Top brands by revenue per week.

| Column | Description |
|---|---|
| `week_start` | Batch week |
| `brand` | Brand name |
| `order_count` | Total purchase events |
| `unique_buyers` | Distinct purchasing users |
| `total_revenue` | Sum of purchase prices |
| `avg_price` | Average product price |

**Write mode:** `append`

---

#### `gold.cohort_retention`

Weekly cohort retention matrix — answers *"of users who first appeared in week X, what % were still active in week X+N?"*

| Column | Description |
|---|---|
| `cohort_index_week` | The cohort entry week label |
| `week_after` | 0 = acquisition week, 1 = week after, etc. |
| `retained_users` | Users active in this week_after |
| `cohort_users` | Total users in the original cohort |
| `retention_rate` | `retained_users / cohort_users * 100` |

**Write mode:** `overwrite` — recalculated from full Silver history each run.

---

#### `gold.rfm_segmentation`

RFM (Recency–Frequency–Monetary) customer segmentation using NTILE(5) window scoring.

| Column | Description |
|---|---|
| `user_id` | User identifier |
| `recency_days` | Days since last purchase |
| `frequency` | Total number of purchases |
| `monetary` | Total spend |
| `r_score` | Recency quintile (1–5, 5 = most recent) |
| `f_score` | Frequency quintile (1–5, 5 = most frequent) |
| `m_score` | Monetary quintile (1–5, 5 = highest spend) |
| `segment` | `Champions` / `Loyal` / `Potential` / `At Risk` / `Lost` |

**Segmentation rules:**

| Segment | Condition |
|---|---|
| Champions | R ≥ 4 AND F ≥ 4 |
| Loyal | R ≥ 3 AND F ≥ 3 |
| Potential | R ≥ 3 AND F < 3 |
| At Risk | R < 3 AND F ≥ 3 |
| Lost | Everything else |

**Write mode:** `overwrite` — full recalculation each run.

---

## Orchestration

The pipeline is orchestrated by **Apache Airflow 3.0.2** using a single DAG: `clickstream_lakehouse`.

### DAG Design

```
Week W44 ──▶ Week W45 ──▶ Week W46 ──▶ Week W47 ──▶ Week W48
```

Each week is a `TaskGroup` with 3 internal stages:

```
bronze (parallel)          silver (parallel)        gold (parallel)
┌─────────────────┐        ┌──────────────────┐     ┌──────────────────┐
│ ingest_log_     │        │ clean_log_        │     │ sales_trend      │
│ tracking        │──────▶ │ tracking          │──▶  │ brand_preference │
│                 │        │                   │     │ cohort_retention │
│ ingest_purchase │        │ clean_purchase_   │     │ rfm_segmentation │
│ _behavior       │        │ behavior          │     └──────────────────┘
└─────────────────┘        └──────────────────┘
```

- Bronze tasks within a week run **in parallel**
- Silver tasks run **after** both Bronze tasks complete
- Gold tasks run **after** both Silver tasks complete
- Weeks run **sequentially** (W44 → W45 → … → W48)

### Operator

`SparkSubmitOperator` with:
- `conn_id = spark_default` → `spark://spark-master:7077`
- `driver_memory = 1g`, `executor_memory = 2g`
- `executor_cores = 2`, `num_executors = 1`

### Weekly Batch Dates

| Week | Start Date |
|---|---|
| W44 | 2019-10-28 |
| W45 | 2019-11-04 |
| W46 | 2019-11-11 |
| W47 | 2019-11-18 |
| W48 | 2019-11-25 |

---

## Query Engine

**Trino 438** is configured with the `delta_lake` connector, enabling standard SQL queries directly over Delta tables registered in Hive Metastore.

### Sample Queries

```sql
-- Weekly revenue trend
SELECT week_start, SUM(revenue) AS total_revenue
FROM delta.gold.sales_trend
WHERE event_type = 'purchase'
GROUP BY week_start
ORDER BY week_start;

-- Top 10 brands by revenue
SELECT brand, SUM(total_revenue) AS revenue
FROM delta.gold.brand_preferences
GROUP BY brand
ORDER BY revenue DESC
LIMIT 10;

-- Cohort retention heatmap data
SELECT cohort_index_week, week_after, retention_rate
FROM delta.gold.cohort_retention
ORDER BY cohort_index_week, week_after;

-- Customer segments breakdown
SELECT segment, COUNT(*) AS users, ROUND(AVG(monetary), 2) AS avg_spend
FROM delta.gold.rfm_segmentation
GROUP BY segment
ORDER BY users DESC;
```

---

## Getting Started

### Prerequisites

- Docker Desktop (≥ 4.x) with at least **8 GB RAM** allocated
- Docker Compose v2
- Git

### 1. Clone the repository

```bash
git clone <repo-url>
cd click_stream_lakehouse
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in:
```env
AIRFLOW__CORE__FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
AIRFLOW__API_AUTH__JWT_SECRET=<same value as FERNET_KEY>
```

### 3. Build custom Docker images

```bash
docker compose build
```

> This downloads Delta Lake and Hadoop AWS JARs (~200 MB). Takes 5–10 minutes on first run.

### 4. Start all services

```bash
# Start infrastructure first
docker compose up postgres minio hive-metastore spark-master spark-worker-1 spark-worker-2 spark-history-server trino -d

# Initialise Airflow database and create admin user
docker compose up airflow-init

# Start Airflow services
docker compose up airflow-webserver airflow-scheduler airflow-dag-processor -d
```

### 5. Trigger the pipeline

**Via Airflow UI** (`http://localhost:8081`):
1. Login with `admin` / `admin`
2. Find DAG `clickstream_lakehouse`
3. Toggle ON → click ▶ **Trigger DAG**

**Via CLI:**
```bash
docker exec airflow-scheduler airflow dags trigger clickstream_lakehouse
```

### 6. Query results with Trino

```bash
docker exec -it trino trino --catalog delta --schema gold
```

```sql
SHOW TABLES;
SELECT * FROM sales_trend LIMIT 10;
```

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Spark Master UI | http://localhost:8080 | — |
| Spark History Server | http://localhost:18080 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Airflow UI | http://localhost:8081 | `admin` / `admin` |
| Trino UI | http://localhost:8082 | — |
| Hive Metastore (Thrift) | `thrift://localhost:9083` | — |
| MinIO S3 API | `http://localhost:9000` | — |

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `MINIO_ROOT_USER` | MinIO admin username | `minioadmin` |
| `MINIO_ROOT_PASSWORD` | MinIO admin password | `minioadmin` |
| `POSTGRES_USER` | PostgreSQL user for Hive Metastore | `hive` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `hive` |
| `POSTGRES_DB` | Hive Metastore database name | `metastore` |
| `AIRFLOW_DB_USER` | PostgreSQL user for Airflow | `airflow` |
| `AIRFLOW_DB_PASSWORD` | PostgreSQL password for Airflow | `airflow` |
| `AIRFLOW_DB_NAME` | Airflow database name | `airflow` |
| `AIRFLOW__CORE__FERNET_KEY` | Fernet key for encrypting Airflow secrets | *(required)* |
| `AIRFLOW__API_AUTH__JWT_SECRET` | JWT secret for Airflow 3.0 execution API | *(required)* |
| `AIRFLOW_ADMIN_USER` | Airflow web UI username | `admin` |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow web UI password | `admin` |
| `SPARK_MASTER_URL` | Spark master URL | `spark://spark-master:7077` |
| `SPARK_WORKER_CORES` | CPU cores per Spark worker | `2` |
| `SPARK_WORKER_MEMORY` | RAM per Spark worker | `3g` |

---

## Data Flow Summary

```
CSV files (./data/)
    │
    │  spark-submit (client mode, driver on Airflow container)
    ▼
Bronze Delta tables  ──  s3a://lakehouse/bronze/*
    │                    (datasource, ingesttime, content:JSON)
    │
    │  from_json() + fillna + filter + dropDuplicates
    ▼
Silver Delta tables  ──  s3a://lakehouse/silver/*
    │                    (typed columns, registered in Hive Metastore)
    │
    │  spark.sql() GROUP BY / WINDOW / JOIN aggregations
    ▼
Gold Delta tables    ──  s3a://lakehouse/gold/*
    │                    (sales_trend, brand_preferences,
    │                     cohort_retention, rfm_segmentation)
    │
    │  Trino delta connector ← Hive Metastore ← S3A → MinIO
    ▼
BI Tools (Power BI, Tableau, SQL clients)
```
