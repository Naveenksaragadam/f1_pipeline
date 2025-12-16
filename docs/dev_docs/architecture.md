# F1 Data Pipeline - Production Architecture 

## 1. Project GoalDesign and implement a production-grade, open-source data platform that ingests Formula 1 data from the Ergast API and serves it for analytical use cases.

This project demonstrates:

* **Medallion Architecture:** Bronze / Silver / Gold 


* **Batch ELT pipelines:** With orchestration 


* **Schema evolution & CDC:** SCD Type 2 


* **Cloud-native storage patterns:** S3-compatible 


* **Observability:** Production readiness 


* **FAANG-level system design trade-offs** 



---

## 2. High-Level Architecture 

## Flow Overview

1. **Source**
   - Ergast API (13 endpoints)

2. **Airflow DAG**
   - **Task 1: Extract (Raw API Ingestion)**
     - Writes raw JSON to **Bronze**
   - **Task 2: Transform (Schema Validation + Parquet)**
     - Writes typed Parquet to **Silver**
   - **Task 3: Load (ClickHouse Ingest)**
     - External S3-backed tables

3. **MinIO (S3-Compatible Storage)**
   - `bronze/`
     - Raw JSON (immutable, append-only)
   - `silver/`
     - Parquet (cleaned, typed, validated)
   - `gold/`
     - Optional materialized outputs

4. **ClickHouse**
   - Staging tables (external S3 → Silver Parquet)
   - dbt snapshots (CDC / SCD Type 2)
   - Gold models (facts & dimensions)





---

3. Technology Stack (OSS-Only) 

| Layer | Tool | Why |
| --- | --- | --- |
| **Orchestration** | Apache Airflow | Industry standard, DAG-based control |
| **Storage** | MinIO | S3-compatible, local-first, portable |
| **Transform** | Python + Pandas / PyArrow | Schema control & Parquet conversion |
| **Warehouse** | ClickHouse | High-performance OLAP, S3-native |
| **Modeling** | dbt Core | Analytics engineering best practices |
| **Observability** | Prometheus + Grafana | Metrics, alerts, visibility |
| **BI** | Apache Superset / Metabase | Open-source analytics |

---

## 4. Medallion Architecture (Strict Implementation) 

Bronze Layer - Immutable Raw Storage 

* **Purpose:** Preserve raw source data exactly as received for auditing, replay, and disaster recovery.


* **Storage:** MinIO (`bronze` bucket).


* **Format:** JSON with metadata envelope.


* **Characteristics:**
* **Immutable:** Files are never modified or deleted.


* **Versioned:** Each extraction creates a new `batch_id`.


* **Append-only:** Enables safe backfills and reprocessing.


* **Schema-free:** Accepts any JSON structure.


* **No transformations:** Data stored exactly as received from API.





**Path Convention (Hive-style partitioning):**


`bronze/ergast/endpoint={endpoint_name}/[season={YYYY}/][round={RR}/]batch_id={timestamp}/page_{NNN}.json | data.json` 

**Examples:**

* **Static:** `bronze/ergast/endpoint=circuits/batch_id=20241216T120000/data.json` 


* **Season-level:** `bronze/ergast/endpoint=drivers/season=2024/batch_id=20241216T120000/data.json` 


* **Race-level (paginated):** `bronze/ergast/endpoint=results/season=2024/round=05/batch_id=20241216T120000/page_001.json` 


* **Race-level (single):** `bronze/ergast/endpoint=qualifying/season=2024/round=05/batch_id=20241216T120000/data.json` 



**Metadata Envelope Structure:**

```json
{
  "metadata": {
    "ingestion_timestamp": "2024-12-16T12:00:00Z",
    "batch_id": "20241216T120000",
    "endpoint": "results",
    "season": 2024,
    "round": 5,
    "page": 1,
    "total_pages": 3,
    "source_url": "https://api.jolpi.ca/ergast/f1/2024/5/results.json?limit=30&offset=0",
    "api_response_total": 90,
    "file_version": "1.0"
  },
  "data": {
    "MRData": { /* actual API response */ }
  }
}

```



**Recovery Strategy:** If downstream logic breaks, we reprocess from Bronze. No transformations allowed here .

Silver Layer - Clean & Typed Data 

* **Purpose:** Standardize, validate, and structure raw data for analytical consumption.


* **Storage:** MinIO (`silver` bucket).


* **Format:** Parquet (columnar, compressed).


* **Transformations Applied:** 


1. **Schema Validation:** Pydantic models enforce structure.
2. **Type Casting:** Strings → dates, integers, floats.
3. **Flattening:** Nested JSON → flat columns.
4. **Null Handling:** Standardize missing values.
5. **Deduplication:** Remove duplicate rows based on natural keys.
6. **Page Consolidation:** Merge paginated JSON into single Parquet file.



**Flow:** Bronze JSON → Python Transform Parquet → Silver Bucket.

**Path Convention:**


`silver/ergast/endpoint={endpoint_name}/[season={YYYY}/][round={RR}/]batch_id={timestamp}/data.parquet` .

**Why Parquet in Silver?** 

* **Columnar compression:** 10x storage reduction vs JSON.
* **Schema enforcement:** Type safety at storage level.
* **Optimized for ClickHouse reads:** Predicate pushdown, column pruning.
* **Easy reprocessing from Bronze:** Fast conversion pipeline.

Gold Layer - Analytics-Ready Data 

* **Purpose:** Business-ready facts and dimensions optimized for BI and analytics.


* **Location:** ClickHouse (managed by dbt).



**Models:** 

1. **Dimension Tables (SCD Type 2):**
   - `dim_drivers` (driver_id, name, nationality, dob, valid_from, valid_to)
   - `dim_constructors`
   - `dim_circuits`

2. **Static Dimensions:**
   - `dim_seasons`
   - `dim_status`

3. **Fact Tables:**
   - `fact_race_results`: Result data (points, position, status)
   - `fact_qualifying_results`: Q1/Q2/Q3 times
   - `fact_lap_times`: Lap-level telemetry
   - `fact_pit_stops`: Pit stop duration and timing

4. **Aggregate Tables:**
   - `agg_driver_season_stats`
   - `agg_constructor_season_stats`
   - `agg_fastest_laps_by_circuit`



**Optimizations:** 

* **Partitioning:** `PARTITION BY (season, round)` for query pruning.
* **Primary Keys:** `ORDER BY (season, round, driver_id)` for data locality.
* **Materialized Views:** Pre-aggregated KPIs for sub-second dashboard queries.
* **Skip Indexes:** On dates and categorical fields for faster filtering.

---

## 5. ClickHouse Ingestion Strategy 

**What NOT to Do:** Do NOT have ClickHouse read JSON from Bronze layer .

**Correct Flow:** Silver Parquet (MinIO) → ClickHouse External Tables → dbt Transformations .

**Option 1: S3 Table Engine (Batch Loading)** 

```sql
CREATE TABLE stg_results
ENGINE = S3(
    'http://minio:9000/silver/ergast/endpoint=results/season=2024/*.parquet',
    'minioadmin',
    'password',
    'Parquet'
)
SETTINGS input_format_parquet_allow_missing_columns = 1;

```

**Option 2: S3Queue (Incremental Ingestion)** 

```sql
CREATE TABLE stg_results_queue
ENGINE = S3Queue(
    'http://minio:9000/silver/ergast/endpoint=results/*/*/*.parquet',
    'minioadmin',
    'password',
    'Parquet'
)
SETTINGS
    mode = 'ordered',
    after_processing = 'keep',
    keeper_path = '/clickhouse/s3queue/results';

```

**Why This Approach?** 

* Avoid expensive JSON parsing in ClickHouse.
* Keep ClickHouse purely analytical (no ETL overhead).
* Decouple compute from storage.
* Enable independent scaling of ingestion and querying.

---

## 6. CDC & SCD Type 2 (Correct Placement) 

**What NOT to Do:** Applying snapshots in Gold models or tracking history at the fact table level .

**Correct Approach:** dbt snapshots on Silver staging tables .

**Why?** 

* Captures source-level changes (driver transfers, team rebrands).
* Preserves historical truth before aggregation.
* Gold models can safely aggregate/filter without losing history.
* Enables time-travel queries ("Who drove for Ferrari in 2019?").

**Example Snapshot Target:** 

```yaml
# snapshots/snap_drivers.sql
{% snapshot snap_drivers %}
{{
    config(
        target_schema='snapshots',
        unique_key='driver_id',
        strategy='check',
        check_cols=['name', 'nationality', 'constructor_id']
    )
}}
SELECT * FROM {{ ref('stg_drivers') }}
{% endsnapshot %}

```

**Output table structure:** `snap_drivers` contains `dbt_valid_from` (When this version became active) and `dbt_valid_to` (When this version was superseded; NULL=current) .

---

## 7. Orchestration (Airflow DAG Design) 

### DAG Structure

1. **Extract Task:** Calls Ergast API, writes raw JSON to Bronze with metadata, handles pagination .


2. **Transform Task:** Reads Bronze JSON, validates schema (Pydantic), consolidates pages, writes Parquet to Silver .


3. **Load Task:** Triggers ClickHouse ingestion from Silver, executes dbt run/snapshot/test, validates data quality .



Task Dependencies 

```python
with DAG("f1_pipeline"):
    extract_static >> extract_season >> extract_races
    extract_races >> extract_race_data >> extract_standings
    extract_standings >> transform_to_silver
    transform_to_silver >> load_to_clickhouse
    load_to_clickhouse >> dbt_run >> dbt_test

```

**Why Airflow?** 

* Clear task boundaries with explicit dependencies.
* Retry & failure handling with exponential backoff.
* Backfills & flexible scheduling (yearly, daily, weekly).
* Industry standard for batch pipelines.
* Built-in monitoring and alerting.

---

## 8. Observability & Reliability 

**Metrics (Prometheus):** 

* `airflow_dag_status`: DAG success/failure rates.
* `airflow_task_duration_seconds`: Task execution times.
* `minio_s3_requests_total`: Data volume per batch.
* `clickhouse_query_duration_seconds`: Query latency (p50, p95, p99).
* `api_rate_limit_remaining`: Ergast API quota tracking.

**Dashboards (Grafana):** 

* **Pipeline Health:** SLA compliance, failure alerts.
* **Throughput Trends:** Data ingested over time.
* **Query Performance:** ClickHouse latency heatmaps.
* **Storage Metrics:** Bronze/Silver layer growth.

**Logging:** 

* Structured logs per task with correlation IDs.
* Failure root-cause analysis with stack traces.
* Audit trail for data lineage and compliance.

---

## 9. Data Volume & Batch Strategy 

**Historical Backfill:**

* One-time batch: Seasons 1950 → Present (75+ years) .


* Chunked by season & round.


* Estimated: ~15,000 races, ~500K results, ~5M lap records.



**Incremental Loads:**

* Latest season scheduled daily during race season (March-November) .


* Idempotent writes (skip if `batch_id` exists).


* Focus: Latest round + standings updates.



**Why Batching?** 

* Easier recovery from failures.
* Realistic production behavior.
* Matches OLAP workloads (query historical trends).
* Aligns with F1 race cadence (weekly/bi-weekly events).

---

## 10. Design Trade-Offs (Interview-Ready) 

| Decision | Why | Trade-Off |
| --- | --- | --- |
| **ELT over ETL** | ClickHouse excels at transformations; keep raw data | Requires robust warehouse |
| **ClickHouse over Postgres** | OLAP performance & scale (100x faster aggregations) | Less mature tooling vs Postgres |
| **MinIO over local FS** | Cloud-native, portable, S3-compatible | Manual HA setup vs managed S3 |
| **dbt snapshots** | Industry-standard CDC, version-controlled | Additional complexity vs triggers |
| **Parquet in Silver** | Performance & schema safety | More complex than CSV/JSON |
| **No Spark** | Dataset fits single-node OLAP (<1TB) | Limits scalability to ~10M rows/table |
| **Batch over streaming** | Matches F1 race cadence (not real-time) | 1-hour lag on results |
| **Airflow over Prefect** | Industry standard, mature ecosystem | Steeper learning curve |

---

11. What This Project Demonstrates 

* **Real-world data platform design:** End-to-end ELT pipeline.


* **OSS-first engineering mindset:** No vendor lock-in, fully portable.


* **Clear separation of concerns:** Bronze (raw) → Silver (typed) → Gold (analytics).


* **Reproducibility & scalability:** Backfills, incremental loads, idempotency.


* **FAANG-level architectural reasoning:** Trade-offs, observability, testing.


* **Production best practices:** Retry logic, CDC, data quality, monitoring.



---

## 12. API Endpoint Coverage 

| Category | Endpoints | Partitioning | Pagination |
| --- | --- | --- | --- |
| **Static Reference** | seasons, circuits, status | None | No |
| **Season-Level** | constructors, drivers, races | `season={YYYY}` | No |
| **Race-Level** | results, qualifying, sprint, pitstops, laps | `season={YYYY}/round={RR}` | Yes |
| **Standings** | driverstandings, constructorstandings | `season={YYYY}/round={RR}` | No |

**Extraction Priority:** 

1. Static reference data.
2. Season-level data.
3. Race-level data.
4. Standings.

---

## 13. Next Steps 

**Phase 1: Core Implementation** 

* Update ingestor.py to support all 13 endpoints.
* Create endpoint configuration file with URL templates.
* Implement Bronze → Silver transformation with Pydantic schemas.
* Build Airflow DAGs (backfill + incremental).

**Phase 2: Warehouse & Modeling** 

* Define ClickHouse table schemas.
* Create dbt models (staging, gold, snapshots).
* Implement SCD Type 2 for drivers/constructors.
* Add dbt tests for data quality.

**Phase 3: Observability** 

* Set up Prometheus metrics collection.
* Build Grafana dashboards.
* Configure alerting rules.
* Add structured logging.

**Phase 4: Analytics & BI** 

* Deploy Apache Superset.
* Create dashboard templates.
* Optimize query performance.
* Enable self-service analytics.

---

## 14. Getting Started 

```bash
# 1. Clone repository
git clone https://github.com/yourusername/f1_pipeline.git
cd f1_pipeline

# 2. Configure environment
cp .env.example .env
# Edit .env with your MinIO credentials

# 3. Start infrastructure
docker-compose up -d

# 4. Access services
# Airflow: http://localhost:8080 (airflow/airflow)
# MinIO: http://localhost:9001 (minioadmin/password)

# 5. Trigger backfill DAG
airflow dags trigger f1_backfill_pipeline

```



---

## 15. References 

* Ergast API Documentation 


* ClickHouse S3 Integration 


* dbt Best Practices 


* Medallion Architecture