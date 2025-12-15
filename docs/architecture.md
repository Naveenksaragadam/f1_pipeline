# F1 Pipeline Architecture

## 1. Project Overview

The **F1 Data Pipeline** is a fully open-source ELT pipeline that extracts, processes, and visualizes historical and live Formula 1 data from the [Ergast API](http://ergast.com/mrd/).

The pipeline follows a **medallion architecture** (Bronze → Silver → Gold) to ensure **reproducibility, auditability, and incremental processing**. The architecture demonstrates **real-world data engineering practices**, including orchestration, incremental loads, CDC/SCD2, observability, and analytics-ready data.

---

## 2. Tech Stack

| Layer                              | Tool                      | Role                                                                        |
| ---------------------------------- | ------------------------- | --------------------------------------------------------------------------- |
| **Orchestration**                  | Airflow                   | Schedules and manages ELT DAGs, handles retries, SLA, and logging           |
| **Raw & Parquet Storage (Bronze)** | MinIO                     | Stores immutable raw JSON and converted Parquet files from Ergast API       |
| **Staging (Silver)**               | ClickHouse staging tables | Flatten, clean, and validate Parquet data                                   |
| **Transformation & Gold Layer**    | dbt Core + ClickHouse     | Curated dimension & fact tables, incremental loads, SCD2 snapshots          |
| **BI / Analytics**                 | Superset / Metabase       | Interactive dashboards and KPI visualizations                               |
| **Observability**                  | Prometheus + Grafana      | Monitors Airflow DAGs and ClickHouse query performance; dashboards & alerts |

---

## 3. Architecture Diagram

```
              ┌─────────────────────────┐
              │       Ergast API        │
              │  (F1 race data source) │
              └─────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────┐
        │       Airflow (ELT Orchestration)     │
        │ - DAG scheduling / retries / SLA      │
        │ - Orchestrates extraction, staging,  │
        │   and dbt transformations             │
        └───────────────────────────────────────┘
                           │
                           ▼
        ┌─────────────────────────────────────────────┐
        │             Bronze Layer                     │
        │  MinIO (S3-compatible object storage)       │
        │  - Raw JSON from Ergast API                 │
        │  - Converted Parquet for efficient storage │
        │  - Immutable backup for replay             │
        └─────────────────────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────┐
        │             Silver Layer               │
        │  ClickHouse staging tables            │
        │  - Flatten & clean Parquet data       │
        │  - Validate schemas / row hashes      │
        │  - Prepare for transformations        │
        └───────────────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────┐
        │              Gold Layer                │
        │  ClickHouse final tables (facts + dims)│
        │  - dbt transformations (incremental)  │
        │  - dbt snapshots for SCDs             │
        │  - Partitioned & indexed for analytics│
        │  - Aggregates for BI dashboards       │
        └───────────────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────┐
        │      BI / Visualization Layer          │
        │  Superset / Metabase dashboards        │
        │  - Fast queries for KPIs               │
        │  - Charts: fastest laps, pit efficiency│
        └───────────────────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────┐
        │          Observability Layer           │
        │  Prometheus + Grafana                  │
        │  - Airflow DAG metrics (success/failure│
        │    rates, SLA compliance)             │
        │  - ClickHouse query performance       │
        │  - Alerts / dashboards for health     │
        └───────────────────────────────────────┘
```

---

## 4. Data Flow & Processing

1. **Extraction (Bronze Layer)**

   * Airflow DAG fetches race, driver, and constructor data from Ergast API.
   * Raw JSON is stored in **MinIO**, organized by season/round.
   * JSON is **converted to Parquet** for efficient storage and faster downstream processing.
   * Immutable storage allows **replay, backfill, and reproducibility**.

2. **Staging (Silver Layer)**

   * Parquet files are loaded into **ClickHouse staging tables**.
   * Data is flattened, validated, and cleaned; row hashes are computed for CDC.

3. **Transformation (Gold Layer)**

   * dbt Core transforms staging tables into **facts and dimensions**.
   * Incremental models ensure **efficient updates**.
   * Snapshots implement **SCD2** for historical tracking of drivers/constructors.
   * Partitioning by season/round improves query performance.

4. **Analytics & BI**

   * Superset/Metabase dashboards query gold tables.
   * Pre-built KPIs include **fastest laps, pit efficiency, driver standings, and constructor stats**.

5. **Observability & Monitoring**

   * Prometheus collects Airflow and ClickHouse metrics.
   * Grafana dashboards visualize DAG health, query latency, and SLA compliance.
   * Alerts notify failures or anomalies in the pipeline.

---

## 5. Design Rationale

| Question                        | Answer                                                                                                                                                 |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Why Medallion Architecture?** | Separates raw, staging, and curated data; enables reproducibility, auditability, and incremental processing.                                           |
| **Why ELT over ETL?**           | Raw data is first loaded into Bronze → transformations are applied in Gold, leveraging ClickHouse compute power and maintaining raw data for backfill. |
| **Why Parquet after JSON?**     | Parquet provides efficient columnar storage, faster query performance, and schema enforcement while preserving raw JSON for replay/backfill.           |
| **Why ClickHouse?**             | Columnar OLAP engine for high-performance analytics; supports partitions, incremental loads, and sub-second aggregation queries.                       |
| **Why MinIO?**                  | Open-source object storage for raw and Parquet data; S3-compatible and cost-effective.                                                                 |
| **Why dbt Core?**               | Modular, testable, version-controlled transformations; supports incremental updates and SCD snapshots.                                                 |
| **Why Observability?**          | Prometheus + Grafana monitor pipeline health and performance; enables SLA alerts for production-grade reliability.                                     |

---

## 6. Batch & Incremental Strategy

* **Extraction**: Batched per race or season.
* **Conversion**: JSON → Parquet for efficient storage & processing.
* **Incremental updates**: dbt incremental models detect new/updated data.
* **CDC / SCD2**: Snapshots track dimension changes like driver transfers or constructor updates.
* **Replay & Backfill**: Bronze layer allows reprocessing any historical batch without recomputation.

---

## 7. Key Learning

* Demonstrates **end-to-end data engineering skills**: extraction, storage, transformation, analytics.
* Implements **incremental loads, CDC/SCD**, and **reproducible pipelines**.
* Showcases **observability & monitoring**, a key production skill.
* Fully **open-source**, Dockerized, and portable — ready for portfolio or interview demo.