# F1 Gold Layer (dbt)

This directory contains the dbt project for the Formula 1 data platform. It is responsible for transforming the Silver Layer (Parquet files in MinIO) into analytical Gold Layer models in ClickHouse.

## 🚀 Orchestration

In this project, dbt is **not** run as a standalone container. Instead, it is orchestrated by **Apache Airflow 3.x** using **Astronomer Cosmos**.

* **Granular Visibility**: Each dbt model is rendered as a separate task in the Airflow UI.
* **Native Execution**: Tasks run within the Airflow worker context, eliminating the need for a separate dbt runner service.

## 🏗️ Project Structure

* `models/bronze`: Staging models that interface with MinIO via ClickHouse S3 engine.
* `models/silver`: Intermediate models for deduplication and cleaning.
* `models/gold`: Final Fact and Dimension tables for Dashboards.

## 🛠️ Usage

While Airflow handles production runs, you can still develop locally:

```bash
# Inside f1_dbt directory
dbt run --select models/gold
dbt test
```

## 🔐 Credentials

Credentials are managed via `profiles.yml` which uses environment variables passed from Docker Compose/Airflow.
