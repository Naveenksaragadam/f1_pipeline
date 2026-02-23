# dags/ingestion_dag.py
"""
F1 Data Pipeline DAG - Production Grade
Orchestrates the full lifecycle from raw API ingestion (Bronze)
to validated, flattened Parquet transformation (Silver).
"""

import logging
from typing import Any

import pendulum
from airflow import DAG  # type: ignore
from airflow.exceptions import AirflowException  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from cosmos import (  # type: ignore
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)

from f1_pipeline.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_BRONZE,
    MINIO_BUCKET_SILVER,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)
from f1_pipeline.ingestion.ingestor import F1DataIngestor
from f1_pipeline.minio.object_store import F1ObjectStore
from f1_pipeline.transform.base import F1Transformer
from f1_pipeline.transform.factory import TRANSFORM_FACTORY

DBT_PROJECT_PATH = "/opt/airflow/f1_dbt"

logger = logging.getLogger(__name__)


def run_ingestion(**kwargs: Any) -> None:
    """
    Orchestrates the extraction of raw F1 data from the Jolpica API.

    Strategy:
    - Historical seasons (< current year): Skip existing files (Idempotent).
    - Current season: Always overwrite to capture post-race changes (Force Refresh).

    Args:
        **kwargs: Standard Airflow context (logical_date, ts_nodash, etc.).

    Raises:
        AirflowException: Re-raised to trigger Airflow retry/failure mechanisms.
    """
    try:
        logical_date = kwargs["logical_date"]
        batch_id = kwargs["ts_nodash"]
        season_year = logical_date.year

        current_year = pendulum.now().year
        should_force_refresh = season_year == current_year
        refresh_mode = "FORCE" if should_force_refresh else "SKIP_EXISTING"

        logger.info(
            f"\n{'=' * 70}\n"
            f"🏎️  STARTING BRONZE INGESTION\n"
            f"   Season: {season_year} | Mode: {refresh_mode}\n"
            f"{'=' * 70}"
        )

        ingestor = F1DataIngestor(validate_connection=True)
        summary = ingestor.run_full_extraction(
            season=season_year, batch_id=batch_id, force_refresh=should_force_refresh
        )

        logger.info(f"✅ Ingestion successful. Written: {summary['files_written']}")
        kwargs["ti"].xcom_push(key="ingestion_stats", value=summary)

        if summary["errors_encountered"] > 0:
            raise AirflowException(f"Ingestion had {summary['errors_encountered']} errors.")

    except Exception as e:
        logger.error(f"❌ Ingestion task failed: {e}", exc_info=True)
        raise AirflowException(f"F1 Ingestion failed: {e}") from e


def run_transformation(**kwargs: Any) -> None:
    """
    Orchestrates the promotion of data from Bronze to Silver layer.

    Workflow:
    - Scans the Bronze bucket for the specific season's JSON files.
    - Reference endpoints (seasons, status) have no season partition so they
      are scanned from the root prefix.
    - Applies factory-driven schemas per endpoint.
    - Validates, flattens, and writes to Silver as Parquet.
    - Per-file failures are isolated so a single bad object doesn't abort
      the entire season's transformation.

    Args:
        **kwargs: Standard Airflow context.

    Raises:
        AirflowException: Triggers Airflow retry logic if transformation fails.
    """
    # Endpoints whose Bronze files are not partitioned by season (global reference data)
    GLOBAL_ENDPOINTS = {"seasons", "status"}

    try:
        logical_date = kwargs["logical_date"]
        season_year = logical_date.year

        logger.info(
            f"\n{'=' * 70}\n🧱 STARTING SILVER TRANSFORMATION\n   Season: {season_year}\n{'=' * 70}"
        )

        # Storage Initialization
        bronze_store = F1ObjectStore(
            bucket_name=MINIO_BUCKET_BRONZE,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
        )
        silver_store = F1ObjectStore(
            bucket_name=MINIO_BUCKET_SILVER,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
        )
        silver_store.create_bucket_if_not_exists()

        total_success = 0
        total_errors = 0

        # Dynamic Endpoint Processing via TRANSFORM_FACTORY
        for endpoint, schema in TRANSFORM_FACTORY.items():
            logger.info(f"🔄 Processing endpoint: '{endpoint}'...")

            transformer = F1Transformer(
                bronze_store=bronze_store, silver_store=silver_store, schema_class=schema
            )

            # Route to correct Bronze prefix — global endpoints have no season partition
            if endpoint in GLOBAL_ENDPOINTS:
                prefix = f"ergast/endpoint={endpoint}/"
            else:
                prefix = f"ergast/endpoint={endpoint}/season={season_year}/"

            source_objects = bronze_store.list_objects(prefix=prefix)

            if not source_objects:
                logger.warning(f"  ⚠️ No Bronze data found for '{endpoint}' (prefix: {prefix})")
                continue

            logger.info(f"  📂 Found {len(source_objects)} objects to transform.")

            ep_success = 0
            ep_errors = 0
            for source_key in source_objects:
                try:
                    target_key = source_key.replace(".json", ".parquet")
                    transformer.process_object(source_key, target_key)
                    ep_success += 1
                except Exception as e:
                    ep_errors += 1
                    logger.error(f"  ❌ Failed to transform '{source_key}': {e}")

            total_success += ep_success
            total_errors += ep_errors
            logger.info(f"  ✅ '{endpoint}' done — {ep_success} written, {ep_errors} errors.")

        logger.info(
            f"\n{'=' * 70}\n"
            f"🏁 Silver Transformation Complete.\n"
            f"   Files written : {total_success}\n"
            f"   Files failed  : {total_errors}\n"
            f"{'=' * 70}"
        )

        if total_errors > 0:
            raise AirflowException(
                f"Silver transformation completed with {total_errors} file-level errors. "
                "Check task logs for details."
            )

    except AirflowException:
        raise
    except Exception as e:
        logger.error(f"❌ Transformation task failed: {e}", exc_info=True)
        raise AirflowException(f"F1 Transformation failed: {e}") from e


# --- DAG DEFINITION ---

default_args = {
    "owner": "f1-data-labs",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=10),
}

with DAG(
    dag_id="f1_production_pipeline",
    description="Unified F1 Data Pipeline: Bronze (JSON) -> Silver (Parquet)",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@yearly",
    catchup=True,
    max_active_runs=1,
    tags=["f1", "production", "polars", "pydantic"],
    doc_md="""
    # F1 Production Data Pipeline

    This DAG manages the full transformation lifecycle for Formula 1 data.

    ## Architecture
    1. **Bronze Layer (`extract_season_data`)**: Retrieves raw JSON from the Ergast API and stores it in MinIO.
       - Uses a `FORCE_REFRESH` policy for the current season.
    2. **Silver Layer (`transform_season_data`)**: Processes raw JSON into cleaned, flattened Parquet files.
       - Uses `Pydantic` for schema enforcement.
       - Uses `Polars` for efficient transformation and recursive flattening.

    ## Monitoring & Maintenance
    - **Logs**: Detailed execution summaries are logged in the task outputs.
    - **Backfills**: Can be re-run for any season between 2024 and 2026.
    """,
) as dag:
    ingest_task = PythonOperator(
        task_id="extract_season_data",
        python_callable=run_ingestion,
    )

    transform_task = PythonOperator(
        task_id="transform_season_data",
        python_callable=run_transformation,
    )

    # --- Gold Layer (dbt) via Astronomer Cosmos ---
    gold_layer = DbtTaskGroup(
        group_id="gold_layer_dbt",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="f1_dbt",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
        ),
        render_config=RenderConfig(
            select=["path:models/gold"],  # Only run gold models for now
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
    )

    # Dependency Flow: Bronze -> Silver -> Gold
    ingest_task >> transform_task >> gold_layer
