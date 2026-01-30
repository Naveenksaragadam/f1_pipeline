# dags/ingestion_dag.py
"""
F1 Data Ingestion DAG - Production Configuration
Orchestrates yearly extraction with automatic force_refresh for current season.
"""
import logging
import pendulum
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.exceptions import AirflowException # type: ignore

from f1_pipeline.ingestion.ingestor import F1DataIngestor
from f1_pipeline.config import ENDPOINT_CONFIG

logger = logging.getLogger(__name__)


def run_ingestion(**kwargs) -> None:
    """
    Orchestrate F1 data ingestion with intelligent refresh strategy.
    
    Strategy:
    - Historical seasons (< current year): Idempotent (skip existing files)
    - Current season: Force refresh (data may change due to penalties)
    
    Args:
        **kwargs: Airflow context parameters
        
    Raises:
        AirflowException: On ingestion failures
    """
    try:
        # Extract Airflow context
        logical_date = kwargs["logical_date"]
        batch_id = kwargs["ts_nodash"]
        season_year = logical_date.year
    
        # Determine refresh strategy
        # Current season data may change (penalties, new races)
        # Historical data is stable and can be safely skipped
        current_year = pendulum.now().year
        should_force_refresh = (season_year == current_year)
        
        # Refresh mode for logging
        refresh_mode = "FORCE_REFRESH" if should_force_refresh else "IDEMPOTENT"

        logger.info(
            f"\n{'=' * 70}\n"
            f"🏎️  F1 INGESTION DAG\n"
            f"   Season: {season_year}\n"
            f"   Batch ID: {batch_id}\n"
            f"   Current Year: {current_year}\n"
            f"   Force Refresh: {should_force_refresh} ({refresh_mode})\n"
            f"   Reason: {'Active season - data may change' if should_force_refresh else 'Historical data - stable'}\n"
            f"{'=' * 70}"
        )

        # Initialize ingestor with connection validation
        ingestor = F1DataIngestor(validate_connection=True)
    
        # Run full extraction
        summary = ingestor.run_full_extraction(
            season=season_year,
            batch_id=batch_id,
            force_refresh=should_force_refresh
        )
        
        # Log summary
        logger.info(
            f"\n{'=' * 70}\n"
            f"✅ INGESTION COMPLETE\n"
            f"   Status: {summary['status']}\n"
            f"   Duration: {summary['duration_seconds']}s\n"
            f"   Files Written: {summary['files_written']}\n"
            f"   Files Skipped: {summary['files_skipped']}\n"
            f"   API Calls: {summary['api_calls_made']}\n"
            f"   Errors: {summary['errors_encountered']}\n"
            f"{'=' * 70}"
        )

        # Push stats to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='ingestion_stats', value=summary)
        
        # Fail the task if there were errors
        if summary['errors_encountered'] > 0:
            raise AirflowException(
                f"Ingestion completed with {summary['errors_encountered']} errors. "
                "Check logs for details."
            )
            
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}", exc_info=True)
        raise AirflowException(f"F1 Ingestion failed: {e}") from e


# DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id="f1_pipeline",
    description="Extract F1 data from Jolpica API to Bronze layer (MinIO)",
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), 
    schedule_interval="@yearly",
    catchup=True,
    max_active_runs=1,  # Sequential execution prevents API rate limits
    tags=["f1", "bronze", "ingestion", "jolpica"],
    doc_md="""
    # F1 Bronze Layer Ingestion
    
    Extracts Formula 1 data from the Jolpica API and stores raw JSON in the Bronze layer.
    
    ## Extraction Strategy
    - **Historical Seasons**: Idempotent (skips existing files)
    - **Current Season**: Force refresh (penalties/updates may occur)
    
    ## Data Coverage
    - Seasons: 2023 → Present
    - Endpoints: 13 (constructors, drivers, races, results, qualifying, etc.)
    - Storage: MinIO `bronze` bucket with Hive-style partitioning
    
    ## Dependencies
    - MinIO (S3-compatible storage)
    - Jolpica API (https://api.jolpi.ca/ergast/f1)
    
    ## Monitoring
    - Check task logs for ingestion statistics
    - XCom key `ingestion_stats` contains summary metrics
    
    ## Error Handling
    - Automatic retries on transient failures
    - Rate limiting to respect API quotas
    - Detailed error logging with stack traces
    """,
) as dag:
    
    ingest_task = PythonOperator(
        task_id="extract_season_data",
        python_callable=run_ingestion,
        provide_context=True,
    )

    ingest_task