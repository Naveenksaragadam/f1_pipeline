import logging
import pendulum
from datetime import datetime
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

from f1_data.ingestion.ingestor import F1DataIngestor

logger = logging.getLogger(__name__)

def run_ingestion(**kwargs):
    """
    Orchestrates the ingestion using the new Config-Driven Engine.
    """
    # 1. Extract Context
    logical_date = kwargs["logical_date"]
    batch_id = kwargs["ts_nodash"]
    season_year = logical_date.year
    
    # 2. Determine "Force Refresh" Logic
    # If we are ingesting the CURRENT active season, data might change (penalties, new races).
    # We should force refresh to ensure we get the latest state.
    # For historical years (e.g. 2015), data never changes, so we use idempotent skip.
    current_year = pendulum.now().year
    should_force_refresh = (season_year == current_year)

    logger.info(
        f"🚀 Starting Job: Season {season_year} | Batch: {batch_id} | "
        f"Refresh: {should_force_refresh}"
    )

    # 3. Instantiate and Run
    ingestor = F1DataIngestor()
    
    # CHANGED: We now call the new master orchestrator method
    ingestor.run_full_extraction(
        season=season_year, 
        batch_id=batch_id,
        force_refresh=should_force_refresh
    )

with DAG(
    dag_id="f1_pipeline",
    # Start a bit earlier to capture history, or keep 2015 as per your preference
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), 
    schedule_interval="@yearly",
    catchup=True,
    max_active_runs=1, # Sequential execution prevents API rate limits
    tags=["bronze", "ingestion"]
) as dag:
    
    ingest_task = PythonOperator(
        task_id="raw_ingest",
        python_callable=run_ingestion,
    )