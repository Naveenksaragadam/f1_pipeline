import logging
import pendulum
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

logger = logging.getLogger(__name__)


from f1_data.ingestion.ingestor import F1DataIngestor

def run_ingestion(**kwargs):
    # Get the "Logical Date" (The date Airflow is simulating)
    logical_date = kwargs["logical_date"]
    
    # Extract the season dynamically
    season_year = logical_date.year
    
    # Get a unique Batch ID (Timestamp with no dashes)
    batch_id = kwargs["ts_nodash"]
    
    logger.info(f"DEBUG: Ingesting Season {season_year} | Batch: {batch_id}")

    # 4. Instantiate and run
    ingestor = F1DataIngestor()
    # Pass BOTH arguments to your ingestor
    ingestor.ingest_season(season_year, batch_id)

with DAG(
    dag_id="f1_pipeline",
    start_date=pendulum.datetime(2015, 1, 1, tz="UTC"),
    schedule_interval="@yearly",
    catchup=True,
    max_active_runs=1,
    tags=["raw", "ingestion"]
) as dag:
    
    ingest_task = PythonOperator(
        task_id="raw_ingest",
        python_callable=run_ingestion,
        
    )

