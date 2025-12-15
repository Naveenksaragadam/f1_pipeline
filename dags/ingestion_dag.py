from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

from f1_data.ingestion.ingestor import F1DataIngestor

def run_ingestion(season_year, **kwargs):
    # Instantiate the ingestor
    # It will automatically pick up env vars from docker-compose
    ingestor = F1DataIngestor()
    ingestor.ingest_season(season_year)

with DAG(
    dag_id="f1_raw_ingestion",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["raw", "ingestion"]
) as dag:
    
    ingest_task = PythonOperator(
        task_id="ingest_2024",
        python_callable=run_ingestion,
        op_kwargs={"season_year": 2024}
    )

