
import logging
import os
import clickhouse_connect
from typing import Optional

from ..config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT,
    CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB,
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_SILVER, MINIO_REGION
)
from f1_data.minio.object_store import F1ObjectStore

logger = logging.getLogger(__name__)

class ClickHouseLoader:
    """
    Manages loading of Silver Parquet data into ClickHouse Gold layer.
    """
    
    def __init__(self):
        """Initialize ClickHouse connection."""
        try:
            self.client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DB
            )
            # Ensure DB exists (though DDL script handles this too, good to double check)
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
            raise

    def setup_schema(self) -> None:
        """Execute the DDL script to create tables."""
        ddl_path = os.path.join(os.path.dirname(__file__), 'ddl.sql')
        
        try:
            with open(ddl_path, 'r') as f:
                ddl_statements = f.read().split(';')
                
            for statement in ddl_statements:
                if statement.strip():
                    logger.info(f"📜 Executing DDL: {statement[:50]}...")
                    self.client.command(statement)
                    
            logger.info("✅ ClickHouse Schema initialized.")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup schema: {e}")
            raise

    def ingest_batch(self, endpoint: str, season: int, batch_id: str) -> None:
        """
        Ingest a specific parquet batch from Silver MinIO into ClickHouse.
        Using S3 table function for direct high-speed ingestion.
        """
        table_name = endpoint  # Table names match endpoints (drivers, races, etc.)
        
        # Internal Docker URL for MinIO (Accessible from ClickHouse container)
        # Note: In config.py MINIO_ENDPOINT is strictly for Python access.
        # ClickHouse needs access via internal docker network alias 'minio'.
        # Assuming standard docker-compose setup where service is named 'minio'.
        minio_internal_host = os.getenv("MINIO_HOST_INTERNAL", "minio") 
        minio_port = os.getenv("MINIO_PORT_INTERNAL", "9000")
        
        s3_url = f"http://{minio_internal_host}:{minio_port}/{MINIO_BUCKET_SILVER}/{endpoint}/season={season}/{batch_id}.parquet"
        
        logger.info(f"🚜 Loading {endpoint} (Season {season}) into ClickHouse...")
        
        # INSERT INTO targets explicitly to avoid schema mismatch issues if possible
        # But SELECT * FROM s3 is standard for matching schemas.
        # Format: Parquet
        
        query = f"""
        INSERT INTO {CLICKHOUSE_DB}.{table_name}
        SELECT * FROM s3(
            '{s3_url}',
            '{MINIO_ACCESS_KEY}',
            '{MINIO_SECRET_KEY}',
            'Parquet'
        )
        """
        
        try:
            self.client.command(query)
            logger.info(f"✅ Loaded {endpoint} into {table_name}")
        except Exception as e:
            logger.error(f"❌ Failed to load {endpoint} from {s3_url}: {e}")
            raise
