"""
F1 Silver Layer Backfill Utility
Standalone maintenance script to transform a full season of data 
from the Bronze (raw) layer to the Silver (clean) layer.
"""

import logging

from f1_pipeline.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_BRONZE,
    MINIO_BUCKET_SILVER,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)
from f1_pipeline.minio.object_store import F1ObjectStore
from f1_pipeline.transform.base import F1Transformer
from f1_pipeline.transform.factory import TRANSFORM_FACTORY

# Standardize Logging Configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("f1_backfiller")


def run_backfill(season: int = 2024) -> None:
    """
    Run the transformation pipeline for all supported endpoints for a given season.

    Args:
        season: The year (e.g., 2024) to process from Bronze to Silver.
    """
    logger.info(f"🚀 Starting Silver Layer Backfill for Season {season}...")

    # 1. Initialize Object Stores with Production Config
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

    # Ensure target environment is ready
    silver_store.create_bucket_if_not_exists()

    # 2. Sequential Processing of Endpoints
    # We use the factory to iterate through all registered entities (drivers, results, etc.)
    for endpoint, schema in TRANSFORM_FACTORY.items():
        logger.info(f"📁 Processing endpoint: {endpoint}")

        # Dependency Injection: Feed the specific schema into the transformer
        transformer = F1Transformer(
            bronze_store=bronze_store, silver_store=silver_store, schema_class=schema
        )

        # Standard partitioning: ergast/endpoint={endpoint}/season={season}/
        prefix = f"ergast/endpoint={endpoint}/season={season}/"

        try:
            # 3. Discovery Phase
            source_objects = bronze_store.list_objects(prefix=prefix)

            if not source_objects:
                logger.warning(f"  ⚠️ No raw data found for prefix: {prefix}")
                continue

            logger.info(f"  📂 Found {len(source_objects)} objects to process.")

            # 4. Transformation Phase
            success_count = 0
            for source_key in source_objects:
                try:
                    # Deterministic mapping: replace extension to signal data promotion
                    target_key = source_key.replace(".json", ".parquet")
                    transformer.process_object(source_key, target_key)
                    success_count += 1
                except Exception as e:
                    logger.error(f"  ❌ Transformation failed for {source_key}: {e}")

            logger.info(
                f"  ✅ Completed {endpoint}: {success_count}/{len(source_objects)} files synced."
            )

        except Exception as e:
            logger.error(f"  ❌ Critical error while processing {endpoint}: {e}")

    logger.info("✨ Backfill synchronized successfully!")


if __name__ == "__main__":
    # Default to 2024 for the current project phase
    run_backfill(2024)
