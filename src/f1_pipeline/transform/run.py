"""
F1 Silver Layer Backfill Utility
Standalone maintenance script to transform full season data 
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

    # 1. Initialize Object Stores
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

    # Define endpoints that do not use season-level partitioning in Bronze
    GLOBAL_ENDPOINTS = ["seasons", "status"]

    # 2. Sequential Processing
    for endpoint, schema in TRANSFORM_FACTORY.items():
        logger.info(f"📁 Starting transformation for endpoint: {endpoint.upper()}")

        # Dependency Injection
        transformer = F1Transformer(
            bronze_store=bronze_store, silver_store=silver_store, schema_class=schema
        )

        # Route to correct Bronze prefix
        if endpoint in GLOBAL_ENDPOINTS:
            prefix = f"ergast/endpoint={endpoint}/"
        else:
            prefix = f"ergast/endpoint={endpoint}/season={season}/"

        try:
            # 3. Discovery
            source_objects = bronze_store.list_objects(prefix=prefix)

            if not source_objects:
                logger.warning(f"  ⚠️ No raw data found for prefix: {prefix}")
                continue

            logger.info(f"  📂 Found {len(source_objects)} source objects.")

            # 4. Transformation
            success_count = 0
            for source_key in source_objects:
                try:
                    # Deterministic mapping: replace extension to signal data promotion
                    target_key = source_key.replace(".json", ".parquet")
                    transformer.process_object(source_key, target_key)
                    success_count += 1
                except Exception as e:
                    logger.error(f"  ❌ Failed {source_key}: {e}")

            logger.info(
                f"  ✅ Completed {endpoint}: {success_count}/{len(source_objects)} synced."
            )

        except Exception as e:
            logger.error(f"  ❌ Critical failure for {endpoint}: {e}")

    logger.info("✨ Backfill operation completed successfully!")


if __name__ == "__main__":
    run_backfill(2024)
