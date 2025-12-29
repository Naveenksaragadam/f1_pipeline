# src/f1_data/scripts/init_storage.py
import logging
from f1_data.minio.object_store import F1ObjectStore
from f1_data.ingestion.config import (
    MINIO_BUCKET_BRONZE, 
    MINIO_BUCKET_SILVER, 
    MINIO_BUCKET_GOLD,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_buckets():
    """
    Idempotent initialization of all required buckets.
    """
    buckets_to_create = [
        MINIO_BUCKET_BRONZE,
        MINIO_BUCKET_SILVER,
        MINIO_BUCKET_GOLD
    ]

    # We reuse your existing class!
    base_store = F1ObjectStore(
        bucket_name="base", # Dummy name, we just want the client
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )
    ## Extract the client to reuse it
    shared_client = base_store.client

    logger.info("🚀 Starting Storage Initialization...")
    
    for bucket in buckets_to_create:
        try:
            # We need to tweak F1ObjectStore to allow checking *any* bucket, 
            # or just instantiate a new store for each bucket loop.
            # Easiest way with your current class:
            current_store = F1ObjectStore(
                bucket_name=bucket,
                endpoint_url=MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                client=shared_client
            )
            current_store.create_bucket_if_not_exists()
        except Exception as e:
            logger.error(f"❌ Failed to initialize bucket {bucket}: {e}")
            raise # Fail hard so we know infrastructure is broken

    logger.info("✅ Storage Initialization Complete.")

if __name__ == "__main__":
    # initialize_buckets()
    # print(MINIO_ENDPOINT)
    store = F1ObjectStore(
        bucket_name=MINIO_BUCKET_BRONZE,
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )
    store.create_bucket_if_not_exists()
    objects = store.list_objects('ergast/endpoint=constructors')
    object = objects[0]
    print(object)
    print(store.get_json(object))
    