# src/F1_PPIPELINE/ingestion/ingestor.py
import logging
from typing import Dict
from requests import RequestException
from http_client import create_session
from object_store import F1ObjectStore
from config import BASE_URL, DEFAULT_LIMIT, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

logger = logging.getLogger(__name__)

class F1DataIngestor:
    def __init__(self, base_url: str = BASE_URL, bucket: str = MINIO_BUCKET, session=None) -> None:
        self.base_url = base_url
        self.session = session or create_session()
        self.bucket = bucket
        self.store = F1ObjectStore(
            bucket_name=self.bucket,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY
        )
        self.store.create_bucket_if_not_exists()
    
    
    def _save_page_minio(self, data: Dict, season: int, page: int ) -> None: # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        """Writes the JSON data to MinIO."""
        key = f"data/season={season}/page_{page:03}.json"
        self.store.put_object(key,data)
        
    
    def fetch_page(self, season: int, limit: int = 30, offset: int = 0) -> Dict: # type: ignore
        """Fetches a single page using the session and params dict."""
        endpoint = f"{self.base_url}/{season}/results.json"
        
        # TODO: Define params dict
        params = {
            "limit":limit,
            "offset":offset
        }
        
        try:
            response = self.session.get(endpoint,params=params)
            response.raise_for_status()
            logger.info("API response recieved sucessfully!!!")
            return response.json()
            
        except RequestException as e:
            logger.error(f"An error occured: {e}")
            raise

        except ValueError: # This catches JSON decoding errors
            logger.error("Error: The response was not valid JSON.")
            raise
        
    def ingest_season(self, season: int) -> None:
        """Orchestrates the loop."""
        logger.info(f"Starting ingestion for Season {season}...")
        
        # 1. Fetch first page to get metadata (Total/Pages)
        response = self.fetch_page(season, offset=0)
        self._save_page_minio(response,season,1) # Save Page 1
        
        # 2. Calculate total pages
        limit = DEFAULT_LIMIT
        total_results = int(response['MRData']['total'])
        total_pages = (total_results + limit - 1) // limit # handiling edge cases for total_pages

        logger.info(f"Total pages to ingest: {total_pages}")
        # 3. Loop through remaining pages
        # We handle offset calculation dynamically: (page - 1) * 30
        for page in range(2,total_pages+1):
            offset = (page - 1) * limit
            logger.info(f"Fetching page {page} (Offset {offset})...")

            response = self.fetch_page(season,limit,offset)
            self._save_page_minio(response,season,page)
    