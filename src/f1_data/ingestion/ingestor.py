# src/F1_PPIPELINE/ingestion/ingestor.py
import logging
from requests import RequestException
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Internal Imports
from .http_client import create_session
from .config import (
    BASE_URL,
    DEFAULT_LIMIT,
    MINIO_BUCKET_BRONZE,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    ENDPOINT_CONFIG,  # The new config dict
)
from f1_data.minio.object_store import F1ObjectStore


# initializing logger
logger = logging.getLogger(__name__)

# Define what errors are worth retrying
# We retry on RequestException (Network errors), but NOT on ValueError (Bad JSON)
RETRY_STRATEGY = retry(
    stop=stop_after_attempt(5),  # Give up after 5 tries
    wait=wait_exponential(min=2, max=30),  # Sleep 1s, then 2s, then 4s...
    retry=retry_if_exception_type(RequestException),
    reraise=True,
    before_sleep=before_sleep_log(logger, logging.WARNING),
)


class F1DataIngestor:
    def __init__(self, base_url: str = BASE_URL, session=None) -> None:
        self.base_url = base_url
        self.session = session or create_session()
        self.store = F1ObjectStore(
            bucket_name=MINIO_BUCKET_BRONZE,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
        )
        # Track ingestion stats
        self.stats = {
            "files_written": 0,
            "files_skipped": 0,
            "api_calls_made": 0,
            "bytes_written": 0,
        }

    def _generate_path(
        self,
        endpoint: str,
        batch_id: str,
        season: Optional[int],
        round: Optional[str],
        page: int,
    ) -> str:
        """
        Implements the Hive-Style Partitioning Strategy.
        Format:
            file_pattern = (
                "bronze/ergast/endpoint={name}/"
                "[season={YYYY}/]"
                "[round={RR}/]"
                "batch_id={timestamp}/"
                "page_{NNN}.json"
            )
        """
        path_parts = ["ergast", f"endpoint={endpoint}"]

        if season:
            path_parts.append(f"season={season}")
        if round:
            path_parts.append(f"round={int(round):02d}")

        path_parts.append(f"batch_id={batch_id}")

        # Filename (Paginated vs Single)
        filename = f"page_{page:03}.json"

        return "/".join(path_parts) + "/" + filename

    def _save_to_minio(self, data: Dict, path: str, metadata: Dict) -> None:
        """Writes the JSON data to MinIO."""
        # 1. Create the Envelope
        envelope = {
            "metadata": metadata,
            "data": data,  # The actual API response lives here
        }

        self.store.put_object(path, envelope)
        self.stats["files_written"] += 1

    @RETRY_STRATEGY
    def fetch_page(self, url: str, limit: int, offset: int) -> Dict:
        """Fetches a single page using the session and params dict."""
        # params dict
        params = {"limit": limit, "offset": offset}

        try:
            logger.debug(f"GET {url} {params}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            self.stats["api_calls_made"] += 1
            logger.info("API response recieved sucessfully!!!")
            return response.json()

        except RequestException as e:
            logger.error(f"An error occured: {e}")
            raise

        except ValueError:  # This catches JSON decoding errors
            logger.error("Error: The response was not valid JSON.")
            raise

    def ingest_endpoint(
        self,
        endpoint_name: str,
        batch_id: str,
        season: int | None = None,
        round: str | None = None,
        force_refresh: bool = False,
    ) -> None:
        """
        Generic Engine: Handles URL building, Pagination, and Saving.

        Args:
            endpoint_name: API endpoint to fetch
            batch_id: Deterministic batch identifier
            season: Optional season filter
            round: Optional round filter
            force_refresh: If True, re-fetch even if file exists
                          If False, skip existing files (idempotent)
        """
        config = ENDPOINT_CONFIG.get(endpoint_name)
        if not config:
            logger.error(f"❌ Config not found for {endpoint_name}")
            return

        # 1. Build URL
        url_template = config["url_template"]
        # Handle formatting safely (ignore missing keys if template doesn't use them)
        url_path = url_template.format(season=season, round=round)
        full_url = f"{self.base_url}/{url_path}"

        # 2. Pagination Loop
        page = 1
        offset = 0
        limit = DEFAULT_LIMIT
        # If config says no pagination, we treat it as single page (total=limit)
        is_paginated = config.get("pagination", True)
        # force refresh for current season as the source data may change (penalties)
        refresh_mode = "FORCE_REFRESH" if force_refresh else "IDEMPOTENT"

        logger.info(f"🚀 Ingesting {endpoint_name} | Season: {season} | Round: {round}")

        while True:
            try:
                # 1. Generate Path first (we don't need data for this)
                s3_key = self._generate_path(
                    endpoint_name, batch_id, season, round, page
                )

                # 2. Check if it exists (Optimistic Skip)
                # If we have the file, we assume we should just check the next page.
                if not force_refresh and self.store.object_exists(s3_key):
                    logger.warning(f"⏭️  Skipping {s3_key} (Already exists)")
                    offset += limit
                    page += 1
                    continue

                # ---------------------------------------------------------
                # 3. Fetch (Only if file is missing)
                # ---------------------------------------------------------
                logger.info(f"📥 Fetching URL: {full_url} (Offset: {offset})")
                response_data = self.fetch_page(full_url, limit, offset)

                # 4. Extract Total Records (The Source of Truth)
                mr_data = response_data.get("MRData", {})
                total_records = int(mr_data.get("total", 0))

                # 5. Build Metadata
                metadata = {
                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                    "batch_id": batch_id,
                    "endpoint": endpoint_name,
                    "season": season,
                    "round": round,
                    "page": page,
                    "source_url": f"{full_url}?limit={limit}&offset={offset}",
                    "api_response_total": total_records,
                    "file_version": "1.0",
                    "force_refresh": force_refresh,
                }

                # 6. Save
                self._save_to_minio(response_data, s3_key, metadata)
                logger.info(f"✅ Saved {s3_key}")

                # 7. Break conditions
                # We stop if it's not paginated OR if we have fetched the last items
                if not is_paginated or (offset + limit >= total_records):
                    logger.info(
                        f"✅ Finished {endpoint_name}. Total records: {total_records}"
                    )
                    break

                # Next Page
                offset += limit
                page += 1

            except Exception as e:
                logger.error(f"❌ Failed processing {endpoint_name} page {page}: {e}")
                raise

    def run_full_extraction(
        self, season: int, batch_id: str, force_refresh: bool = False
    ):
        """
        Orchestrates the dependency graph for a given season.

        Args:
            season: F1 season year (e.g., 2023)
            batch_id: Deterministic batch identifier (from Airflow or manual)
            force_refresh: If True, re-fetch all data regardless of existence
        """
        logger.info(
            f"\n{'=' * 70}\n"
            f"📦 Starting Extraction\n"
            f"   Season: {season}\n"
            f"   Batch ID: {batch_id}\n"
            f"   Force Refresh: {force_refresh}\n"
            f"{'=' * 70}\n"
        )

        self.stats = {
            "files_written": 0,
            "files_skipped": 0,
            "api_calls_made": 0,
            "bytes_written": 0,
        }
        start_time = datetime.now()

        try:
            # Step 1: Season-Level Reference (Constructors, Drivers)
            # Note: We skip 'Static' here (Seasons/Circuits) as they are usually separate
            logger.info("\n--- Step 1: Season-Level Data ---")
            for endpoint in ["constructors", "drivers", "races"]:
                self.ingest_endpoint(
                    endpoint, batch_id, season=season, force_refresh=force_refresh
                )

            # Step 2: Get the Calendar to determine Rounds
            # We assume we just ingested 'races' (Schedule), so we can fetch it or just re-fetch quickly.
            # For simplicity in this function, we fetch the schedule again to parse it.
            logger.info("\n--- Step 2: Fetching Race Calendar ---")
            schedule_url = f"{self.base_url}/{season}.json"
            schedule_data = self.fetch_page(schedule_url, limit=DEFAULT_LIMIT, offset=0)
            races_list = schedule_data["MRData"]["RaceTable"]["Races"]

            total_rounds = len(races_list)
            logger.info(f"📅 Found {len(races_list)} rounds for {season}")

            # Step 3: Loop Rounds
            logger.info("\n--- Step 3: Processing Race Data ---")
            for idx, race in enumerate(races_list, 1):
                round_num = race["round"]
                race_name = race.get("raceName", "Unknown")

                logger.info(
                    f"\n   [{idx}/{total_rounds}] Round {round_num}: {race_name}"
                )

                for endpoint in ["results", "qualifying", "laps", "pitstops", "sprint"]:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round=round_num,
                        force_refresh=force_refresh,
                    )

                # Group 4: Standings
                for endpoint in ["driverstandings", "constructorstandings"]:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round=round_num,
                        force_refresh=force_refresh,
                    )
            # Final stats
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"\n{'=' * 70}\n"
                f"✅ Batch {batch_id} Complete!\n"
                f"   Duration: {duration:.2f}s\n"
                f"   Files Written: {self.stats['files_written']}\n"
                f"   Files Skipped: {self.stats['files_skipped']}\n"
                f"   API Calls: {self.stats['api_calls_made']}\n"
                f"{'=' * 70}\n"
            )
        except Exception as e:
            logger.error(f"\n❌ Batch {batch_id} FAILED: {e}")
            raise
