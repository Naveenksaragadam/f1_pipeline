# src/F1_PPIPELINE/ingestion/ingestor.py
"""
F1 Data Ingestor - Production-Grade Implementation
Handles API extraction with retry logic, rate limiting, and idempotent writes.
"""
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
    RETRY_MAX_ATTEMPTS,
    RETRY_MIN_WAIT,
    RETRY_MAX_WAIT,
)
from f1_data.minio.object_store import F1ObjectStore


# initializing logger
logger = logging.getLogger(__name__)

# Define what errors are worth retrying
# We retry on RequestException (Network errors), but NOT on ValueError (Bad JSON)
RETRY_STRATEGY = retry(
    stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),  # Give up after 5 tries
    wait=wait_exponential(min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),  # Sleep 1s, then 2s, then 4s...
    retry=retry_if_exception_type(RequestException),
    reraise=True,
    before_sleep=before_sleep_log(logger, logging.WARNING),
)


class F1DataIngestor:
    """
    Production-grade data ingestor for F1 Ergast API.
    
    Features:
    - Idempotent writes (skip existing files by default)
    - Force refresh for current season data
    - Automatic pagination handling
    - Comprehensive error handling and retry logic
    - Detailed stats tracking
    """

    def __init__(
            self, 
            base_url: str = BASE_URL, 
            session=None,
            validate_connection: bool = True
        ) -> None:
        """
        Initialize the ingestor with HTTP session and object store.
        
        Args:
            base_url: API base URL
            session: Optional pre-configured requests session
            validate_connection: If True, validate MinIO connection on init
        """
        self.base_url = base_url
        self.session = session or create_session()

        # Initialize object store
        self.store = F1ObjectStore(
            bucket_name=MINIO_BUCKET_BRONZE,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
        )

        # Validate connection if requested
        if validate_connection:
            self._validate_infrastructure()
        
        # Initialize stats tracking
        self._reset_stats()

    
        # Track ingestion stats
        self.stats = {
            "files_written": 0,
            "files_skipped": 0,
            "api_calls_made": 0,
            "bytes_written": 0,
        }

    def _validate_infrastructure(self) -> None:
        """Validates that MinIO is accessible and bucket exists."""
        try:
            logger.info("🔍 Validating infrastructure...")
            self.store.create_bucket_if_not_exists()
            logger.info("✅ Infrastructure validation successful")
        except Exception as e:
            logger.error(f"❌ Infrastructure validation failed: {e}")
            raise RuntimeError(
                "Cannot connect to MinIO. Ensure docker-compose services are running."
            ) from e
    
    def _reset_stats(self) -> None:
        """Reset statistics counters."""
        self.stats = {
            "files_written": 0,
            "files_skipped": 0,
            "api_calls_made": 0,
            "bytes_written": 0,
            "errors_encountered": 0,
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
        Generate Hive-style partitioned path for Bronze layer.

        Format:
            file_pattern = (
                "bronze/ergast/endpoint={name}/"
                "[season={YYYY}/]"
                "[round={RR}/]"
                "batch_id={timestamp}/"
                "page_{NNN}.json"
            )
        Args:
            endpoint: API endpoint name
            batch_id: Unique batch identifier
            season: Optional season year
            round_num: Optional round number
            page: Page number for pagination
            
        Returns:
            S3 object key path
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

    def _save_to_minio(
            self, 
            data: Dict[str, Any], 
            path: str, 
            metadata: Dict[str, Any]
        ) -> None:
        """
        Save data to MinIO with metadata envelope.

        Args:
            data: API response data
            path: S3 object key
            metadata: Ingestion metadata
        """
        # 1. Create the Envelope
        envelope = {
            "metadata": metadata,
            "data": data,  # The actual API response lives here
        }

        try:
            self.store.put_object(path, envelope)
            self.stats["files_written"] += 1
            logger.info(f"✅ Saved {path}")
        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"❌ Failed to save {path}: {e}")
            raise

    @RETRY_STRATEGY
    def fetch_page(
        self, 
        url: str, 
        limit: int, 
        offset: int
    ) -> Dict[str, Any]:
        """
        Fetch a single page from the API with automatic retries.
        
        Args:
            url: API endpoint URL
            limit: Page size limit
            offset: Pagination offset
            
        Returns:
            JSON response as dictionary
            
        Raises:
            RequestException: On network/HTTP errors
            ValueError: On invalid JSON response
        """
        # params dict
        params = {"limit": limit, "offset": offset}

        try:
            logger.debug(f"📡 GET {url} | limit={limit} offset={offset}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            self.stats["api_calls_made"] += 1
            data = response.json()
            
            logger.info(f"✅ API call successful | Status: {response.status_code}")
            return data

        except RequestException as e:
            logger.error(f"❌ API request failed: {e}")
            self.stats["errors_encountered"] += 1
            raise

        except ValueError as e:  # This catches JSON decoding errors
            logger.error(f"❌ Invalid JSON response: {e}")
            self.stats["errors_encountered"] += 1
            raise

    def ingest_endpoint(
        self,
        endpoint_name: str,
        batch_id: str,
        season: int | None = None,
        round_num: str | None = None,
        force_refresh: bool = False,
    ) -> None:
        """
        Generic Engine: Handles URL building, Pagination, and Saving. 
        Ingest data from a specific API endpoint with pagination support

        Args:
            endpoint_name: API endpoint identifier (must exist in ENDPOINT_CONFIG)
            batch_id: Unique batch identifier for this ingestion run
            season: Optional season filter (e.g., 2024)
            round_num: Optional round filter (e.g., "5")
            force_refresh: If True, re-fetch even if files exist
            
        Raises:
            ValueError: If endpoint_name not found in config
            Exception: On API or storage failures
        """
        # Validate endpoint config exists
        config = ENDPOINT_CONFIG.get(endpoint_name)
        if not config:
            raise ValueError(
                f"Unknown endpoint '{endpoint_name}'. "
                f"Valid endpoints: {list(ENDPOINT_CONFIG.keys())}"
            )

        # 1. Build URL
        url_pattern = config["url_pattern"]
        # Handle formatting safely (ignore missing keys if template doesn't use them)
        url_path = url_pattern.format(season=season or "", round=round_num or "")
        full_url = f"{self.base_url}/{url_path}"

        # 2. Pagination Setup
        page = 1
        offset = 0
        limit = DEFAULT_LIMIT
        # If config says no pagination, we treat it as single page (total=limit)
        is_paginated = config.get("pagination", True)

        # force refresh for current season as the source data may change (penalties)
        refresh_mode = "FORCE_REFRESH" if force_refresh else "IDEMPOTENT"

        logger.info(
            f"🚀 Starting: {endpoint_name} | "
            f"Season: {season} | Round: {round_num} | "
            f"Mode: {refresh_mode}"
        )

        # Pagination loop
        while True:
            try:
                # 1. Generate Path first (we don't need data for this)
                s3_key = self._generate_path(
                    endpoint_name, batch_id, season, round_num, page
                )

                # 2. Check if it exists (Optimistic Skip)
                # If we have the file, we assume we should just check the next page.
                if not force_refresh and self.store.object_exists(s3_key):
                    logger.warning(f"⏭️  Skipping {s3_key} (already exists)")
                    self.stats["files_skipped"] += 1 
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
                    "round": round_num,
                    "page": page,
                    "source_url": f"{full_url}?limit={limit}&offset={offset}",
                    "api_response_total": total_records,
                    "file_version": "1.0",
                    "force_refresh": force_refresh,
                }

                # 6. Save to MinIO
                self._save_to_minio(response_data, s3_key, metadata)
                logger.info(f"✅ Saved {s3_key}")

                # 7. Break conditions
                # We stop if it's not paginated OR if we have fetched the last items
                if not is_paginated or (offset + limit >= total_records):
                    logger.info(
                        f"✅ Completed {endpoint_name} | "
                        f"Total records: {total_records} | "
                        f"Pages: {page}"
                    )
                    break

                # Next Page
                offset += limit
                page += 1

            except Exception as e:
                logger.error(
                    f"❌ Failed processing {endpoint_name} "
                    f"page {page}: {e}"
                )

    def run_full_extraction(
        self, 
        season: int, 
        batch_id: str, 
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Orchestrate full season extraction following dependency graph.
        
        Extraction order:
        1. Season-level data (constructors, drivers, races)
        2. Race calendar parsing
        3. Race-level data (results, qualifying, laps, pitstops, sprint)
        4. Standings (driver, constructor)
        
        Args:
            season: F1 season year (e.g., 2024)
            batch_id: Unique batch identifier
            force_refresh: If True, re-fetch all data
            
        Returns:
            Dictionary containing extraction statistics
        """
        logger.info(
            f"\n{'=' * 70}\n"
            f"📦 F1 Data Extraction Started\n"
            f"   Season: {season}\n"
            f"   Batch ID: {batch_id}\n"
            f"   Force Refresh: {force_refresh}\n"
            f"{'=' * 70}\n"
        )

        self._reset_stats()
        start_time = datetime.now()

        try:
            # Step 1: Season-Level Reference (Constructors, Drivers)
            # Note: We skip 'Static' here (Seasons/Circuits) as they are usually separate
            logger.info("\n--- Phase 1: Season-Level Data ---")
            for endpoint in ["constructors", "drivers", "races"]:
                self.ingest_endpoint(
                    endpoint, 
                    batch_id, 
                    season=season, 
                    force_refresh=force_refresh
                )

            # Step 2: Get the Calendar to determine Rounds
            # We assume we just ingested 'races' (Schedule), so we can fetch it or just re-fetch quickly.
            # For simplicity in this function, we fetch the schedule again to parse it.
            logger.info("\n--- Phase 2: Race Calendar ---")
            schedule_url = f"{self.base_url}/{season}.json"
            schedule_data = self.fetch_page(schedule_url, limit=DEFAULT_LIMIT, offset=0)
            races_list = schedule_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

            total_rounds = len(races_list)
            logger.info(f"📅 Found {len(races_list)} rounds for season {season}")

            if total_rounds == 0:
                logger.warning(f"⚠️  No races found for season {season}")
                return self._generate_summary(start_time)


            # Step 3: Loop Rounds
            logger.info("\n--- Phase 3: Race-Level Data ---")
            for idx, race in enumerate(races_list, 1):
                round_num = race["round"]
                race_name = race.get("raceName", "Unknown")

                logger.info(
                    f"\n   📍 [{idx}/{total_rounds}] Round {round_num}: {race_name}"
                )

                for endpoint in ["results", "qualifying", "laps", "pitstops", "sprint"]:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round_num=round_num,
                        force_refresh=force_refresh,
                    )

                # Group 4: Standings
                for endpoint in ["driverstandings", "constructorstandings"]:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round_num=round_num,
                        force_refresh=force_refresh,
                    )
            return self._generate_summary(start_time)
        
        except Exception as e:
            logger.error(f"\n❌ Extraction FAILED for season {season}: {e}")
            raise
        
    def _generate_summary(self, start_time: datetime) -> Dict[str, Any]:
        """Generate extraction summary with statistics."""
        duration = (datetime.now() - start_time).total_seconds()
        
        summary = {
            "status": "SUCCESS" if self.stats["errors_encountered"] == 0 else "PARTIAL",
            "duration_seconds": round(duration, 2),
            **self.stats
        }
        
        logger.info(
            f"\n{'=' * 70}\n"
            f"✅ Extraction Complete\n"
            f"   Duration: {duration:.2f}s\n"
            f"   Files Written: {self.stats['files_written']}\n"
            f"   Files Skipped: {self.stats['files_skipped']}\n"
            f"   API Calls: {self.stats['api_calls_made']}\n"
            f"   Errors: {self.stats['errors_encountered']}\n"
            f"{'=' * 70}\n"
        )
        
        return summary