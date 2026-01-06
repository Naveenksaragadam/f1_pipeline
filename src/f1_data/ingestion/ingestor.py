# src/f1_data/ingestion/ingestor.py
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
    ENDPOINT_CONFIG,
    RETRY_MAX_ATTEMPTS,
    RETRY_MIN_WAIT,
    RETRY_MAX_WAIT,
)
from f1_data.minio.object_store import F1ObjectStore


logger = logging.getLogger(__name__)

# Define retry strategy for transient errors
RETRY_STRATEGY = retry(
    stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
    wait=wait_exponential(min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
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

        # Initialize stats tracking
        self._reset_stats()

        # Validate connection if requested
        if validate_connection:
            self._validate_infrastructure()

    def _validate_infrastructure(self) -> None:
        """
        Validate that MinIO is accessible and bucket exists.
        
        Raises:
            RuntimeError: If MinIO connection or bucket creation fails
        """
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
        """Reset statistics counters to initial state."""
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
        round_num: Optional[int],
        page: int,
    ) -> str:
        """
        Generate Hive-style partitioned path for Bronze layer.

        Format:
            bronze/ergast/endpoint={name}/
            [season={YYYY}/]
            [round={RR}/]
            batch_id={timestamp}/
            page_{NNN}.json

        Args:
            endpoint: API endpoint name
            batch_id: Unique batch identifier
            season: Optional season year
            round_num: Optional round number (converted to int)
            page: Page number for pagination
            
        Returns:
            S3 object key path
            
        Example:
            _generate_path("results", "20241216T120000", 2024, 5, 1)
            -> "ergast/endpoint=results/season=2024/round=05/batch_id=20241216T120000/page_001.json"
        """
        path_parts = ["ergast", f"endpoint={endpoint}"]

        if season:
            path_parts.append(f"season={season}")
        if round_num:
            path_parts.append(f"round={int(round_num):02d}")

        path_parts.append(f"batch_id={batch_id}")

        # Filename (paginated format)
        filename = f"page_{page:03d}.json"

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
            
        Raises:
            Exception: On S3/MinIO storage failures
        """
        envelope = {
            "metadata": metadata,
            "data": data,
        }

        try:
            self.store.put_object(path, envelope)
            self.stats["files_written"] += 1
            logger.info(f"✅ Saved: {path}")
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
            RequestException: On network/HTTP errors after retries
            ValueError: On invalid JSON response
        """
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

        except ValueError as e:
            logger.error(f"❌ Invalid JSON response: {e}")
            self.stats["errors_encountered"] += 1
            raise

    def ingest_endpoint(
        self,
        endpoint_name: str,
        batch_id: str,
        season: Optional[int] = None,
        round_num: Optional[int] = None,
        force_refresh: bool = False,
    ) -> None:
        """
        Ingest data from a specific API endpoint with pagination support.

        Args:
            endpoint_name: API endpoint identifier (must exist in ENDPOINT_CONFIG)
            batch_id: Unique batch identifier for this ingestion run
            season: Optional season filter (e.g., 2024)
            round_num: Optional round filter (e.g., 5)
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

        # Build URL
        url_pattern = config["url_pattern"]
        url_path = url_pattern.format(season=season or "", round=round_num or "")
        full_url = f"{self.base_url}/{url_path}"

        # Pagination setup
        page = 1
        offset = 0
        limit = DEFAULT_LIMIT
        is_paginated = config.get("pagination", True)

        refresh_mode = "FORCE_REFRESH" if force_refresh else "IDEMPOTENT"

        logger.info(
            f"🚀 Starting: {endpoint_name} | "
            f"Season: {season} | Round: {round_num} | "
            f"Mode: {refresh_mode}"
        )

        # Pagination loop
        while True:
            try:
                # Generate S3 path
                s3_key = self._generate_path(
                    endpoint_name, batch_id, season, round_num, page
                )

                # Check if file exists (optimistic skip)
                if not force_refresh and self.store.object_exists(s3_key):
                    logger.warning(f"⏭️  Skipping {s3_key} (already exists)")
                    self.stats["files_skipped"] += 1 
                    offset += limit
                    page += 1
                    continue

                # Fetch data from API
                logger.info(f"📥 Fetching: {full_url} (Offset: {offset})")
                response_data = self.fetch_page(full_url, limit, offset)

                # Extract total records
                mr_data = response_data.get("MRData", {})
                total_records = int(mr_data.get("total", 0))

                # Build metadata
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

                # Save to MinIO
                self._save_to_minio(response_data, s3_key, metadata)

                # Check if we're done with pagination
                if not is_paginated or (offset + limit >= total_records):
                    logger.info(
                        f"✅ Completed {endpoint_name} | "
                        f"Total records: {total_records} | "
                        f"Pages: {page}"
                    )
                    break

                # Move to next page
                offset += limit
                page += 1

            except Exception as e:
                logger.error(
                    f"❌ Failed processing {endpoint_name} "
                    f"page {page}: {e}"
                )
                raise

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
            
        Raises:
            Exception: On critical extraction failures
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
            # Phase 1: Season-level data
            logger.info("\n--- Phase 1: Season-Level Data ---")
            for endpoint in ["constructors", "drivers", "races"]:
                self.ingest_endpoint(
                    endpoint, 
                    batch_id, 
                    season=season, 
                    force_refresh=force_refresh
                )

            # Phase 2: Fetch race calendar
            logger.info("\n--- Phase 2: Race Calendar ---")
            schedule_url = f"{self.base_url}/{season}.json"
            schedule_data = self.fetch_page(schedule_url, limit=DEFAULT_LIMIT, offset=0)
            races_list = schedule_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

            total_rounds = len(races_list)
            logger.info(f"📅 Found {total_rounds} rounds for season {season}")

            if total_rounds == 0:
                logger.warning(f"⚠️  No races found for season {season}")
                return self._generate_summary(start_time)

            # Phase 3: Race-level data extraction
            logger.info("\n--- Phase 3: Race-Level Data ---")
            for idx, race in enumerate(races_list, 1):
                round_num = int(race["round"])
                race_name = race.get("raceName", "Unknown")

                logger.info(
                    f"\n   📍 [{idx}/{total_rounds}] Round {round_num}: {race_name}"
                )

                # Race data endpoints
                for endpoint in ["results", "qualifying", "laps", "pitstops", "sprint"]:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round_num=round_num,
                        force_refresh=force_refresh,
                    )

                # Standings endpoints
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
            # Generate summary even on failure to capture partial stats
            summary = self._generate_summary(start_time)
            summary["status"] = "FAILED"
            summary["error_message"] = str(e)
            raise
        
    def _generate_summary(self, start_time: datetime) -> Dict[str, Any]:
        """
        Generate extraction summary with statistics.
        
        Args:
            start_time: Extraction start timestamp
            
        Returns:
            Dictionary containing execution metrics
        """
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