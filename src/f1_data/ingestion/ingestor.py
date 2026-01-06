# src/f1_data/ingestion/ingestor.py
"""
F1 Data Ingestor - Production-Grade Implementation
Handles API extraction with retry logic, rate limiting, and idempotent writes.

Key Improvements:
- Fixed pagination logic to always determine total_records first
- Optimized S3 existence checks with batch listing
- Eliminated duplicate race calendar fetching
- Added sprint season filtering (2021+)
- Enhanced performance tracking with phase timings
"""
import logging
from requests import RequestException
from typing import Dict, Any, Optional, Set
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
    - Optimized batch existence checks
    - Automatic pagination handling
    - Comprehensive error handling and retry logic
    - Detailed stats tracking with phase timings
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

    def _get_existing_keys(
        self,
        endpoint: str,
        batch_id: str,
        season: Optional[int],
        round_num: Optional[int],
    ) -> Set[str]:
        """
        Batch fetch existing object keys for an endpoint to minimize S3 HEAD requests.
        
        Args:
            endpoint: API endpoint name
            batch_id: Unique batch identifier
            season: Optional season filter
            round_num: Optional round filter
            
        Returns:
            Set of existing S3 object keys
        """
        # Build prefix for listing
        prefix_parts = ["ergast", f"endpoint={endpoint}"]
        
        if season:
            prefix_parts.append(f"season={season}")
        if round_num:
            prefix_parts.append(f"round={int(round_num):02d}")
        
        prefix_parts.append(f"batch_id={batch_id}")
        prefix = "/".join(prefix_parts) + "/"
        
        # List all objects with this prefix
        try:
            existing_keys = self.store.list_objects(prefix=prefix)
            logger.debug(f"Found {len(existing_keys)} existing files for prefix: {prefix}")
            return set(existing_keys)
        except Exception as e:
            logger.warning(f"Failed to list existing objects: {e}. Will check individually.")
            return set()

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
            
            logger.debug(f"✅ API call successful | Status: {response.status_code}")
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
        
        REFACTORED LOGIC:
        1. Always fetch first page to determine total_records
        2. Calculate total pages needed
        3. Batch check existing files (one S3 LIST instead of N HEADs)
        4. Process all pages with proper skip logic

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
        limit = DEFAULT_LIMIT
        is_paginated = config.get("pagination", True)
        refresh_mode = "FORCE_REFRESH" if force_refresh else "IDEMPOTENT"

        logger.info(
            f"🚀 Starting: {endpoint_name} | "
            f"Season: {season} | Round: {round_num} | "
            f"Mode: {refresh_mode}"
        )

        # STEP 1: Always fetch first page to determine total records
        try:
            first_page_data = self.fetch_page(full_url, limit, offset=0)
            mr_data = first_page_data.get("MRData", {})
            total_records = int(mr_data.get("total", 0))
        except Exception as e:
            logger.error(f"❌ Failed to fetch first page for {endpoint_name}: {e}")
            raise

        # STEP 2: Calculate total pages
        if not is_paginated or total_records == 0:
            total_pages = 1
        else:
            total_pages = (total_records + limit - 1) // limit

        logger.info(
            f"📊 {endpoint_name}: {total_records} records across {total_pages} page(s)"
        )

        # STEP 3: Batch check existing files (optimization)
        existing_keys = set()
        if not force_refresh:
            existing_keys = self._get_existing_keys(
                endpoint_name, batch_id, season, round_num
            )

        # STEP 4: Process all pages
        for page in range(1, total_pages + 1):
            offset = (page - 1) * limit
            s3_key = self._generate_path(
                endpoint_name, batch_id, season, round_num, page
            )

            # Check if file exists (using batch-fetched set)
            if not force_refresh and s3_key in existing_keys:
                logger.debug(f"⏭️  Skipping {s3_key} (already exists)")
                self.stats["files_skipped"] += 1
                continue

            # Fetch data (reuse first page data if page == 1)
            if page == 1:
                response_data = first_page_data
            else:
                try:
                    response_data = self.fetch_page(full_url, limit, offset)
                except Exception as e:
                    logger.error(f"❌ Failed to fetch page {page}: {e}")
                    self.stats["errors_encountered"] += 1
                    continue  # Skip this page, continue with others

            # Build metadata
            metadata = {
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "batch_id": batch_id,
                "endpoint": endpoint_name,
                "season": season,
                "round": round_num,
                "page": page,
                "total_pages": total_pages,
                "source_url": f"{full_url}?limit={limit}&offset={offset}",
                "api_response_total": total_records,
                "file_version": "1.0",
                "force_refresh": force_refresh,
            }

            # Save to MinIO
            try:
                self._save_to_minio(response_data, s3_key, metadata)
            except Exception as e:
                logger.error(f"❌ Failed to save page {page}: {e}")
                continue  # Continue processing other pages

        logger.info(
            f"✅ Completed {endpoint_name} | "
            f"Records: {total_records} | "
            f"Pages: {total_pages}"
        )

    def run_full_extraction(
        self, 
        season: int, 
        batch_id: str, 
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Orchestrate full season extraction following dependency graph.
        
        REFACTORED IMPROVEMENTS:
        - Read race calendar from Bronze instead of re-fetching from API
        - Added sprint season filtering (only for 2021+)
        - Enhanced performance tracking with phase timings
        
        Extraction order:
        1. Reference data (seasons, circuits, status)
        2. Season-level data (constructors, drivers, races)
        3. Race-level data (results, qualifying, laps, pitstops, sprint*)
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
            f"🏎️  F1 Data Extraction Started\n"
            f"   Season: {season}\n"
            f"   Batch ID: {batch_id}\n"
            f"   Force Refresh: {force_refresh}\n"
            f"{'=' * 70}\n"
        )

        self._reset_stats()
        extraction_start = datetime.now()
        phase_timings = {}

        try:
            # ============================================================
            # PHASE 1: Reference Data (Static Endpoints)
            # ============================================================
            phase_start = datetime.now()
            logger.info("\n--- Phase 1: Reference Data ---")
            
            for endpoint in ["seasons", "circuits", "status"]:
                if endpoint == "circuits":
                    # Circuits are fetched per season to capture season-specific tracks
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        force_refresh=force_refresh
                    )
                else:
                    # Seasons and status are truly static
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        force_refresh=force_refresh
                    )
            
            phase_timings['reference_data'] = (datetime.now() - phase_start).total_seconds()
            logger.info(f"✅ Phase 1 Complete: {phase_timings['reference_data']:.2f}s\n")
            
            # ============================================================
            # PHASE 2: Season-Level Data
            # ============================================================
            phase_start = datetime.now()
            logger.info("--- Phase 2: Season-Level Data ---")
            
            for endpoint in ["constructors", "drivers", "races"]:
                self.ingest_endpoint(
                    endpoint, 
                    batch_id, 
                    season=season, 
                    force_refresh=force_refresh
                )
            
            phase_timings['season_data'] = (datetime.now() - phase_start).total_seconds()
            logger.info(f"✅ Phase 2 Complete: {phase_timings['season_data']:.2f}s\n")

            # ============================================================
            # PHASE 3: Parse Race Calendar (Read from Bronze)
            # ============================================================
            phase_start = datetime.now()
            logger.info("--- Phase 3: Race Calendar Parsing ---")
            
            # FIXED: Read race calendar from Bronze instead of re-fetching from API
            races_key = f"ergast/endpoint=races/season={season}/batch_id={batch_id}/page_001.json"
            
            try:
                logger.info(f"📖 Reading race calendar from Bronze: {races_key}")
                races_envelope = self.store.get_json(races_key)
                races_list = races_envelope["data"]["MRData"]["RaceTable"]["Races"]
                logger.info("✅ Successfully read race calendar from Bronze")
            except Exception as e:
                logger.warning(
                    f"⚠️  Failed to read race calendar from Bronze: {e}\n"
                    f"   Falling back to API fetch..."
                )
                # Fallback: Fetch from API if Bronze read fails
                schedule_url = f"{self.base_url}/{season}.json"
                schedule_data = self.fetch_page(schedule_url, limit=DEFAULT_LIMIT, offset=0)
                races_list = schedule_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

            total_rounds = len(races_list)
            logger.info(f"📅 Found {total_rounds} rounds for season {season}")
            
            phase_timings['calendar_parse'] = (datetime.now() - phase_start).total_seconds()

            if total_rounds == 0:
                logger.warning(f"⚠️  No races found for season {season}")
                return self._generate_summary(extraction_start, phase_timings)

            # ============================================================
            # PHASE 4: Race-Level Data Extraction
            # ============================================================
            logger.info(
                f"\n{'=' * 70}\n"
                f"--- Phase 4: Race-Level Data ({total_rounds} rounds) ---\n"
                f"{'=' * 70}"
            )
            
            race_phase_start = datetime.now()
            
            for idx, race in enumerate(races_list, 1):
                round_num = int(race["round"])
                race_name = race.get("raceName", "Unknown")
                round_start = datetime.now()
                
                logger.info(
                    f"\n{'─' * 70}\n"
                    f"[{idx}/{total_rounds}] Round {round_num}: {race_name}\n"
                    f"{'─' * 70}"
                )

                # FIXED: Dynamic endpoint list based on season (sprint only for 2021+)
                race_endpoints = ["results", "qualifying", "laps", "pitstops"]
                
                if season >= 2021:
                    race_endpoints.append("sprint")
                    logger.debug("Sprint races enabled for season >= 2021")

                # Extract race-level data
                for endpoint in race_endpoints:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round_num=round_num,
                        force_refresh=force_refresh,
                    )

                # Extract standings (cumulative after each race)
                for endpoint in ["driverstandings", "constructorstandings"]:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round_num=round_num,
                        force_refresh=force_refresh,
                    )
                
                round_duration = (datetime.now() - round_start).total_seconds()
                logger.info(
                    f"{'─' * 70}\n"
                    f"✅ Round {round_num} Complete: {round_duration:.2f}s "
                    f"({round_duration/60:.1f}m)\n"
                    f"{'─' * 70}"
                )
            
            phase_timings['race_data'] = (datetime.now() - race_phase_start).total_seconds()
            
            return self._generate_summary(extraction_start, phase_timings)
        
        except Exception as e:
            logger.error(f"\n❌ Extraction FAILED for season {season}: {e}")
            summary = self._generate_summary(extraction_start, phase_timings)
            summary["status"] = "FAILED"
            summary["error_message"] = str(e)
            raise
        
    def _generate_summary(
        self,
        start_time: datetime,
        phase_timings: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Generate extraction summary with statistics.
        
        Args:
            start_time: Extraction start timestamp
            phase_timings: Optional phase timing data
            
        Returns:
            Dictionary containing execution metrics
        """
        duration = (datetime.now() - start_time).total_seconds()
        
        summary = {
            "status": "SUCCESS" if self.stats["errors_encountered"] == 0 else "PARTIAL",
            "duration_seconds": round(duration, 2),
            "duration_minutes": round(duration / 60, 2),
            **self.stats
        }
        
        # Add timing breakdowns if available
        if phase_timings:
            summary["phase_timings"] = {
                k: round(v, 2) for k, v in phase_timings.items()
            }
        
        logger.info(
            f"\n{'=' * 70}\n"
            f"✅ Extraction Complete\n"
            f"   Status: {summary['status']}\n"
            f"   Duration: {duration:.2f}s ({duration/60:.1f}m)\n"
            f"   Files Written: {self.stats['files_written']}\n"
            f"   Files Skipped: {self.stats['files_skipped']}\n"
            f"   API Calls: {self.stats['api_calls_made']}\n"
            f"   Errors: {self.stats['errors_encountered']}\n"
        )
        
        if phase_timings:
            logger.info("   Phase Breakdown:")
            for phase, timing in phase_timings.items():
                logger.info(f"     - {phase}: {timing:.2f}s ({timing/60:.1f}m)")
        
        logger.info(f"{'=' * 70}\n")
        
        return summary