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
- Support for concurrent pagination (max_workers)
"""
import logging
from requests import RequestException
from typing import Dict, Any, Optional, Set, Tuple, List
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Internal Imports
from .http_client import create_session
from ..config import (
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
    MAX_CONCURRENT_WORKERS,
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
    - Concurrent pagination support
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
        """
        envelope = {
            "metadata": metadata,
            "data": data,
        }

        # Extract S3 object metadata for tagging (enables querying without downloading)
        s3_metadata = {
            "batch_id": str(metadata["batch_id"]),
            "endpoint": str(metadata["endpoint"]),
            "ingestion_timestamp": str(metadata["ingestion_timestamp"]),
            "season": str(metadata.get("season", "")),
            "round": str(metadata.get("round", "")),
            "page": str(metadata.get("page", "")),
        }

        try:
            self.store.put_object(path, envelope, metadata=s3_metadata)
            self.stats["files_written"] += 1
            if self.stats["files_written"] % 10 == 0:
                 logger.debug(f"Snapshot: {self.stats}")
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
        """
        params = {"limit": limit, "offset": offset}

        try:
            # logger.debug(f"📡 GET {url} | limit={limit} offset={offset}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            self.stats["api_calls_made"] += 1
            data = response.json()
            
            return data

        except RequestException as e:
            logger.error(f"❌ API request failed: {e}")
            self.stats["errors_encountered"] += 1
            raise

        except ValueError as e:
            logger.error(f"❌ Invalid JSON response: {e}")
            self.stats["errors_encountered"] += 1
            raise

    def _fetch_and_save_page(
        self,
        endpoint_name: str,
        batch_id: str,
        full_url: str,
        season: Optional[int],
        round_num: Optional[int],
        page: int,
        limit: int,
        offset: int,
        force_refresh: bool,
        existing_keys: Optional[Set[str]] = None
    ) -> Tuple[bool, int]:
        """Fetch and save a single page (used for concurrent processing)."""
        try:
            s3_key = self._generate_path(
                endpoint_name, batch_id, season, round_num, page
            )

            if not force_refresh:
                # Check provided batch set or fallback to individual check
                if existing_keys is not None:
                     if s3_key in existing_keys:
                        self.stats["files_skipped"] += 1
                        return (True, 0)
                elif self.store.object_exists(s3_key):
                    self.stats["files_skipped"] += 1
                    return (True, 0)

            response_data = self.fetch_page(full_url, limit, offset)
            mr_data = response_data.get("MRData", {})
            total_records = int(mr_data.get("total", 0))

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

            self._save_to_minio(response_data, s3_key, metadata)
            return (True, total_records)

        except Exception as e:
            logger.error(f"Failed processing {endpoint_name} page {page}: {e}", exc_info=True)
            self.stats["errors_encountered"] += 1
            return (False, 0)

    def ingest_endpoint(
        self,
        endpoint_name: str,
        batch_id: str,
        season: Optional[int] = None,
        round_num: Optional[int] = None,
        force_refresh: bool = False,
        max_workers: int = 1,
    ) -> None:
        """
        Ingest data from a specific API endpoint with pagination support and optional concurrency.
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
            f"🚀 Starting: {endpoint_name:20s} | "
            f"Season: {season} | Round: {round_num} | "
            f"Workers: {max_workers}"
        )

        # STEP 1: Always fetch first page to determine total records
        try:
            first_page_success, total_records = self._fetch_and_save_page(
                endpoint_name, batch_id, full_url, season, round_num,
                page=1, limit=limit, offset=0, force_refresh=force_refresh
            )
            if not first_page_success:
                raise Exception(f"Failed to fetch first page of {endpoint_name}")
        except Exception as e:
             logger.error(f"❌ Failed to init ingestion for {endpoint_name}: {e}")
             raise

        # STEP 2: Calculate total pages
        if not is_paginated or total_records == 0:
            total_pages = 1
        else:
            total_pages = (total_records + limit - 1) // limit

        logger.info(
            f"📊 {endpoint_name}: {total_records} records across {total_pages} page(s)"
        )

        if total_pages <= 1:
            return

        # STEP 3: Fetch remaining pages
        remaining_pages = list(range(2, total_pages + 1))
        
        # Decide concurrency strategy
        if max_workers > 1 and len(remaining_pages) >= 2:
             logger.info(f"Fetching {len(remaining_pages)} pages concurrently (workers={max_workers})")
             with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for page in remaining_pages:
                    offset = (page - 1) * limit
                    future = executor.submit(
                        self._fetch_and_save_page,
                        endpoint_name, batch_id, full_url, season, round_num,
                        page, limit, offset, force_refresh, existing_keys
                    )
                    futures.append((page, future))
                
                # Check results
                for page, future in futures:
                    try:
                        success, _ = future.result()
                        if not success:
                            logger.warning(f"Page {page} failed")
                    except Exception as e:
                        logger.error(f"Page {page} exception: {e}")
        else:
            # Sequential fallback (optimized)
            existing_keys = set()
            if not force_refresh:
                existing_keys = self._get_existing_keys(
                    endpoint_name, batch_id, season, round_num
                )

            for page in remaining_pages:
                offset = (page - 1) * limit
                s3_key = self._generate_path(
                    endpoint_name, batch_id, season, round_num, page
                )

                if not force_refresh and s3_key in existing_keys:
                    self.stats["files_skipped"] += 1
                    continue
                
                self._fetch_and_save_page(
                    endpoint_name, batch_id, full_url, season, round_num,
                    page, limit, offset, force_refresh
                )

        logger.info(f"✅ Completed {endpoint_name}")

    def run_full_extraction(
        self, 
        season: int, 
        batch_id: str, 
        force_refresh: bool = False,
        concurrency_enabled: bool = True
    ) -> Dict[str, Any]:
        """
        Orchestrate full season extraction following dependency graph.
        
        Args:
            season: F1 season year (e.g., 2024)
            batch_id: Unique batch identifier
            force_refresh: If True, re-fetch all data
            concurrency_enabled: Enable threading for paginated endpoints
            
        Returns:
            Dictionary containing extraction statistics
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
        MAX_WORKERS = MAX_CONCURRENT_WORKERS if concurrency_enabled else 1

        try:
            # ============================================================
            # PHASE 1: Reference Data (Static Endpoints)
            # ============================================================
            phase_start = datetime.now()
            logger.info("\n--- Phase 1: Reference Data ---")
            
            for endpoint in ["seasons", "circuits", "status"]:
                self.ingest_endpoint(
                    endpoint,
                    batch_id,
                    season=(season if endpoint == "circuits" else None),
                    force_refresh=force_refresh,
                    max_workers=1
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
                    force_refresh=force_refresh,
                    max_workers=MAX_WORKERS
                )
            
            phase_timings['season_data'] = (datetime.now() - phase_start).total_seconds()
            logger.info(f"✅ Phase 2 Complete: {phase_timings['season_data']:.2f}s\n")

            # ============================================================
            # PHASE 3: Parse Race Calendar (Read from Bronze)
            # ============================================================
            phase_start = datetime.now()
            logger.info("--- Phase 3: Race Calendar Parsing ---")
            
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

                race_endpoints = ["results", "qualifying", "laps", "pitstops"]
                if season >= 2021:
                    race_endpoints.append("sprint")
                    logger.debug("Sprint races enabled for season >= 2021")

                for endpoint in race_endpoints:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round_num=round_num,
                        force_refresh=force_refresh,
                        max_workers=MAX_WORKERS 
                    )
                
                for endpoint in ["driverstandings", "constructorstandings"]:
                    self.ingest_endpoint(
                        endpoint,
                        batch_id,
                        season=season,
                        round_num=round_num,
                        force_refresh=force_refresh,
                        max_workers=1
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
        """
        duration = (datetime.now() - start_time).total_seconds()
        
        summary = {
            "status": "SUCCESS" if self.stats["errors_encountered"] == 0 else "PARTIAL",
            "duration_seconds": round(duration, 2),
            "duration_minutes": round(duration / 60, 2),
            **self.stats
        }
        
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