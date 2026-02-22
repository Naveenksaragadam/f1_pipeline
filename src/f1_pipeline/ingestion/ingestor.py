# src/f1_pipeline/ingestion/ingestor.py
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
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import requests
from requests import RequestException
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from f1_pipeline.minio.object_store import F1ObjectStore

from ..config import (
    BASE_URL,
    DEFAULT_LIMIT,
    ENDPOINT_CONFIG,
    MAX_CONCURRENT_WORKERS,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_BRONZE,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    REQUEST_TIMEOUT,
    RETRY_MAX_ATTEMPTS,
    RETRY_MAX_WAIT,
    RETRY_MIN_WAIT,
)
from ..exceptions import DataCorruptionError

# Internal Imports
from .http_client import create_session

logger = logging.getLogger(__name__)


@dataclass
class IngestorStats:
    """Thread-safe accumulator for extraction run statistics.

    All mutation methods acquire a lock so that concurrent page workers
    can safely update shared counters without data races.
    """

    files_written: int = field(default=0)
    files_skipped: int = field(default=0)
    api_calls_made: int = field(default=0)
    bytes_written: int = field(default=0)
    errors_encountered: int = field(default=0)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def increment(self, field_name: str, amount: int = 1) -> None:
        """Atomically increment a named counter."""
        with self._lock:
            setattr(self, field_name, getattr(self, field_name) + amount)

    def reset(self) -> None:
        """Reset all counters to zero."""
        with self._lock:
            self.files_written = 0
            self.files_skipped = 0
            self.api_calls_made = 0
            self.bytes_written = 0
            self.errors_encountered = 0

    def to_dict(self) -> dict[str, int]:
        """Return a snapshot dict for use in summaries."""
        with self._lock:
            return {
                "files_written": self.files_written,
                "files_skipped": self.files_skipped,
                "api_calls_made": self.api_calls_made,
                "bytes_written": self.bytes_written,
                "errors_encountered": self.errors_encountered,
            }


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
        session: requests.Session | None = None,
        validate_connection: bool = True,
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
        self.stats = IngestorStats()

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
                f"Cannot connect to MinIO ({e}). Ensure docker-compose services are running."
            ) from e

    def _reset_stats(self) -> None:
        """Reset statistics counters to initial state."""
        self.stats.reset()

    def _generate_path(
        self,
        endpoint: str,
        batch_id: str,
        season: int | None,
        round_num: int | None,
        page: int,
    ) -> str:
        """
        Generate Hive-style partitioned path for Bronze layer.
        """
        path_parts = ["ergast", f"endpoint={endpoint}"]

        if season is not None:
            path_parts.append(f"season={season}")
        if round_num is not None:
            path_parts.append(f"round={int(round_num):02d}")

        path_parts.append(f"batch_id={batch_id}")

        # Filename (paginated format)
        filename = f"page_{page:03d}.json"

        return "/".join(path_parts) + "/" + filename

    @staticmethod
    def _check_for_items(obj: Any, _depth: int = 0) -> bool:
        """Recursively check whether obj contains any non-empty list."""
        if _depth > 20:
            # Guard against pathological nesting / adversarial input.
            return False
        if isinstance(obj, list) and len(obj) > 0:
            return True
        if isinstance(obj, dict):
            return any(F1DataIngestor._check_for_items(v, _depth + 1) for v in obj.values())
        return False

    def _get_existing_keys(
        self,
        endpoint: str,
        batch_id: str,
        season: int | None,
        round_num: int | None,
    ) -> set[str]:
        """
        Batch fetch existing object keys for an endpoint to minimize S3 HEAD requests.
        """
        # Build prefix for listing
        prefix_parts = ["ergast", f"endpoint={endpoint}"]

        if season is not None:
            prefix_parts.append(f"season={season}")
        if round_num is not None:
            prefix_parts.append(f"round={int(round_num):02d}")

        prefix_parts.append(f"batch_id={batch_id}")
        prefix = "/".join(prefix_parts) + "/"

        # List all objects with this prefix
        try:
            existing_keys = self.store.list_objects(prefix=prefix)
            logger.debug(f"Found {len(existing_keys)} existing files for prefix: {prefix}")
            return set(existing_keys)
        except Exception as e:
            # Safely extract Boto3 error code if present (e.g. ClientError)
            response = getattr(e, "response", {})
            error_code = (
                response.get("Error", {}).get("Code", "") if isinstance(response, dict) else ""
            )
            if error_code in ("403", "AccessDenied"):
                # Permission errors are infrastructure problems — don't silently swallow.
                logger.error(f"❌ Access denied listing objects (prefix={prefix}): {e}")
                raise
            logger.warning(
                f"Failed to list existing objects: {e}. Falling back to per-key HEAD checks."
            )
            return set()

    def _save_to_minio(self, data: dict[str, Any], path: str, metadata: dict[str, Any]) -> None:
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
            self.stats.increment("files_written")
            if self.stats.files_written % 10 == 0:
                logger.debug(f"Snapshot: {self.stats.to_dict()}")
        except Exception as e:
            self.stats.increment("errors_encountered")
            logger.error(f"❌ Failed to save {path}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
        wait=wait_exponential(min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
        retry=retry_if_exception_type(RequestException),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def fetch_page(self, url: str, limit: int, offset: int) -> dict[str, Any]:
        """
        Fetch a single page from the API with automatic retries.
        """
        params = {"limit": limit, "offset": offset}

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            self.stats.increment("api_calls_made")
            data: dict[str, Any] = response.json()

            return data

        except RequestException as e:
            # Note: do NOT increment errors_encountered here — this function is decorated
            # with @retry and will be called up to RETRY_MAX_ATTEMPTS times per logical
            # request. The caller (_fetch_and_save_page) increments errors_encountered
            # once at the page level after all retries are exhausted.
            logger.error(f"❌ API request failed: {e}")
            raise

        except ValueError as e:
            logger.error(f"❌ Invalid JSON response: {e}")
            self.stats.increment("errors_encountered")
            raise

    def _fetch_and_save_page(
        self,
        endpoint_name: str,
        batch_id: str,
        full_url: str,
        season: int | None,
        round_num: int | None,
        page: int,
        limit: int,
        offset: int,
        force_refresh: bool,
        existing_keys: set[str] | None = None,
    ) -> tuple[bool, int]:
        """Fetch and save a single page (used for concurrent processing)."""
        try:
            s3_key = self._generate_path(endpoint_name, batch_id, season, round_num, page)

            if not force_refresh:
                # Check provided batch set or fallback to individual check
                if existing_keys is not None:
                    if s3_key in existing_keys:
                        self.stats.increment("files_skipped")
                        return (True, 0)
                elif self.store.object_exists(s3_key):
                    self.stats.increment("files_skipped")
                    return (True, 0)

            response_data = self.fetch_page(full_url, limit, offset)
            mr_data = response_data.get("MRData", {})
            total_records = int(mr_data.get("total", 0))

            metadata = {
                "ingestion_timestamp": datetime.now(UTC).isoformat(),
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

            # Data Quality Layer: Catch silent API failures (e.g. total > 0 but empty payload)
            if total_records > 0:
                # Ergast wraps data in MRData -> [Endpoint]Table -> [Item]s.
                # If the API claims total > 0 but returns no list items, the response is corrupt.
                if not self._check_for_items(mr_data):
                    raise DataCorruptionError(
                        f"Silent Data Corruption: API reported total={total_records} "
                        f"but returned an empty data payload for '{endpoint_name}' page {page}."
                    )

            self._save_to_minio(response_data, s3_key, metadata)
            return (True, total_records)

        except DataCorruptionError:
            # Data corruption is unrecoverable — do not count as a skippable error.
            # Let it propagate to halt the run and surface the underlying API bug.
            raise
        except Exception as e:
            logger.error(f"Failed processing {endpoint_name} page {page}: {e}", exc_info=True)
            self.stats.increment("errors_encountered")
            return (False, 0)

    def ingest_endpoint(
        self,
        endpoint_name: str,
        batch_id: str,
        season: int | None = None,
        round_num: int | None = None,
        force_refresh: bool = False,
        max_workers: int = 1,
    ) -> None:
        """
        Ingest data from a specific API endpoint with pagination support and optional concurrency.

        Stats lifecycle note:
            Results are accumulated into ``self.stats``, which is shared across calls.
            ``_reset_stats()`` is NOT called here — it is the responsibility of the caller
            (i.e. ``run_full_extraction``) to reset before starting a new extraction run.
            If calling ``ingest_endpoint`` directly, reset manually with ``_reset_stats()``
            if you need isolated counters.
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

        logger.info(
            f"🚀 Starting: {endpoint_name:20s} | "
            f"Season: {season} | Round: {round_num} | "
            f"Workers: {max_workers}"
        )

        # STEP 1: Always fetch first page to determine total records.
        # Note: page 1 deliberately uses an individual object_exists() HEAD check
        # (via existing_keys=None) rather than a batch LIST. A batch LIST would cost
        # more than a single HEAD for endpoints with only one page.
        try:
            first_page_success, total_records = self._fetch_and_save_page(
                endpoint_name,
                batch_id,
                full_url,
                season,
                round_num,
                page=1,
                limit=limit,
                offset=0,
                force_refresh=force_refresh,
            )
            if not first_page_success:
                raise RuntimeError(f"Failed to fetch first page of {endpoint_name}")
        except Exception as e:
            logger.error(f"❌ Failed to init ingestion for {endpoint_name}: {e}")
            raise

        # STEP 2: Calculate total pages
        if not is_paginated or total_records == 0:
            total_pages = 1
        else:
            total_pages = (total_records + limit - 1) // limit

        logger.info(f"📊 {endpoint_name}: {total_records} records across {total_pages} page(s)")

        if total_pages <= 1:
            return

        # STEP 3: Fetch remaining pages
        remaining_pages = list(range(2, total_pages + 1))

        if max_workers > 1 and len(remaining_pages) >= 2:
            existing_keys = set()
            if not force_refresh:
                existing_keys = self._get_existing_keys(endpoint_name, batch_id, season, round_num)

            logger.info(
                f"Fetching {len(remaining_pages)} pages concurrently (workers={max_workers})"
            )
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for page in remaining_pages:
                    offset = (page - 1) * limit
                    future = executor.submit(
                        self._fetch_and_save_page,
                        endpoint_name,
                        batch_id,
                        full_url,
                        season,
                        round_num,
                        page,
                        limit,
                        offset,
                        force_refresh,
                        existing_keys,
                    )
                    futures.append((page, future))

                # Check results — apply timeout to prevent indefinite hangs
                for page, future in futures:
                    try:
                        success, _ = future.result(timeout=REQUEST_TIMEOUT)
                        if not success:
                            logger.warning(f"Page {page} failed")
                    except TimeoutError:
                        logger.error(f"Page {page} timed out after {REQUEST_TIMEOUT}s")
                    except Exception as e:
                        logger.error(f"Page {page} exception: {e}")
        else:
            # Sequential fallback
            existing_keys = set()
            if not force_refresh:
                existing_keys = self._get_existing_keys(endpoint_name, batch_id, season, round_num)

            for page in remaining_pages:
                offset = (page - 1) * limit
                s3_key = self._generate_path(endpoint_name, batch_id, season, round_num, page)

                if not force_refresh and s3_key in existing_keys:
                    self.stats.increment("files_skipped")
                    continue

                self._fetch_and_save_page(
                    endpoint_name,
                    batch_id,
                    full_url,
                    season,
                    round_num,
                    page,
                    limit,
                    offset,
                    force_refresh,
                    existing_keys,  # pass the pre-fetched set so workers don't HEAD individually
                )

        logger.info(f"✅ Completed {endpoint_name}")

    def _run_phase_reference_data(self, batch_id: str, season: int, force_refresh: bool) -> None:
        """Phase 1: Ingest static reference endpoints — seasons, circuits, status."""
        logger.info("--- Phase 1: Reference Data ---")
        for endpoint in ["seasons", "circuits", "status"]:
            self.ingest_endpoint(
                endpoint,
                batch_id,
                season=(season if endpoint == "circuits" else None),
                force_refresh=force_refresh,
                max_workers=1,
            )

    def _run_phase_season_data(
        self, batch_id: str, season: int, force_refresh: bool, max_workers: int
    ) -> list[dict[str, Any]]:
        """Phase 2 + 3: Ingest season-level data and parse the race calendar.

        Returns:
            List of race dicts from the Bronze race calendar (or API fallback).
        """
        logger.info("--- Phase 2: Season-Level Data ---")
        for endpoint in ["constructors", "drivers", "races"]:
            self.ingest_endpoint(
                endpoint,
                batch_id,
                season=season,
                force_refresh=force_refresh,
                max_workers=max_workers,
            )

        # Phase 3: Parse race calendar from Bronze (races just ingested above).
        logger.info("--- Phase 3: Race Calendar Parsing ---")
        races_key = f"ergast/endpoint=races/season={season}/batch_id={batch_id}/page_001.json"
        try:
            logger.info(f"📖 Reading race calendar from Bronze: {races_key}")
            races_envelope = self.store.get_json(races_key)
            races_list: list[dict[str, Any]] = (
                races_envelope.get("data", {})
                .get("MRData", {})
                .get("RaceTable", {})
                .get("Races", [])
            )
            logger.info("✅ Successfully read race calendar from Bronze")
        except Exception as e:
            # Invariant: Phase 2 already ingested races successfully (or would have raised),
            # so the data IS in Bronze. A read failure here is a transient MinIO issue.
            # We fall back to a direct API call to unblock Phase 4, but this is unexpected.
            logger.error(
                f"❌ Failed to read race calendar from Bronze (unexpected — data should exist): {e}\n"
                f"   Falling back to direct API call to unblock Phase 4..."
            )
            schedule_pattern = ENDPOINT_CONFIG["races"]["url_pattern"]
            schedule_url = f"{self.base_url}/{schedule_pattern.format(season=season)}"
            schedule_data = self.fetch_page(schedule_url, limit=DEFAULT_LIMIT, offset=0)
            races_list = schedule_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

        logger.info(f"📅 Found {len(races_list)} rounds for season {season}")
        return races_list

    def _run_phase_race_data(
        self,
        races_list: list[dict[str, Any]],
        batch_id: str,
        season: int,
        force_refresh: bool,
        max_workers: int,
    ) -> None:
        """Phase 4: Ingest all per-round race endpoints for every round in the season."""
        total_rounds = len(races_list)
        logger.info(
            f"\n{'=' * 70}\n--- Phase 4: Race-Level Data ({total_rounds} rounds) ---\n{'=' * 70}"
        )

        for idx, race in enumerate(races_list, 1):
            round_num = int(race["round"])
            race_name = race.get("raceName", "Unknown")
            round_start = datetime.now()

            logger.info(
                f"\n{'─' * 70}\n[{idx}/{total_rounds}] Round {round_num}: {race_name}\n{'─' * 70}"
            )

            race_endpoints = ["results", "qualifying", "laps", "pitstops"]
            if "Sprint" in race:
                race_endpoints.append("sprint")
                logger.debug(f"🏃‍♂️ Sprint session detected for Round {round_num}")

            for endpoint in race_endpoints:
                self.ingest_endpoint(
                    endpoint,
                    batch_id,
                    season=season,
                    round_num=round_num,
                    force_refresh=force_refresh,
                    max_workers=max_workers,
                )

            for endpoint in ["driverstandings", "constructorstandings"]:
                self.ingest_endpoint(
                    endpoint,
                    batch_id,
                    season=season,
                    round_num=round_num,
                    force_refresh=force_refresh,
                    max_workers=1,
                )

            round_duration = (datetime.now() - round_start).total_seconds()
            logger.info(
                f"{'─' * 70}\n"
                f"✅ Round {round_num} Complete: {round_duration:.2f}s "
                f"({round_duration / 60:.1f}m)\n"
                f"{'─' * 70}"
            )

    def run_full_extraction(
        self,
        season: int,
        batch_id: str,
        force_refresh: bool = False,
        concurrency_enabled: bool = True,
    ) -> dict[str, Any]:
        """
        Orchestrate full season extraction following the 4-phase dependency graph.

        Args:
            season: F1 season year (e.g., 2024)
            batch_id: Unique batch identifier
            force_refresh: If True, re-fetch all data regardless of existing files
            concurrency_enabled: Enable threading for paginated endpoints

        Returns:
            Dictionary containing extraction statistics and phase timings
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
        phase_timings: dict[str, float] = {}
        max_workers = MAX_CONCURRENT_WORKERS if concurrency_enabled else 1

        try:
            phase_start = datetime.now()
            self._run_phase_reference_data(batch_id, season, force_refresh)
            phase_timings["reference_data"] = (datetime.now() - phase_start).total_seconds()
            logger.info(f"✅ Phase 1 Complete: {phase_timings['reference_data']:.2f}s\n")

            phase_start = datetime.now()
            races_list = self._run_phase_season_data(batch_id, season, force_refresh, max_workers)
            phase_timings["season_data"] = (datetime.now() - phase_start).total_seconds()
            logger.info(f"✅ Phases 2+3 Complete: {phase_timings['season_data']:.2f}s\n")

            if not races_list:
                logger.warning(f"⚠️  No races found for season {season}")
                return self._generate_summary(extraction_start, phase_timings)

            phase_start = datetime.now()
            self._run_phase_race_data(races_list, batch_id, season, force_refresh, max_workers)
            phase_timings["race_data"] = (datetime.now() - phase_start).total_seconds()

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
        phase_timings: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """
        Generate extraction summary with statistics.
        """
        duration = (datetime.now() - start_time).total_seconds()
        stats_snapshot = self.stats.to_dict()

        summary: dict[str, Any] = {
            "status": "SUCCESS" if self.stats.errors_encountered == 0 else "PARTIAL",
            "duration_seconds": round(duration, 2),
            "duration_minutes": round(duration / 60, 2),
            **stats_snapshot,
        }

        if phase_timings:
            summary["phase_timings"] = {k: round(v, 2) for k, v in phase_timings.items()}

        logger.info(
            f"\n{'=' * 70}\n"
            f"✅ Extraction Complete\n"
            f"   Status: {summary['status']}\n"
            f"   Duration: {duration:.2f}s ({duration / 60:.1f}m)\n"
            f"   Files Written: {self.stats.files_written}\n"
            f"   Files Skipped: {self.stats.files_skipped}\n"
            f"   API Calls: {self.stats.api_calls_made}\n"
            f"   Errors: {self.stats.errors_encountered}\n"
        )

        if phase_timings:
            logger.info("   Phase Breakdown:")
            for phase, timing in phase_timings.items():
                logger.info(f"     - {phase}: {timing:.2f}s ({timing / 60:.1f}m)")

        logger.info(f"{'=' * 70}\n")

        return summary
