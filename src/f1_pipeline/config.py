"""
Configuration module for F1 Data Pipeline.
Loads environment variables and defines endpoint specifications.
"""

import logging
import os
import sys
from typing import TypedDict

from dotenv import find_dotenv, load_dotenv

# --- LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- ENV LOADING ---
_env_found = load_dotenv(find_dotenv())
if not _env_found:
    logger.info("ℹ️  No .env file found. Using environment variables from system/Docker.")


def get_env_required(key: str, default: str | None = None) -> str:
    """
    Get required environment variable with validation.

    Args:
        key: Environment variable name
        default: Default value if not set

    Returns:
        Environment variable value

    Raises:
        SystemExit: If required variable is missing and no default provided
    """
    value = os.getenv(key, default)
    if value is None:
        logger.error(f"❌ Required environment variable '{key}' is not set!")
        logger.error("Please check your .env file or docker-compose environment section.")
        sys.exit(1)
    return value


# --- 1. API CONFIGURATION (Ergast via Jolpica) ---
BASE_URL: str = os.getenv("API_BASE_URL", "https://api.jolpi.ca/ergast/f1")
REQUEST_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "30"))

# Rate Limiting
DEFAULT_LIMIT: int = int(os.getenv("API_PAGE_LIMIT", "100"))
API_RATE_PER_SEC: int = int(os.getenv("API_RATE_LIMIT_PER_SEC", "4"))
API_RATE_PER_MIN: int = int(os.getenv("API_RATE_LIMIT_PER_MIN", "200"))
API_BURST: int = int(os.getenv("API_BURST_LIMIT", "10"))

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": "F1-Data-Pipeline/1.0 (Production)",
    "Accept": "application/json",
}

# --- 2. RETRY STRATEGY CONFIGURATION ---
RETRY_MAX_ATTEMPTS: int = int(os.getenv("RETRY_MAX_ATTEMPTS", "10"))
RETRY_MIN_WAIT: int = int(os.getenv("RETRY_MIN_WAIT", "2"))
RETRY_MAX_WAIT: int = int(os.getenv("RETRY_MAX_WAIT", "60"))

# --- 2.1 CONCURRENCY CONFIGURATION ---
# Guideline: Be polite to the API. Ergast/Jolpica has rate limits.
MAX_CONCURRENT_WORKERS: int = int(os.getenv("MAX_CONCURRENT_WORKERS", "3"))

# --- 3. OBJECT STORE CONFIGURATION (MinIO) ---
# Docker internal endpoint vs external local endpoint
MINIO_ENDPOINT: str = os.getenv(
    "MINIO_ENDPOINT", os.getenv("MINIO_ENDPOINT_EXTERNAL", "http://localhost:9000")
)

MINIO_ACCESS_KEY: str = get_env_required("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY: str = get_env_required("MINIO_SECRET_KEY")

# Bucket Names (Medallion Architecture)
MINIO_BUCKET_BRONZE: str = os.getenv("BUCKET_BRONZE", "bronze")
MINIO_BUCKET_SILVER: str = os.getenv("BUCKET_SILVER", "silver")
MINIO_BUCKET_GOLD: str = os.getenv("BUCKET_GOLD", "gold")
MINIO_REGION: str = os.getenv("MINIO_REGION", "us-east-1")
SILVER_PARTITION_COLS: list[str] = ["season", "round"]

# --- 4. DATA WAREHOUSE CONFIGURATION (ClickHouse) ---
CLICKHOUSE_HOST: str = os.getenv(
    "CLICKHOUSE_HOST", os.getenv("CLICKHOUSE_HOST_EXTERNAL", "localhost")
)
CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD: str = get_env_required("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB: str = os.getenv("CLICKHOUSE_DB", "f1_analytics")

# --- 5. ENDPOINT STRATEGY (Business Rules) ---


class EndpointConfig(TypedDict):
    """Type-safe shape for a single endpoint entry in ENDPOINT_CONFIG."""

    group: str
    url_pattern: str
    has_season: bool
    has_round: bool
    pagination: bool


ENDPOINT_CONFIG: dict[str, EndpointConfig] = {
    # --- Reference Data (Static or Rarely Changes) ---
    "seasons": {
        "group": "reference",
        "url_pattern": "seasons.json",
        "has_season": False,
        "has_round": False,
        "pagination": True,
    },
    "circuits": {
        "group": "reference",
        "url_pattern": "{season}/circuits.json",
        "has_season": True,
        "has_round": False,
        "pagination": True,
    },
    "status": {
        "group": "reference",
        "url_pattern": "status.json",
        "has_season": False,
        "has_round": False,
        "pagination": True,
    },
    # --- Season Level Data ---
    "constructors": {
        "group": "season",
        "url_pattern": "{season}/constructors.json",
        "has_season": True,
        "has_round": False,
        "pagination": True,
    },
    "drivers": {
        "group": "season",
        "url_pattern": "{season}/drivers.json",
        "has_season": True,
        "has_round": False,
        "pagination": True,
    },
    "races": {
        "group": "season",
        "url_pattern": "{season}.json",
        "has_season": True,
        "has_round": False,
        "pagination": False,
    },
    # --- Race Level Data ---
    "results": {
        "group": "race",
        "url_pattern": "{season}/{round}/results.json",
        "has_season": True,
        "has_round": True,
        "pagination": True,
    },
    "qualifying": {
        "group": "race",
        "url_pattern": "{season}/{round}/qualifying.json",
        "has_season": True,
        "has_round": True,
        "pagination": True,
    },
    "laps": {
        "group": "race",
        "url_pattern": "{season}/{round}/laps.json",
        "has_season": True,
        "has_round": True,
        "pagination": True,
    },
    "pitstops": {
        "group": "race",
        "url_pattern": "{season}/{round}/pitstops.json",
        "has_season": True,
        "has_round": True,
        "pagination": True,
    },
    "sprint": {
        "group": "race",
        "url_pattern": "{season}/{round}/sprint.json",
        "has_season": True,
        "has_round": True,
        "pagination": True,
    },
    # --- Standings ---
    "driverstandings": {
        "group": "race",
        "url_pattern": "{season}/{round}/driverStandings.json",
        "has_season": True,
        "has_round": True,
        "pagination": True,
    },
    "constructorstandings": {
        "group": "race",
        "url_pattern": "{season}/{round}/constructorStandings.json",
        "has_season": True,
        "has_round": True,
        "pagination": True,
    },
}


# --- 6. CONFIGURATION VALIDATION ---
def validate_configuration() -> bool:
    """
    Validate that all critical configuration is present.

    Returns:
        True if validation passes

    Raises:
        SystemExit: If validation fails
    """
    errors = []

    # Check MinIO config
    if not MINIO_ACCESS_KEY or MINIO_ACCESS_KEY == "":
        errors.append("MINIO_ACCESS_KEY is not set")

    if not MINIO_SECRET_KEY or MINIO_SECRET_KEY == "":
        errors.append("MINIO_SECRET_KEY is not set")

    # Check endpoint config
    if not ENDPOINT_CONFIG:
        errors.append("ENDPOINT_CONFIG is empty")

    # Validate endpoint configurations
    for endpoint_name, config in ENDPOINT_CONFIG.items():
        if not config.get("url_pattern"):
            errors.append(f"Endpoint '{endpoint_name}' missing url_pattern")
        if "group" not in config:
            errors.append(f"Endpoint '{endpoint_name}' missing group")

    if errors:
        logger.error("❌ Configuration validation failed:")
        for error in errors:
            logger.error(f"   - {error}")
        sys.exit(1)

    logger.info("✅ Configuration validation passed")
    return True
