# src/f1_data/ingestion/config.py
import os
import logging
from typing import Dict, TypedDict, Literal
from dotenv import load_dotenv, find_dotenv

# --- LOGGING CONFIG ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- ENV LOADING ---
# Load .env file for local development. 
# In Docker, these values are injected directly via docker-compose.
_env_found = load_dotenv(find_dotenv())
if not _env_found:
    logger.info("ℹ️  No .env file found. Assuming running in Docker or CI environment.")

# --- TYPE DEFINITIONS ---
class EndpointSettings(TypedDict):
    """Schema for defining API endpoint characteristics."""
    group: Literal["season", "race", "reference"]
    url_pattern: str
    has_season: bool
    has_round: bool
    pagination: bool

# --- 1. API CONFIGURATION (Jolpica) ---
BASE_URL: str = "https://api.jolpi.ca/ergast/f1"
REQUEST_TIMEOUT: int = 30

# Rate Limiting (Loaded from .env with safe defaults)
DEFAULT_LIMIT: int = int(os.getenv("API_PAGE_LIMIT", "100"))
API_RATE_PER_SEC: int = int(os.getenv("API_RATE_LIMIT_PER_SEC", "4"))
API_RATE_PER_MIN: int = int(os.getenv("API_RATE_LIMIT_PER_MIN", "200"))
API_BURST: int = int(os.getenv("API_BURST_LIMIT", "10"))

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": "F1-Data-Pipeline/1.0",
    "Accept": "application/json"
}

# --- 2. RETRY STRATEGY CONFIGURATION ---
# Controlled centrally here, used by ingestor.py
RETRY_MAX_ATTEMPTS: int = 10
RETRY_MIN_WAIT: int = 2
RETRY_MAX_WAIT: int = 60

# --- 3. OBJECT STORE CONFIGURATION (MinIO) ---
# Logic: Check for 'MINIO_ENDPOINT' (set by Docker). 
# If missing, fallback to 'MINIO_ENDPOINT_EXTERNAL' (set by .env for local).
# If both missing, default to localhost.
MINIO_ENDPOINT: str = os.getenv(
    "MINIO_ENDPOINT", 
    os.getenv("MINIO_ENDPOINT_EXTERNAL", "http://localhost:9000")
)
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Bucket Names
MINIO_BUCKET_BRONZE: str = os.getenv("BUCKET_BRONZE", "bronze")
MINIO_BUCKET_SILVER: str = os.getenv("BUCKET_SILVER", "silver")
MINIO_BUCKET_GOLD: str = os.getenv("BUCKET_GOLD", "gold")

# --- 4. DATA WAREHOUSE CONFIGURATION (ClickHouse) ---
# Similar logic: Docker env var takes precedence over local .env var
CLICKHOUSE_HOST: str = os.getenv(
    "CLICKHOUSE_HOST", 
    os.getenv("CLICKHOUSE_HOST_EXTERNAL", "localhost")
)

# Port selection logic: 
# If running in Docker, we usually use the internal HTTP port (8123).
# If running locally, we use the external mapped port (8123).
CLICKHOUSE_PORT: int = int(os.getenv(
    "CLICKHOUSE_PORT", 
    os.getenv("CLICKHOUSE_PORT_HTTP_EXTERNAL", "8123")
))

CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB: str = os.getenv("CLICKHOUSE_DB", "default")

# --- 5. ENDPOINT STRATEGY (Business Rules) ---
# Maps pipeline definitions to Jolpica API patterns
ENDPOINT_CONFIG: Dict[str, EndpointSettings] = {
    # --- Reference Data (Rarely Changes) ---
    "seasons": {
        "group": "reference", "url_pattern": "seasons.json",
        "has_season": False, "has_round": False, "pagination": True
    },
    "circuits": {
        "group": "reference", "url_pattern": "{season}/circuits.json",
        "has_season": True, "has_round": False, "pagination": True
    },
    "status": {
        "group": "reference", "url_pattern": "status.json",
        "has_season": False, "has_round": False, "pagination": True
    },
    
    # --- Season Level Data (Updates Annually/Quarterly) ---
    "constructors": {
        "group": "season", "url_pattern": "{season}/constructors.json",
        "has_season": True, "has_round": False, "pagination": True
    },
    "drivers": {
        "group": "season", "url_pattern": "{season}/drivers.json",
        "has_season": True, "has_round": False, "pagination": True
    },
    "races": {
        "group": "season", "url_pattern": "{season}.json",
        "has_season": True, "has_round": False, "pagination": False
    },

    # --- Race Level Data (Updates Weekly) ---
    "results": {
        "group": "race", "url_pattern": "{season}/{round}/results.json",
        "has_season": True, "has_round": True, "pagination": True
    },
    "qualifying": {
        "group": "race", "url_pattern": "{season}/{round}/qualifying.json",
        "has_season": True, "has_round": True, "pagination": True
    },
    "laps": {
        "group": "race", "url_pattern": "{season}/{round}/laps.json",
        "has_season": True, "has_round": True, "pagination": True
    },
    "pitstops": {
        "group": "race", "url_pattern": "{season}/{round}/pitstops.json",
        "has_season": True, "has_round": True, "pagination": True
    },
    "sprint": {
        "group": "race", "url_pattern": "{season}/{round}/sprint.json",
        "has_season": True, "has_round": True, "pagination": True
    },
    
    # --- Standings (Race Level Dependency) ---
    "driverstandings": {
        "group": "race", "url_pattern": "{season}/{round}/driverStandings.json",
        "has_season": True, "has_round": True, "pagination": True
    },
    "constructorstandings": {
        "group": "race", "url_pattern": "{season}/{round}/constructorStandings.json",
        "has_season": True, "has_round": True, "pagination": True
    }
}