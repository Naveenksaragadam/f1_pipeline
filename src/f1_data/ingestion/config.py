# src/F1_PPIPELINE/ingestion/config.py
import os
import logging
from dotenv import load_dotenv, find_dotenv

# Configure a basic logger so we can see warnings
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. Load env vars (Do this ONCE here)
# find_dotenv() searches up directory trees to find the file
_env_found = load_dotenv(find_dotenv())

if not _env_found:
    logger.warning("⚠️  Warning: No .env file found. Relying on system environment variables.")

# 2. API Config
BASE_URL = "https://api.jolpi.ca/ergast/f1"

DEFAULT_HEADERS = {
    "User-Agent": "F1-Data-Project/1.0",
    "Accept": "application/json"
}

# Cast to int here so your logic doesn't fail later
DEFAULT_LIMIT = int(os.getenv("API_PAGE_LIMIT", "30")) 

# 3. Object Store Config
# We use os.getenv(KEY, DEFAULT) to provide fallbacks for local dev
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")
MINIO_BUCKET_BRONZE = os.getenv('MINIO_BUCKET_BRONZE',"bronze")
MINIO_BUCKET_SILVER = os.getenv('MINIO_BUCKET_SILVER',"silver")
MINIO_BUCKET_GOLD = os.getenv('MINIO_BUCKET_GOLD',"gold")

# 4. ENDPOINT STRATEGY (The "Business Rules")
# Classifies every endpoint by Type (Static, Season, Race) and defined URL patterns.

ENDPOINT_CONFIG = {
    # --- GROUP 1: Static / Reference (Run Quarterly) ---
    "seasons": {
        "type": "static",
        "url_template": "seasons.json",
        "pagination": True
    },
    "circuits": {
        "type": "static",
        "url_template": "circuits.json",
        "pagination": True
    },
    "status": {
        "type": "static",
        "url_template": "status.json",
        "pagination": True
    },

    # --- GROUP 2: Season-Level (Run Yearly/Quarterly) ---
    "constructors": {
        "type": "season",
        "url_template": "{season}/constructors.json",
        "pagination": True
    },
    "drivers": {
        "type": "season",
        "url_template": "{season}/drivers.json",
        "pagination": True
    },
    # The Schedule is special: We use it to drive the loop for Group 3
    "races": {
        "type": "season",
        "url_template": "{season}.json", 
        "pagination": False 
    },

    # --- GROUP 3: Race-Level (Run Weekly during Season) ---
    "results": {
        "type": "race",
        "url_template": "{season}/{round}/results.json",
        "pagination": True
    },
    "qualifying": {
        "type": "race",
        "url_template": "{season}/{round}/qualifying.json",
        "pagination": True
    },
    "sprint": {
        "type": "race",
        "url_template": "{season}/{round}/sprint.json",
        "pagination": True
    },
    "pitstops": {
        "type": "race",
        "url_template": "{season}/{round}/pitstops.json",
        "pagination": True
    },
    "laps": {
        "type": "race",
        "url_template": "{season}/{round}/laps.json",
        "pagination": True
    },

    # --- GROUP 4: Standings (Race-Level Dependency) ---
    "driverstandings": {
        "type": "race",
        "url_template": "{season}/{round}/driverstandings.json",
        "pagination": True
    },
    "constructorstandings": {
        "type": "race",
        "url_template": "{season}/{round}/constructorstandings.json",
        "pagination": True
    },
}