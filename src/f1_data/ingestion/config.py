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