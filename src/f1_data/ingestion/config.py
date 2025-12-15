# src/F1_PPIPELINE/ingestion/config.py

BASE_URL = "https://api.jolpi.ca/ergast/f1"

DEFAULT_HEADERS = {
    "User-Agent": "F1-Data-Project/1.0",
    "Accept": "application/json"
}

DEFAULT_LIMIT = 30

BUCKET = 'raw'