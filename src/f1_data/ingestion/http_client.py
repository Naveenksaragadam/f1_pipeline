# src/F1_PPIPELINE/ingestion/http_client.py

import requests
from .config import DEFAULT_HEADERS


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session