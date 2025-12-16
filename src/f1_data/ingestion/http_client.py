# src/F1_PPIPELINE/ingestion/http_client.py

import requests
from .config import DEFAULT_HEADERS
from requests_ratelimiter import LimiterSession


def create_session() -> requests.Session:
    session = LimiterSession(
        per_second=4, 
        per_hour=500
    )
    session.headers.update(DEFAULT_HEADERS)
    return session