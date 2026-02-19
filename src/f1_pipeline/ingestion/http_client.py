# src/f1_pipeline/ingestion/http_client.py

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests_ratelimiter import LimiterSession
from ..config import (
    DEFAULT_HEADERS,
    API_RATE_PER_SEC,
    API_RATE_PER_MIN,
    API_BURST,
    RETRY_MAX_ATTEMPTS,
)


def create_session() -> requests.Session:
    """
    Creates a Session with:
    1. Rate Limiting (Client-side throttling)
    2. Automatic Retries (Server-side backoff respect)
    """
    # 1. Rate Limiter (Throttles outgoing requests)
    # Note: Uses memory backend by default, so it resets on script restart.
    session = LimiterSession(
        per_second=API_RATE_PER_SEC, per_minute=API_RATE_PER_MIN, burst=API_BURST
    )

    # 2. Retry Strategy (Handles 429s gracefully)
    # This logic runs INSIDE requests.get(). If we get a 429, it checks the
    # 'Retry-After' header and sleeps automatically before returning control.
    retry_strategy = Retry(
        total=RETRY_MAX_ATTEMPTS,  # Maximum number of retries
        backoff_factor=1,  # Exponential backoff: 0s, 2s, 4s, 8s... (if no Retry-After header)
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these errors
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # Only retry safe methods
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Mount the adapter to both HTTP and HTTPS
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(DEFAULT_HEADERS)
    return session
