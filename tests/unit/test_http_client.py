from typing import cast
from unittest.mock import patch

from requests.adapters import HTTPAdapter
from requests_ratelimiter import LimiterSession

from f1_pipeline.ingestion.http_client import create_session


def test_create_session_returns_limiter_session(mock_env: dict[str, str]) -> None:
    """Test that create_session returns a properly configured LimiterSession."""
    session = create_session()
    assert isinstance(session, LimiterSession)

    # Check headers
    assert "User-Agent" in session.headers
    assert session.headers["Accept"] == "application/json"


def test_session_retries(mock_env: dict[str, str]) -> None:
    """Test that retry adapter is mounted."""
    with patch("f1_pipeline.ingestion.http_client.RETRY_MAX_ATTEMPTS", 1):
        session = create_session()
        # Cast to HTTPAdapter because get_adapter returns BaseAdapter but we know it's HTTPAdapter
        adapter = cast(HTTPAdapter, session.get_adapter("https://api.test.com"))

        # Check max retries configuration
        # Note: requests stores this in max_retries
        assert adapter.max_retries.total == 1
    assert adapter.max_retries.status_forcelist == [429, 500, 502, 503, 504]
