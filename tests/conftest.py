import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_env(monkeypatch):
    """
    Sets up a mock environment for testing.
    Ensures no real API calls or S3 connections are attempted.
    """
    env_vars = {
        "MINIO_ENDPOINT": "http://mock-minio:9000",
        "MINIO_ACCESS_KEY": "mock_access",
        "MINIO_SECRET_KEY": "mock_secret",
        "MINIO_REGION": "us-east-1",
        "BUCKET_BRONZE": "test-bronze",
        "BUCKET_SILVER": "test-silver",
        "BUCKET_GOLD": "test-gold",
        "API_BASE_URL": "https://api.test.com",
        "MAX_CONCURRENT_WORKERS": "2",
        "API_PAGE_LIMIT": "10",
        "RETRY_MAX_ATTEMPTS": "1",
        "RETRY_MIN_WAIT": "0",
        "RETRY_MAX_WAIT": "1",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def mock_s3_client():
    """Returns a magic mock for boto3 client"""
    return MagicMock()
