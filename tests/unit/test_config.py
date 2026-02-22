import logging
from unittest.mock import patch

import pytest

from f1_pipeline import config


def test_config_validation(mock_env: dict[str, str]) -> None:
    """Test that valid configuration passes validation."""
    assert config.validate_configuration() is True


def test_get_env_required_success() -> None:
    """Test get_env_required with present variable."""
    with patch("os.getenv", return_value="test_value"):
        assert config.get_env_required("TEST_KEY") == "test_value"


def test_config_reload_no_env(caplog: pytest.LogCaptureFixture) -> None:
    """Test module initialization when .env is not found (covers line 21)."""
    import importlib

    caplog.set_level(logging.INFO)

    # Patch at the source so the 'from dotenv import load_dotenv' pulls the mock
    with patch("dotenv.load_dotenv", return_value=False):
        # Reload to trigger the 'if not _env_found' block
        importlib.reload(config)

    # Verify the log was produced
    assert any("No .env file found" in record.message for record in caplog.records)


def test_validate_configuration_explicit_call(caplog: pytest.LogCaptureFixture) -> None:
    """Test that validate_configuration() can be explicitly called by a startup entrypoint.

    The module no longer auto-calls this at import time (removed fragile pytest guard).
    Callers (DAGs, scripts) are expected to call it once explicitly on startup.
    """
    caplog.set_level(logging.INFO)
    result = config.validate_configuration()
    assert result is True
    assert any("Configuration validation passed" in r.message for r in caplog.records)


def test_get_env_required_failure() -> None:
    """Test get_env_required with missing variable."""
    with patch("os.getenv", return_value=None):
        with pytest.raises(EnvironmentError) as excinfo:
            config.get_env_required("MISSING_KEY")
        assert "Required environment variable 'MISSING_KEY' is not set" in str(excinfo.value)


def test_validate_configuration_missing_access_key() -> None:
    """Test validation failure when MINIO_ACCESS_KEY is missing."""
    with patch("f1_pipeline.config.MINIO_ACCESS_KEY", ""):
        with pytest.raises(EnvironmentError) as excinfo:
            config.validate_configuration()
        assert "MINIO_ACCESS_KEY is not set" in str(excinfo.value)


def test_validate_configuration_missing_secret_key() -> None:
    """Test validation failure when MINIO_SECRET_KEY is missing."""
    with patch("f1_pipeline.config.MINIO_SECRET_KEY", None):
        with pytest.raises(EnvironmentError) as excinfo:
            config.validate_configuration()
        assert "MINIO_SECRET_KEY is not set" in str(excinfo.value)


def test_validate_configuration_empty_endpoints() -> None:
    """Test validation failure when ENDPOINT_CONFIG is empty."""
    with patch("f1_pipeline.config.ENDPOINT_CONFIG", {}):
        with pytest.raises(EnvironmentError) as excinfo:
            config.validate_configuration()
        assert "ENDPOINT_CONFIG is empty" in str(excinfo.value)

    bad_config = {"bad": {"group": "error"}}  # Missing url_pattern
    with patch("f1_pipeline.config.ENDPOINT_CONFIG", bad_config):
        with pytest.raises(EnvironmentError) as excinfo:
            config.validate_configuration()
        assert "missing url_pattern" in str(excinfo.value)

    bad_config = {"bad": {"url_pattern": "test"}}  # Missing group
    with patch("f1_pipeline.config.ENDPOINT_CONFIG", bad_config):
        with pytest.raises(EnvironmentError) as excinfo:
            config.validate_configuration()
        assert "missing group" in str(excinfo.value)


def test_concurrency_config(mock_env: dict[str, str]) -> None:
    """Test that MAX_CONCURRENT_WORKERS is loaded correctly."""
    assert hasattr(config, "MAX_CONCURRENT_WORKERS")
