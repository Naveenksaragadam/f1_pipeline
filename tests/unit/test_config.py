import sys
import logging
from unittest.mock import patch, MagicMock

import pytest

from f1_pipeline import config


def test_config_validation(mock_env: dict[str, str]) -> None:
    """Test that valid configuration passes validation."""
    assert config.validate_configuration() is True


def test_get_env_required_success() -> None:
    """Test get_env_required with present variable."""
    with patch("os.getenv", return_value="test_value"):
        assert config.get_env_required("TEST_KEY") == "test_value"


def test_get_env_required_default() -> None:
    """Test get_env_required with missing variable and default."""
    with patch("os.getenv", return_value="default"):
        assert config.get_env_required("TEST_KEY", default="default") == "default"


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


def test_config_import_validation(caplog: pytest.LogCaptureFixture) -> None:
    """Test module-level validation call when not in pytest (covers line 238)."""
    import importlib
    import sys
    caplog.set_level(logging.INFO)

    # Better: Patch sys.modules directly
    original_modules = sys.modules.copy()
    try:
        # Create a clean slate for the test
        temp_modules = sys.modules.copy()
        if "pytest" in temp_modules:
            del temp_modules["pytest"]
        
        with patch.object(sys, "modules", temp_modules):
            # We don't mock validate_configuration because it overwrites it on reload.
            # We just check the log it produces.
            importlib.reload(config)
            assert any("Configuration validation passed" in record.message for record in caplog.records)
    finally:
        sys.modules = original_modules


def test_get_env_required_failure() -> None:
    """Test get_env_required with missing variable and no default."""
    with patch("os.getenv", return_value=None):
        with pytest.raises(SystemExit) as excinfo:
            config.get_env_required("MISSING_KEY")
        assert excinfo.value.code == 1


def test_validate_configuration_missing_access_key() -> None:
    """Test validation failure when MINIO_ACCESS_KEY is missing."""
    with patch("f1_pipeline.config.MINIO_ACCESS_KEY", ""):
        with pytest.raises(SystemExit) as excinfo:
            config.validate_configuration()
        assert excinfo.value.code == 1


def test_validate_configuration_missing_secret_key() -> None:
    """Test validation failure when MINIO_SECRET_KEY is missing."""
    with patch("f1_pipeline.config.MINIO_SECRET_KEY", None):
        with pytest.raises(SystemExit) as excinfo:
            config.validate_configuration()
        assert excinfo.value.code == 1


def test_validate_configuration_empty_endpoints() -> None:
    """Test validation failure when ENDPOINT_CONFIG is empty."""
    with patch("f1_pipeline.config.ENDPOINT_CONFIG", {}):
        with pytest.raises(SystemExit) as excinfo:
            config.validate_configuration()
        assert excinfo.value.code == 1


def test_validate_configuration_bad_endpoint() -> None:
    """Test validation failure with malformed endpoint config."""
    bad_config = {"bad": {"group": "error"}}  # Missing url_pattern
    with patch("f1_pipeline.config.ENDPOINT_CONFIG", bad_config):
        with pytest.raises(SystemExit) as excinfo:
            config.validate_configuration()
        assert excinfo.value.code == 1

    bad_config = {"bad": {"url_pattern": "test"}}  # Missing group
    with patch("f1_pipeline.config.ENDPOINT_CONFIG", bad_config):
        with pytest.raises(SystemExit) as excinfo:
            config.validate_configuration()
        assert excinfo.value.code == 1


def test_concurrency_config(mock_env: dict[str, str]) -> None:
    """Test that MAX_CONCURRENT_WORKERS is loaded correctly."""
    assert hasattr(config, "MAX_CONCURRENT_WORKERS")
