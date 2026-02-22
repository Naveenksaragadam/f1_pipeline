from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
import requests

from f1_pipeline.ingestion.ingestor import F1DataIngestor


@pytest.fixture
def mock_session() -> MagicMock:
    mock = MagicMock()
    mock.get.return_value.status_code = 200
    mock.get.return_value.json.return_value = {
        "MRData": {
            "total": "20",  # Default: 1 page
            "RaceTable": {"Races": [{"dummy": "data"}]},
        }
    }
    return mock


@pytest.fixture
def ingestor(mock_env: Any, mock_s3_client: MagicMock, mock_session: MagicMock) -> F1DataIngestor:
    with patch("f1_pipeline.ingestion.ingestor.F1ObjectStore") as MockStore:
        MockStore.return_value.client = mock_s3_client
        # Mock existence check to return False by default (so we act as new)
        MockStore.return_value.list_objects.return_value = []
        MockStore.return_value.object_exists.return_value = False

        ingestor = F1DataIngestor(session=mock_session, validate_connection=False)
        ingestor.store = MockStore.return_value  # Ensure we use the mocked instance
        return ingestor


def test_validate_infrastructure_success(mock_env: Any, mock_session: MagicMock) -> None:
    """Test successful infrastructure validation."""
    with patch("f1_pipeline.ingestion.ingestor.F1ObjectStore") as MockStore:
        F1DataIngestor(session=mock_session, validate_connection=True)
        MockStore.return_value.create_bucket_if_not_exists.assert_called_once()


def test_validate_infrastructure_failure(mock_env: Any, mock_session: MagicMock) -> None:
    """Test failed infrastructure validation."""
    with patch("f1_pipeline.ingestion.ingestor.F1ObjectStore") as MockStore:
        MockStore.return_value.create_bucket_if_not_exists.side_effect = Exception("Conn error")
        with pytest.raises(RuntimeError) as excinfo:
            F1DataIngestor(session=mock_session, validate_connection=True)
        assert "Cannot connect to MinIO" in str(excinfo.value)


def test_generate_path_with_round(ingestor: F1DataIngestor) -> None:
    """Test path generation including round number."""
    path = ingestor._generate_path("results", "b1", 2024, 1, 1)
    assert "round=01" in path
    assert "page_001.json" in path


def test_get_existing_keys_with_round_and_error(ingestor: F1DataIngestor) -> None:
    """Test key listing with round and error handling."""
    ingestor.store.list_objects.return_value = ["key1"]  # type: ignore[attr-defined]
    keys = ingestor._get_existing_keys("results", "b1", 2024, 1)
    assert "key1" in keys
    ingestor.store.list_objects.assert_called_with(  # type: ignore[attr-defined]
        prefix="ergast/endpoint=results/season=2024/round=01/batch_id=b1/"
    )

    # Test error fallback
    ingestor.store.list_objects.side_effect = Exception("S3 list error")  # type: ignore[attr-defined]
    keys = ingestor._get_existing_keys("results", "b1", 2024, 1)
    assert keys == set()


def test_save_to_minio_error(ingestor: F1DataIngestor) -> None:
    """Test error handling in _save_to_minio."""
    ingestor.store.put_object.side_effect = Exception("Write error")  # type: ignore[attr-defined]
    with pytest.raises(Exception, match="Write error"):
        ingestor._save_to_minio(
            {"data": 1}, "path", {"batch_id": "b1", "endpoint": "e1", "ingestion_timestamp": "t1"}
        )
    assert ingestor.stats["errors_encountered"] == 1


def test_fetch_page_errors(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test various API error scenarios in fetch_page."""
    # 1. RequestException
    mock_session.get.side_effect = requests.RequestException("API error")

    # The @retry decorator makes it hard to patch RETRY_MAX_ATTEMPTS or the strategy.
    # We'll patch the instance method to return the undecorated one for this test
    # if it has a __wrapped__ attribute, or just mock the decorator.
    # Actually, we can just use patch.dict on sys.modules to mock tenacity.retry
    # but that's overkill. Let's just patch the method's retry behavior.

    # Simple fix: patch the fetch_page method on the instance to avoid retries
    with patch("f1_pipeline.ingestion.ingestor.stop_after_attempt", return_value=MagicMock()):
        with patch.object(
            ingestor, "fetch_page", side_effect=requests.RequestException("API error")
        ):
            with pytest.raises(requests.RequestException):
                ingestor.fetch_page("http://test", 10, 0)
    # The above doesn't hit the internal catch blocks of fetch_page though.
    # To hit catch blocks:
    mock_session.get.side_effect = requests.RequestException("API error")
    with patch(
        "f1_pipeline.ingestion.ingestor.RETRY_MAX_ATTEMPTS", 1
    ):  # This is used by stop_after_attempt(RETRY_MAX_ATTEMPTS)
        # However, the decorator is already applied.
        # Let's just use a shortcut: call the .__wrapped__ if it's there? No.
        # We'll just patch the retry itself in the module for all tests.
        pass

    # NEW APPROACH: Patch the retry strategy *completely* at import time? No.
    # Let's just use a side effect that triggers the error and then check stats.
    with patch("f1_pipeline.ingestion.ingestor.RETRY_STRATEGY", lambda x: x):  # Null decorator
        # But ingestor is already loaded.
        pass

    # OK, let's just make the test wait a bit if needed, or better,
    # use a mock that calls the original but without the decorator.
    with patch.object(
        ingestor,
        "fetch_page",
        side_effect=ingestor.fetch_page.__wrapped__.__get__(ingestor, F1DataIngestor),  # type: ignore[attr-defined]
    ):
        # Reset stats to be sure
        ingestor._reset_stats()
        mock_session.get.side_effect = requests.RequestException("API error")
        with pytest.raises(requests.RequestException):
            ingestor.fetch_page("http://test", 10, 0)
        assert ingestor.stats["errors_encountered"] >= 1

        mock_session.get.side_effect = None
        mock_session.get.return_value.json.side_effect = ValueError("Bad JSON")
        with pytest.raises(ValueError):
            ingestor.fetch_page("http://test", 10, 0)
        assert ingestor.stats["errors_encountered"] >= 2


def test_fetch_and_save_page_skip(ingestor: F1DataIngestor) -> None:
    """Test skipping logic in _fetch_and_save_page."""
    # 1. Skip via existing_keys
    success, total = ingestor._fetch_and_save_page(
        "drivers",
        "b1",
        "url",
        2024,
        None,
        1,
        10,
        0,
        False,
        existing_keys={"ergast/endpoint=drivers/season=2024/batch_id=b1/page_001.json"},
    )
    assert success is True
    assert total == 0
    assert ingestor.stats["files_skipped"] == 1

    # 2. Skip via store.object_exists
    ingestor.store.object_exists.return_value = True  # type: ignore[attr-defined]
    success, total = ingestor._fetch_and_save_page(
        "drivers", "b2", "url", 2024, None, 1, 10, 0, False, existing_keys=None
    )
    assert success is True
    assert ingestor.stats["files_skipped"] == 2


def test_fetch_and_save_page_error(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test error in _fetch_and_save_page."""
    mock_session.get.side_effect = Exception("Fatal error")
    success, total = ingestor._fetch_and_save_page(
        "drivers", "b1", "url", 2024, None, 1, 10, 0, False
    )
    assert success is False
    assert ingestor.stats["errors_encountered"] == 1


def test_ingest_endpoint_invalid_config(ingestor: F1DataIngestor) -> None:
    """Test ingesting unknown endpoint."""
    with pytest.raises(ValueError) as excinfo:
        ingestor.ingest_endpoint("unknown", "b1")
    assert "Unknown endpoint" in str(excinfo.value)


def test_silent_data_corruption_exception(
    ingestor: F1DataIngestor, mock_session: MagicMock
) -> None:
    """Test ValueError is caught internally when API reports total > 0 but payload array is empty."""
    # API claims 100 records but provides no actual items
    mock_session.get.return_value.json.return_value = {
        "MRData": {"total": "100", "RaceTable": {"Races": []}}
    }

    # We need to bypass the fetch_page retry logic for a clean test so we call _fetch_and_save
    # _fetch_and_save_page catches all exceptions and logs them
    success, count = ingestor._fetch_and_save_page(
        "races", "b1", "url", 2024, None, 1, 10, 0, False
    )

    # It should fail and return (False, 0)
    assert not success
    assert count == 0
    assert ingestor.stats["errors_encountered"] == 1


def test_ingest_endpoint_first_page_fail(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test failure when fetching the first page."""
    # Make _fetch_and_save_page return False
    with patch.object(ingestor, "_fetch_and_save_page", return_value=(False, 0)):
        with pytest.raises(Exception) as excinfo:
            ingestor.ingest_endpoint("drivers", "b1")
        assert "Failed to fetch first page" in str(excinfo.value)


def test_ingest_pagination_edge_cases(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test edge cases: no pagination or zero records."""
    # 1. Non-paginated (e.g. races)
    mock_session.get.return_value.json.return_value = {
        "MRData": {"total": "100", "RaceTable": {"Races": [{"dummy": "data"}]}}
    }
    ingestor.ingest_endpoint("races", "b1", season=2024)
    assert mock_session.get.call_count == 1  # Only first page

    # 2. Zero records
    mock_session.get.return_value.json.return_value = {"MRData": {"total": "0"}}
    ingestor.ingest_endpoint("drivers", "b1", season=2024)
    assert mock_session.get.call_count == 2  # Once more after reset? No, total 2 calls now.


def test_ingest_concurrent_worker_failure(
    ingestor: F1DataIngestor, mock_session: MagicMock
) -> None:
    """Test handling of failed worker in concurrent ingestion."""
    mock_session.get.return_value.json.return_value = {
        "MRData": {"total": "50", "table": {"items": [{"id": 1}]}}
    }

    with patch("f1_pipeline.ingestion.ingestor.ThreadPoolExecutor") as MockExecutor:
        mock_executor_instance = MockExecutor.return_value.__enter__.return_value

        # Scenario 1: Success returns (False, 0)
        mock_future_fail = MagicMock()
        mock_future_fail.result.return_value = (False, 0)

        # Scenario 2: Exception raised during result()
        mock_future_exc = MagicMock()
        mock_future_exc.result.side_effect = Exception("Worker died")

        mock_executor_instance.submit.side_effect = [
            mock_future_fail,
            mock_future_exc,
            mock_future_fail,
            mock_future_fail,
        ]

        with patch("f1_pipeline.ingestion.ingestor.DEFAULT_LIMIT", 10):
            ingestor.ingest_endpoint("drivers", "b1", season=2024, max_workers=3)


def test_ingest_sequential_skipping(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test skipping files in sequential ingestion."""
    mock_session.get.return_value.json.return_value = {
        "MRData": {"total": "20", "table": {"items": [{"id": 1}]}}
    }
    # Page 1 key (unused but kept as comment for clarity if needed)
    # page1_key = ingestor._generate_path("drivers", "b1", 2024, None, 1)
    # Page 2 key
    page2_key = ingestor._generate_path("drivers", "b1", 2024, None, 2)

    ingestor.store.list_objects.return_value = [page2_key]  # type: ignore[attr-defined]

    with patch("f1_pipeline.ingestion.ingestor.DEFAULT_LIMIT", 10):
        ingestor.ingest_endpoint("drivers", "b1", season=2024, max_workers=1)

    # Page 1 should be fetched (total=20)
    # Page 2 should be skipped if round_num logic lines up.
    assert ingestor.stats["files_skipped"] >= 1


def test_run_full_extraction_success(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test full extraction with Bronze success path."""
    # 1. Mock Bronze read success
    races_envelope = {
        "data": {"MRData": {"RaceTable": {"Races": [{"round": "1", "raceName": "GP1"}]}}}
    }
    ingestor.store.get_json.return_value = races_envelope  # type: ignore[attr-defined]

    # 2. Mock ingest_endpoint to do nothing
    with patch.object(ingestor, "ingest_endpoint"):
        summary = ingestor.run_full_extraction(season=2024, batch_id="b1")
        assert summary["status"] == "SUCCESS"


def test_run_full_extraction_zero_rounds(ingestor: F1DataIngestor) -> None:
    """Test extraction when zero rounds found."""
    ingestor.store.get_json.return_value = {"data": {"MRData": {"RaceTable": {"Races": []}}}}  # type: ignore[attr-defined]
    with patch.object(ingestor, "ingest_endpoint"):
        summary = ingestor.run_full_extraction(season=2024, batch_id="b1")
        assert summary["files_written"] == 0


def test_run_full_extraction_failure(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test extraction failure handling."""
    # Force failure in one of the phases
    with patch.object(ingestor, "ingest_endpoint", side_effect=Exception("Major failure")):
        with pytest.raises(Exception, match="Major failure"):
            ingestor.run_full_extraction(season=2024, batch_id="b1")
    # run_full_extraction re-raises but doesn't increment stats itself


def test_save_to_minio_snapshot_logging(ingestor: F1DataIngestor) -> None:
    """Trigger the snapshot logging in _save_to_minio."""
    # We need files_written % 10 == 0
    ingestor.stats["files_written"] = 9
    ingestor._save_to_minio(
        {"data": 1}, "path", {"batch_id": "b1", "endpoint": "e1", "ingestion_timestamp": "t1"}
    )
    assert ingestor.stats["files_written"] == 10


def test_ingest_pagination_sequential(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test standard sequential pagination logic."""
    # Setup: 25 items, limit 10 => 3 pages (10, 10, 5)
    mock_session.get.return_value.json.side_effect = [
        {"MRData": {"total": "25", "table": {"items": [{"id": 1}]}}},  # Page 1
        {"MRData": {"total": "25", "table": {"items": [{"id": 2}]}}},  # Page 2
        {"MRData": {"total": "25", "table": {"items": [{"id": 3}]}}},  # Page 3
    ]

    with patch("f1_pipeline.ingestion.ingestor.DEFAULT_LIMIT", 10):
        ingestor.ingest_endpoint("drivers", batch_id="b1", season=2024, max_workers=1)

    assert mock_session.get.call_count == 3


def test_ingest_concurrent_workers(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test that max_workers > 1 triggers ThreadPoolExecutor logic."""
    mock_session.get.return_value.json.return_value = {
        "MRData": {"total": "50", "table": {"items": [{"id": 1}]}}
    }

    with patch("f1_pipeline.ingestion.ingestor.ThreadPoolExecutor") as MockExecutor:
        mock_executor_instance = MockExecutor.return_value.__enter__.return_value
        mock_future = MagicMock()
        mock_executor_instance.submit.return_value = mock_future
        mock_future.result.return_value = (True, 50)

        with patch("f1_pipeline.ingestion.ingestor.DEFAULT_LIMIT", 10):
            ingestor.ingest_endpoint("drivers", batch_id="b1", season=2024, max_workers=3)

        MockExecutor.assert_called_with(max_workers=3)
        assert mock_executor_instance.submit.call_count == 4


def test_sprint_season_filtering(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test that 'sprint' endpoint is only added when Sprint is in the race schedule."""
    races_data_no_sprint = {"MRData": {"RaceTable": {"Races": [{"round": "1", "raceName": "T"}]}}}
    races_data_with_sprint = {
        "MRData": {
            "RaceTable": {
                "Races": [{"round": "1", "raceName": "T", "Sprint": {"date": "2024-04-20"}}]
            }
        }
    }
    cast(MagicMock, ingestor.store.get_json).side_effect = Exception("Not found")

    with patch.object(ingestor, "ingest_endpoint") as mock_ingest:
        mock_session.get.return_value.json.return_value = races_data_no_sprint
        ingestor.run_full_extraction(season=2024, batch_id="b1")
        endpoints_called = [call[0][0] for call in mock_ingest.call_args_list]
        assert "sprint" not in endpoints_called

    with patch.object(ingestor, "ingest_endpoint") as mock_ingest:
        mock_session.get.return_value.json.return_value = races_data_with_sprint
        ingestor.run_full_extraction(season=2024, batch_id="b1")
        endpoints_called = [call[0][0] for call in mock_ingest.call_args_list]
        assert "sprint" in endpoints_called


def test_race_calendar_bronze_fallback(ingestor: F1DataIngestor, mock_session: MagicMock) -> None:
    """Test logic: Read from Bronze -> Fail -> Fallback to API."""
    cast(MagicMock, ingestor.store.get_json).side_effect = Exception("S3 error")
    mock_session.get.return_value.json.return_value = {
        "MRData": {"RaceTable": {"Races": [{"round": "1"}]}}
    }

    with patch.object(ingestor, "ingest_endpoint"):
        ingestor.run_full_extraction(season=2024, batch_id="b1")

    args, _ = mock_session.get.call_args
    assert "2024.json" in args[0]
