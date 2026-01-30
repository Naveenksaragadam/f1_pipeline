
import pytest
from unittest.mock import MagicMock, patch, call, ANY
from f1_pipeline.ingestion.ingestor import F1DataIngestor

@pytest.fixture
def mock_session():
    mock = MagicMock()
    mock.get.return_value.status_code = 200
    mock.get.return_value.json.return_value = {
        "MRData": {
            "total": "20",  # Default: 1 page
            "RaceTable": {"Races": []}
        }
    }
    return mock

@pytest.fixture
def ingestor(mock_env, mock_s3_client, mock_session):
    with patch("f1_pipeline.ingestion.ingestor.F1ObjectStore") as MockStore:
        MockStore.return_value.client = mock_s3_client
        # Mock existence check to return False by default (so we act as new)
        MockStore.return_value.list_objects.return_value = []
        MockStore.return_value.object_exists.return_value = False
        
        ingestor = F1DataIngestor(session=mock_session, validate_connection=False)
        ingestor.store = MockStore.return_value # Ensure we use the mocked instance
        return ingestor

def test_ingest_pagination_sequential(ingestor, mock_session):
    """Test standard sequential pagination logic."""
    # Setup: 25 items, limit 10 => 3 pages (10, 10, 5)
    mock_session.get.return_value.json.side_effect = [
        {"MRData": {"total": "25"}}, # Page 1 (initial check)
        # Sequence of fetches
        {"MRData": {"data": "page2"}}, # Page 2
        {"MRData": {"data": "page3"}}, # Page 3
    ]
    
    # Patch DEFAULT_LIMIT to 10 so 25 records = 3 pages
    with patch("f1_pipeline.ingestion.ingestor.DEFAULT_LIMIT", 10):
        ingestor.ingest_endpoint("drivers", batch_id="b1", season=2024, max_workers=1)
    
    # Verify calls
    # Call 1: Page 1 (offset 0) to get total
    # Call 2: Page 2 (offset 10)
    # Call 3: Page 3 (offset 20)
    assert mock_session.get.call_count == 3
    
def test_ingest_concurrent_workers(ingestor, mock_session):
    """Test that max_workers > 1 triggers ThreadPoolExecutor logic."""
    # Setup: 50 items, limit 10 => 5 pages.
    # Page 1 handled sequentially. Pages 2-5 handled concurrently.
    mock_session.get.return_value.json.return_value = {"MRData": {"total": "50"}}
    
    with patch("f1_pipeline.ingestion.ingestor.ThreadPoolExecutor") as MockExecutor:
        # We need the context manager to yield the executor mock
        mock_executor_instance = MockExecutor.return_value.__enter__.return_value
        
        # When submit is called, return a future with success
        mock_future = MagicMock()
        mock_executor_instance.submit.return_value = mock_future
        
        # Patch limit to force pagination
        with patch("f1_pipeline.ingestion.ingestor.DEFAULT_LIMIT", 10):
            ingestor.ingest_endpoint("drivers", batch_id="b1", season=2024, max_workers=3)
        
        # Verify Executor was initialized with max_workers=3
        MockExecutor.assert_called_with(max_workers=3)
        # Verify 4 tasks submitted (Pages 2, 3, 4, 5)
        assert mock_executor_instance.submit.call_count == 4

def test_sprint_season_filtering(ingestor, mock_session):
    """Test that 'sprint' endpoint is only added for seasons >= 2021."""
    # Setup race calendar with 1 race
    races_data = {
        "MRData": {
            "RaceTable": {
                "Races": [{"round": "1", "raceName": "Test GP"}]
            }
        }
    }
    
    # Mock bronze read failure to force API fallback
    ingestor.store.get_json.side_effect = Exception("Not found")
    mock_session.get.return_value.json.return_value = races_data
    
    # Test 2020 (No Sprint)
    with patch.object(ingestor, 'ingest_endpoint') as mock_ingest:
        ingestor.run_full_extraction(season=2020, batch_id="b1")
        # Check calls. "sprint" should NOT be in the call args
        endpoints_called = [call[0][0] for call in mock_ingest.call_args_list]
        assert "sprint" not in endpoints_called
        assert "results" in endpoints_called

    # Test 2021 (Has Sprint)
    with patch.object(ingestor, 'ingest_endpoint') as mock_ingest:
        ingestor.run_full_extraction(season=2021, batch_id="b1")
        endpoints_called = [call[0][0] for call in mock_ingest.call_args_list]
        assert "sprint" in endpoints_called

def test_race_calendar_bronze_fallback(ingestor, mock_session):
    """Test logic: Read from Bronze -> Fail -> Fallback to API."""
    # 1. Setup Bronze Read Failure
    ingestor.store.get_json.side_effect = Exception("S3 error")
    
    # 2. Setup API Success
    mock_session.get.return_value.json.return_value = {
        "MRData": {"RaceTable": {"Races": [{"round": "1"}]}}
    }
    
    # 3. patch ingest_endpoint to prevent actual ingestion recursion
    with patch.object(ingestor, 'ingest_endpoint'):
        ingestor.run_full_extraction(season=2024, batch_id="b1")
    
    # Verify API was called for calendar
    # URL should be .../2024.json
    args, _ = mock_session.get.call_args
    assert "2024.json" in args[0]
