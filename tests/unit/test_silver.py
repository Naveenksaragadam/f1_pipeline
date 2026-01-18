
import pytest
from unittest.mock import MagicMock, patch, ANY
import polars as pl
import io
from f1_data.transform.silver import SilverProcessor

@pytest.fixture
def mock_stores():
    mock_bronze = MagicMock()
    mock_silver = MagicMock()
    return mock_bronze, mock_silver

@pytest.fixture
def processor(mock_stores):
    mock_bronze, mock_silver = mock_stores
    # We patch the init to avoid creating real clients, or we pass mocks if we refactored init.
    # The current init creates generic clients if not passed. 
    # But we can inject them if we subclass or patch.
    # Easier: Just pass one store to init (the Second arg) and patch the First arg creation inside Init?
    # Or better: Construct generic, then overwrite attributes.
    
    with patch("f1_data.transform.silver.F1ObjectStore") as MockStoreClass:
        # This mocks the calls inside __init__
        # But we want to control the instances.
        
        # Strategy: Let's manually set the attributes after creation if possible,
        # or use dependency injection pattern fully.
        # The current code:
        # self.bronze_store = F1ObjectStore(...)
        # self.silver_store = store or F1ObjectStore(...)
        
        MockStoreClass.return_value = mock_bronze # Default return
        
        # If we pass a store, silver_store uses that.
        proc = SilverProcessor(store=mock_silver)
        
        # Fix: The init called F1ObjectStore() for bronze_store.
        # So proc.bronze_store is the mock_bronze (from return_value)
        # proc.silver_store is mock_silver (passed arg)
        
        return proc

def test_extract_inner_data_drivers(processor):
    """Test flattening of DriverTable."""
    data = {
        "DriverTable": {
            "Drivers": [
                {"driverId": "hamilton", "givenName": "Lewis"},
                {"driverId": "bottas", "givenName": "Valtteri"}
            ]
        }
    }
    result = processor._extract_inner_data("drivers", data)
    assert len(result) == 2
    assert result[0]["driverId"] == "hamilton"

def test_extract_inner_data_results(processor):
    """Test flattening of RaceTable -> Races -> Results."""
    data = {
        "RaceTable": {
            "Races": [
                {
                    "season": "2024",
                    "round": "1",
                    "raceName": "Bahrain",
                    "Results": [
                        {"position": "1", "driverId": "max"},
                        {"position": "2", "driverId": "checo"}
                    ]
                }
            ]
        }
    }
    result = processor._extract_inner_data("results", data)
    assert len(result) == 2
    assert result[0]["raceName"] == "Bahrain"
    assert result[0]["driverId"] == "max" # Flattened
    assert "Results" not in result[0] # Removed nested list

def test_extract_inner_data_laps(processor):
    """Test flattening of Laps (Deep nesting)."""
    data = {
        "RaceTable": {
            "Races": [
                {
                    "raceName": "Test GP",
                    "Laps": [
                        {
                            "number": "1",
                            "Timings": [
                                {"driverId": "d1", "time": "1:30"},
                                {"driverId": "d2", "time": "1:31"}
                            ]
                        }
                    ]
                }
            ]
        }
    }
    result = processor._extract_inner_data("laps", data)
    assert len(result) == 2
    assert result[0]["raceName"] == "Test GP"
    assert result[0]["lap"] == "1"
    assert result[0]["driverId"] == "d1"

def test_process_batch_flow(processor):
    """Test the full process_batch flow."""
    # 1. Setup Bronze Mock
    processor.bronze_store.list_objects.return_value = ["file1.json"]
    
    # Mock return data
    mock_json = {
        "metadata": {},
        "data": {
            "MRData": {
                "DriverTable": {
                    "Drivers": [{"driverId": "d1"}, {"driverId": "d1"}] # Duplicate for testing unique
                }
            }
        }
    }
    processor.bronze_store.get_json.return_value = mock_json

    # 2. Run Process
    processor.process_batch("batch1", 2024, "drivers")
    
    # 3. Verify Bronze Read
    processor.bronze_store.list_objects.assert_called()
    processor.bronze_store.get_json.assert_called_with("file1.json")
    
    # 4. Verify Silver Write
    processor.silver_store.put_object.assert_called_once()
    call_args = processor.silver_store.put_object.call_args[1]
    
    assert call_args["key"] == "drivers/season=2024/batch1.parquet"
    assert "rows" in call_args["metadata"]
    # Should be 1 row if deduplication works (d1 vs d1 duplicate)
    # Wait, unique() dedups entire rows.
    # {"driverId": "d1"} == {"driverId": "d1"}
    assert call_args["metadata"]["rows"] == "1" 
    
    # Verify Content (Parquet)
    # We can't easily read the bytes buffer mock without pyarrow, 
    # but the presence of the call + metadata is good enough for unit logic.
