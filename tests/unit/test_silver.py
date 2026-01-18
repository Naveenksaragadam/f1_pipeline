
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
    with patch("f1_data.transform.silver.F1ObjectStore") as MockStoreClass:
        MockStoreClass.return_value = mock_bronze
        proc = SilverProcessor(store=mock_silver)
        return proc

def test_extract_inner_data_drivers(processor):
    """Test flattening of DriverTable with validation."""
    data = {
        "DriverTable": {
            "Drivers": [
                {
                    "driverId": "hamilton", 
                    "givenName": "Lewis", 
                    "familyName": "Hamilton",
                    "dateOfBirth": "1985-01-07",
                    "nationality": "British",
                    "url": "http://test"
                }
            ]
        }
    }
    result = processor._validate_and_extract("drivers", data)
    assert len(result) == 1
    assert result[0]["driver_id"] == "hamilton"

def test_extract_inner_data_results(processor):
    """Test flattening of RaceTable -> Races -> Results."""
    data = {"RaceTable": {"season": "2024", "Races": [{
        "season": "2024", "round": "1", "raceName": "Bahrain", "date": "2024-03-02", "url": "http://test",
        "Circuit": {"circuitId": "bahrain", "url": "http://", "circuitName": "Bahrain", "Location": {}},
        "Results": [{
            "number": "1", "position": "1", "positionText": "1", "points": "25", "grid": "1", "laps": "57", "status": "Finished",
            "Driver": {"driverId": "max", "givenName": "Max", "familyName": "Ver", "dateOfBirth": "1997-09-30", "nationality": "Dutch", "url": "ht"},
            "Constructor": {"constructorId": "red_bull", "url": "h", "name": "Red Bull", "nationality": "Austrian"}
        }]
    }]}}
    
    result = processor._validate_and_extract("results", data)
    assert len(result) == 1
    assert result[0]["race_name"] == "Bahrain"
    assert result[0]["driver"]["driver_id"] == "max"

def test_extract_validation_error(processor):
    """Test that invalid data raises or logs error."""
    # Missing required field 'driverId'
    data = {
        "DriverTable": {
            "Drivers": [{"givenName": "Unknown"}] 
        }
    }
    with pytest.raises(Exception): # The processor catches it, but _validate_and_extract raises it? 
        # Actually _validate_and_extract raises ValidationError, process_batch catches it.
        # Let's verify _validate_and_extract behavior directly.
         processor._validate_and_extract("drivers", data)

def test_process_batch_flow(processor):
    """Test the full process_batch flow."""
    # 1. Setup Bronze Mock
    processor.bronze_store.list_objects.return_value = ["file1.json"]
    
    mock_json = {
        "metadata": {},
        "data": {
            "MRData": {
                "DriverTable": {
                    "Drivers": [{
                        "driverId": "d1", "givenName": "D", "familyName": "O", 
                        "dateOfBirth": "2000-01-01", "nationality": "Test", "url": "u"
                    }] 
                }
            }
        }
    }
    processor.bronze_store.get_json.return_value = mock_json

    # 2. Run Process
    processor.process_batch("batch1", 2024, "drivers")
    
    # 3. Verify Silver Write
    processor.silver_store.put_object.assert_called_once()
    call_args = processor.silver_store.put_object.call_args[1]
    assert call_args["metadata"]["rows"] == "1"
