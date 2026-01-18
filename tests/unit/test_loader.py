
import pytest
from unittest.mock import MagicMock, patch, mock_open
from f1_data.analytics.loader import ClickHouseLoader

@pytest.fixture
def mock_client():
    mock = MagicMock()
    return mock

@pytest.fixture
def loader(mock_client):
    with patch("clickhouse_connect.get_client", return_value=mock_client):
        loader = ClickHouseLoader()
        return loader

def test_init_connection_success(mock_client):
    """Test successful connection initialization."""
    with patch("clickhouse_connect.get_client", return_value=mock_client) as mock_connect:
        ClickHouseLoader()
        mock_connect.assert_called_once()
        mock_client.command.assert_called_with("CREATE DATABASE IF NOT EXISTS f1_analytics")

def test_setup_schema(loader):
    """Test DDL execution."""
    mock_ddl = "CREATE TABLE t1; CREATE TABLE t2;"
    with patch("builtins.open", mock_open(read_data=mock_ddl)):
        loader.setup_schema()
        
    # Should execute non-empty statements
    assert loader.client.command.call_count == 2 + 1 # +1 for init DB check
    # Check args.
    # Note: init calls command once. setup_schema calls it 2 times.
    # We can check specific calls.

def test_ingest_batch(loader):
    """Test ingestion query generation."""
    endpoint = "drivers"
    season = 2024
    batch_id = "batch123"
    
    loader.ingest_batch(endpoint, season, batch_id)
    
    # Verify command was called with INSERT INTO ... SELECT * FROM s3
    call_args = loader.client.command.call_args[0][0]
    
    assert "INSERT INTO f1_analytics.drivers" in call_args
    assert "SELECT * FROM s3(" in call_args
    assert f"silver/{endpoint}/season={season}/{batch_id}.parquet" in call_args
    assert "Parquet" in call_args

def test_ingest_batch_error(loader):
    """Test error handling in ingestion."""
    loader.client.command.side_effect = Exception("ClickHouse Error")
    
    with pytest.raises(Exception):
        loader.ingest_batch("drivers", 2024, "batch1")
