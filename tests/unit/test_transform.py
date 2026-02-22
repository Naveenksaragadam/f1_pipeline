import io
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from pydantic import Field

from f1_pipeline.transform.base import F1Transformer
from f1_pipeline.transform.factory import get_schema_for_endpoint
from f1_pipeline.transform.schemas import CleanStr, DriverSchema, F1BaseModel

# --- Mock Schemas for Testing ---


class NestedSchema(F1BaseModel):
    id: int
    name: str


class ListNestedSchema(F1BaseModel):
    id: int
    items: list[NestedSchema] = Field(alias="Items")


class SimpleSchema(F1BaseModel):
    driver_id: str = Field(alias="driverId")
    nationality: CleanStr


# --- Factory Tests ---


def test_get_schema_for_endpoint_success() -> None:
    """Test successful schema retrieval from factory."""
    schema = get_schema_for_endpoint("drivers")
    assert schema == DriverSchema


def test_get_schema_for_endpoint_case_insensitive() -> None:
    """Test case-insensitive schema retrieval."""
    schema = get_schema_for_endpoint("DRIVERS")
    assert schema == DriverSchema


def test_get_schema_for_endpoint_failure() -> None:
    """Test factory error handling for unknown endpoints."""
    with pytest.raises(ValueError, match="No transformation schema registered"):
        get_schema_for_endpoint("invalid")


# --- Schema & Validator Tests ---


def test_clean_str_validator() -> None:
    """Test the CleanStr validator (Title Case + Strip)."""

    class TestModel(F1BaseModel):
        val: CleanStr

    m = TestModel(val=" british  ")
    assert m.val == "British"

    m2 = TestModel(val="MONACO")
    assert m2.val == "Monaco"


def test_f1_base_model_config() -> None:
    """Test F1BaseModel configuration (extra ignore, populate by name)."""
    m = SimpleSchema(
        **{"driverId": "verstappen", "nationality": "dutch", "extra_field": "ignore_me"}
    )  # type: ignore
    assert m.driver_id == "verstappen"
    assert not hasattr(m, "extra_field")


# --- Transformer Tests ---


@pytest.fixture
def mock_stores() -> tuple[MagicMock, MagicMock]:
    return MagicMock(), MagicMock()


@pytest.fixture
def transformer(mock_stores: tuple[MagicMock, MagicMock]) -> F1Transformer:
    bronze, silver = mock_stores
    return F1Transformer(bronze, silver, SimpleSchema)


def test_extract_records_basic(transformer: F1Transformer) -> None:
    """Test basic record extraction from Ergast structure."""
    raw_data = {
        "MRData": {"DriverTable": {"Drivers": [{"driverId": "max", "nationality": "Dutch"}]}}
    }
    records = transformer._extract_records(raw_data)
    assert len(records) == 1
    assert records[0]["driverId"] == "max"


def test_extract_records_nested_lists(transformer: F1Transformer) -> None:
    """Test extraction with deeper nesting (e.g. Laps)."""
    raw_data = {"MRData": {"RaceTable": {"Races": [{"Laps": [{"number": "1", "Timings": []}]}]}}}
    records = transformer._extract_records(raw_data)
    assert len(records) == 1
    assert "Laps" in str(records)  # Heuristic finds the first list of dicts


def test_process_batch_success(transformer: F1Transformer) -> None:
    """Test batch processing with valid records."""
    records = [{"driverId": "max", "nationality": "Dutch"}]
    df = transformer.process_batch(records)
    assert df.height == 1
    assert df.columns == ["driver_id", "nationality"]


def test_process_batch_error_threshold(transformer: F1Transformer) -> None:
    """Test that batch processing fails if error threshold is exceeded."""
    # 3 records, 1 fails -> 33% error rate (> 20%)
    records = [
        {"driverId": "max", "nationality": "Dutch"},
        {"driverId": "lewis", "nationality": "British"},
        {"invalid": "record"},
    ]
    with pytest.raises(ValueError, match="Schema Enforcement Triggered"):
        transformer.process_batch(records)


def test_process_batch_small_batch_error(transformer: F1Transformer) -> None:
    """Test fail-fast for small batches with even one error."""
    records = [{"invalid": "record"}]
    with pytest.raises(ValueError, match="Schema Enforcement Triggered"):
        transformer.process_batch(records)


def test_process_complex_types_explosion() -> None:
    """Test list explosion logic."""
    transformer = F1Transformer(MagicMock(), MagicMock(), ListNestedSchema)
    df = pl.DataFrame([{"id": 1, "items": [{"id": 10, "name": "A"}, {"id": 11, "name": "B"}]}])

    # Cast manually since we aren't going through process_batch here
    df = df.with_columns(
        pl.col("items").cast(
            pl.List(pl.Struct([pl.Field("id", pl.Int64), pl.Field("name", pl.Utf8)]))
        )
    )

    flat_df = transformer._process_complex_types(df)

    # 1 row exploded into 2
    assert flat_df.height == 2
    assert "items_id" in flat_df.columns
    assert "items_name" in flat_df.columns
    assert flat_df["items_id"].to_list() == [10, 11]


def test_extract_records_no_lists(transformer: F1Transformer) -> None:
    """Test extraction when no lists are found in JSON."""
    raw_data = {"MRData": {"Other": "Data"}}
    records = transformer._extract_records(raw_data)
    assert len(records) == 0


def test_process_object_empty_df_warning(mock_stores: tuple[MagicMock, MagicMock]) -> None:
    """Test handling when records validate but result in an empty DF (edge case)."""
    bronze_store, silver_store = mock_stores
    transformer = F1Transformer(bronze_store, silver_store, SimpleSchema)

    # Mock extract_records to return something, but process_batch to return empty
    with patch.object(transformer, "_extract_records", return_value=[{"some": "data"}]):
        with patch.object(transformer, "process_batch", return_value=pl.DataFrame()):
            with pytest.raises(ValueError) as excinfo:
                transformer.process_object("source.json", "target.parquet")
            assert "Data Quality Error" in str(excinfo.value)
            silver_store.put_object.assert_not_called()


def test_process_object_empty_post_complex_types(mock_stores: tuple[MagicMock, MagicMock]) -> None:
    """Test Data Quality error when DataFrame becomes empty after explode/unnest operations."""
    bronze_store, silver_store = mock_stores
    transformer = F1Transformer(bronze_store, silver_store, SimpleSchema)

    # Mock extract_records to return something so we pass the first check
    with patch.object(transformer, "_extract_records", return_value=[{"some": "data"}]):
        # Mock process_batch to return a valid DF with 1 row so we pass the first empty check
        valid_df = pl.DataFrame({"some": ["data"]})
        with patch.object(transformer, "process_batch", return_value=valid_df):
            # Then mock _process_complex_types to somehow return an empty DataFrame
            with patch.object(transformer, "_process_complex_types", return_value=pl.DataFrame()):
                with pytest.raises(
                    ValueError, match="Transformation resulted in an empty DataFrame"
                ):
                    transformer.process_object("source.json", "target.parquet")


def test_process_object_full_workflow(mock_stores: tuple[MagicMock, MagicMock]) -> None:
    """Test the complete process_object workflow."""
    bronze_store, silver_store = mock_stores
    transformer = F1Transformer(bronze_store, silver_store, SimpleSchema)

    bronze_store.get_json.return_value = {
        "MRData": {"DriverTable": {"Drivers": [{"driverId": "max", "nationality": "dutch"}]}}
    }

    transformer.process_object("source.json", "target.parquet")

    # Verify MinIO interactions
    bronze_store.get_json.assert_called_once_with("source.json")
    silver_store.put_object.assert_called_once()

    # Check target key
    args, kwargs = silver_store.put_object.call_args
    assert kwargs["key"] == "target.parquet"
    assert isinstance(kwargs["body"], io.BytesIO)


def test_process_object_empty_records(mock_stores: tuple[MagicMock, MagicMock]) -> None:
    """Test graceful handling of empty source data."""
    bronze_store, silver_store = mock_stores
    transformer = F1Transformer(bronze_store, silver_store, SimpleSchema)

    bronze_store.get_json.return_value = {"MRData": {"Empty": []}}
    transformer.process_object("source.json", "target.parquet")

    silver_store.put_object.assert_not_called()

def test_process_batch_custom_threshold(mock_stores: tuple[MagicMock, MagicMock]) -> None:
    """Test that custom error threshold is respected."""
    bronze, silver = mock_stores
    # Set threshold to 50%
    transformer = F1Transformer(bronze, silver, SimpleSchema, error_threshold=0.50)

    # 6 records, 2 fail -> 33% error rate (< 50%)
    # total_count >= 5 to avoid the separate small-batch safety check
    records = [
        {"driverId": "max", "nationality": "Dutch"},
        {"driverId": "lewis", "nationality": "British"},
        {"driverId": "charles", "nationality": "Monegasque"},
        {"driverId": "lando", "nationality": "British"},
        {"invalid": "record 1"},
        {"invalid": "record 2"},
    ]
    # Should NOT raise because 33% < 50%
    df = transformer.process_batch(records)
    assert df.height == 4
