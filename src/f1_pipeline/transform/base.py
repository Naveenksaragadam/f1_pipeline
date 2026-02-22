"""
F1 Transformer Engine
Provides a robust orchestration layer for transforming raw JSON from the Bronze layer
into typed, flattened Parquet files in the Silver layer.
"""

import io
import logging
from typing import Any

import polars as pl
from pydantic import ValidationError

from f1_pipeline.minio.object_store import F1ObjectStore
from f1_pipeline.transform.schemas import F1BaseModel

logger = logging.getLogger(__name__)


class F1Transformer:
    """
    Orchestrates the transformation of raw F1 data objects.

    Uses Pydantic for schema validation/cleaning and Polars for efficient
    columnar data manipulation and Parquet writing.
    """

    def __init__(
        self,
        bronze_store: F1ObjectStore,
        silver_store: F1ObjectStore,
        schema_class: type[F1BaseModel],
        error_threshold: float = 0.20,
    ):
        """
        Initialize the transformer with storage interfaces and a target schema.

        Args:
            bronze_store: Object store instance for the raw source bucket.
            silver_store: Object store instance for the transformed target bucket.
            schema_class: Pydantic model class to use for record validation.
            error_threshold: Maximum fraction of records that may fail validation
                before the pipeline aborts. Defaults to 0.20 (20%). Set lower for
                small batches (e.g. qualifying/standings) where any failure is suspicious.
        """
        self.bronze_store = bronze_store
        self.silver_store = silver_store
        self.schema_class = schema_class
        self.error_threshold = error_threshold

    def process_object(self, source_key: str, target_key: str) -> None:
        """
        Execute the full transformation pipeline for a single S3/MinIO object.

        Workflow:
        1. Load raw JSON from Bronze.
        2. Recursively find and extract the data records.
        3. Validate and clean records using Pydantic.
        4. Convert cleaned records to a Polars DataFrame.
        5. Explode any list columns (1-to-many relationships).
        6. Recursively flatten all nested structures with prefixes.
        7. Write the final flat table to Silver in Parquet format.

        Args:
            source_key: The path to the raw JSON file in the Bronze bucket.
            target_key: The path where the Parquet file should be saved in Silver.
        """
        # 1. Load raw JSON
        logger.info(f"📥 Loading raw JSON from: {source_key}")
        raw_data = self.bronze_store.get_json(source_key)

        # 2. Extract records
        records = self._extract_records(raw_data)
        if not records:
            logger.warning(f"⚠️ No records to process for {source_key}")
            return

        logger.info(f"📊 Extracted {len(records)} records. Moving to validation...")

        # 3 & 4. Validate and convert to Polars
        df = self.process_batch(records)

        # Data Quality Layer: Assert we haven't silently lost all records
        if df.is_empty():
            if len(records) > 0:
                raise ValueError(
                    f"Data Quality Error: Started with {len(records)} records but ended "
                    f"with an empty DataFrame after validation. This indicates a logic error."
                )

        # 5. Explode lists and Flatten structures
        logger.info(f"🧱 Processing complex structures for {target_key}...")
        df = self._process_complex_types(df)

        # Data Quality Layer: Post-transformation check
        if df.is_empty():
            raise ValueError("Data Quality Error: Transformation resulted in an empty DataFrame.")

        # 6. Write to Parquet
        logger.info(f"💾 Writing {df.height} rows to Silver Layer: {target_key}")

        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)

        self.silver_store.put_object(
            key=target_key,
            body=buffer,
            compress=False,  # Parquet has internal compression
        )
        logger.info(f"✅ Successfully transformed {source_key} -> {target_key}")

    def _extract_records(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Dynamically locate the actual data rows within the nested JSON payload.

        API responses from Ergast are often deeply nested (e.g., MRData -> RaceTable -> Races -> Results).
        This method uses a recursive heuristic to find lists of objects. It respects the
        nesting depth expected by the schema class.
        """
        data_root = raw_data.get("data", {}).get("MRData", raw_data)

        def _find_first_list_of_dicts(obj: Any) -> list[dict[str, Any]] | None:
            """Recursively find the first list whose first element is a dict.

            Returns early on the first match — avoids allocating all nested lists
            upfront. The outermost list of dicts is always our target because our
            Pydantic schemas handle internal nesting (e.g., LapSchema contains Timings).
            """
            if isinstance(obj, list) and obj and isinstance(obj[0], dict):
                return obj  # type: ignore[return-value]
            if isinstance(obj, dict):
                for value in obj.values():
                    result = _find_first_list_of_dicts(value)
                    if result is not None:
                        return result
            return None

        result = _find_first_list_of_dicts(data_root)
        return result if result is not None else []

    def process_batch(self, records: list[dict[str, Any]]) -> pl.DataFrame:
        """
        Validate records against the Pydantic schema and convert to Polars.

        Implements a 'Fail-Fast' strategy: If more than 20% of records fail
        validation, the pipeline crashes to prevent silent data loss.
        """
        valid_items = []
        error_details = []
        total_count = len(records)

        for i, record in enumerate(records):
            try:
                validated_obj = self.schema_class.model_validate(record)
                valid_items.append(validated_obj.model_dump(by_alias=False))
            except ValidationError as e:
                error_details.append(f"Record {i}: {str(e)}")
                if len(error_details) <= 3:
                    logger.debug(f"❌ Validation failure on record {i}: {e}")
                continue

        error_count = len(error_details)
        if error_count > 0:
            error_rate = error_count / total_count
            logger.warning(
                f"⚠️ Validation issues found: {error_count}/{total_count} records failed "
                f"({error_rate:.1%})"
            )

            # Failure Threshold: Crash if above the configured threshold, or if it's a
            # small batch with any errors (small batches like qualifying/standings have
            # fewer records and any failure is suspicious).
            if error_rate > self.error_threshold or (total_count < 5 and error_count > 0):
                logger.error("🛑 Error threshold exceeded! First 3 errors:")
                for err in error_details[:3]:
                    logger.error(f"   - {err}")
                raise ValueError(
                    f"Schema Enforcement Triggered: {error_count} validation errors found "
                    f"for {self.schema_class.__name__}. Check logs for details."
                )

        return pl.DataFrame(valid_items)

    def _process_complex_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Unified handler for exploding lists and unnesting structs.
        """
        curr_df = df

        while True:
            # 1. Handle List Explosion (1-to-many)
            # Find any columns that are lists (e.g., 'timings' in Laps, 'constructors' in Standings)
            list_cols = [
                name
                for name, dtype in zip(curr_df.columns, curr_df.dtypes, strict=True)
                if isinstance(dtype, pl.List)
            ]

            if list_cols:
                # Explode all list columns in a single Polars operation.
                # Safe because our schemas never have two independent list columns
                # on the same row — each schema has exactly one nested list at a time.
                logger.debug(f"💥 Exploding list columns: {list_cols}")
                curr_df = curr_df.explode(list_cols)
                continue

            # 2. Handle Struct Unnesting
            struct_cols = [
                name
                for name, dtype in zip(curr_df.columns, curr_df.dtypes, strict=True)
                if isinstance(dtype, pl.Struct)
            ]

            if not struct_cols:
                break

            for col in struct_cols:
                # Add prefix to the nested fields to avoid collisions
                dtype = curr_df.schema[col]
                if isinstance(dtype, pl.Struct):
                    new_field_names = [f"{col}_{f.name}" for f in dtype.fields]
                    curr_df = curr_df.with_columns(
                        pl.col(col).struct.rename_fields(new_field_names)
                    )

            curr_df = curr_df.unnest(struct_cols)

        return curr_df
