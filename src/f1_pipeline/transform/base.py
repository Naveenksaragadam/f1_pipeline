"""
F1 Transformer Engine
Provides a robust orchestration layer for transforming raw JSON from the Bronze layer 
into typed, flattened Parquet files in the Silver layer.
"""

import io
import logging
from typing import Type, List, Dict, Any

import polars as pl

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
        schema_class: Type[F1BaseModel],
    ):
        """
        Initialize the transformer with storage interfaces and a target schema.

        Args:
            bronze_store: Object store instance for the raw source bucket.
            silver_store: Object store instance for the transformed target bucket.
            schema_class: Pydantic model class to use for record validation.
        """
        self.bronze_store = bronze_store
        self.silver_store = silver_store
        self.schema_class = schema_class

    def process_object(self, source_key: str, target_key: str) -> None:
        """
        Execute the full transformation pipeline for a single S3/MinIO object.

        Workflow:
        1. Load raw JSON from Bronze.
        2. Recursively find and extract the deepest data list.
        3. Validate and clean records using Pydantic.
        4. Convert cleaned records to a Polars DataFrame.
        5. Recursively flatten all nested structures with prefixes.
        6. Write the final flat table to Silver in Parquet format.

        Args:
            source_key: The path to the raw JSON file in the Bronze bucket.
            target_key: The path where the Parquet file should be saved in Silver.
        """
        # 1. Load raw JSON
        logger.info(f"📥 Loading raw JSON from: {source_key}")
        raw_data = self.bronze_store.get_json(source_key)

        # 2. Extract records (The 'Drivers' part or 'Results' part)
        records = self._extract_records(raw_data)
        if not records:
            logger.warning(f"⚠️ No records to process for {source_key}")
            return

        logger.info(f"📊 Extracted {len(records)} records. Moving to validation...")

        # 3 & 4. Validate and convert to Polars
        df = self.process_batch(records)

        # 5. Flatten the structure
        logger.info(f"🧱 Flattening nested structures for {target_key}...")
        df = self._flatten_dataframe(df)

        # 6. Write to Parquet
        logger.info(f"💾 Writing {df.height} rows to Silver Layer: {target_key}")

        # We use a memory buffer to avoid slow disk I/O
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)

        self.silver_store.put_object(
            key=target_key,
            body=buffer,
            compress=False,  # Parquet has internal compression (Snappy/Zstd)
        )
        logger.info(f"✅ Successfully transformed {source_key} -> {target_key}")

    def _extract_records(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Dynamically locate the actual data rows within the nested JSON payload.

        API responses from Ergast are often deeply nested (e.g., MRData -> RaceTable -> Races -> Results).
        This method uses a recursive heuristic to find the "deepest" list of objects, which
        is typically where the actual data records live.

        Args:
            raw_data: The full JSON payload retrieved from the source.

        Returns:
            A list of dictionary records ready for validation.
        """
        data_root = raw_data.get("data", {}).get("MRData", raw_data)

        def find_deepest_list(obj: Any) -> List[Dict[str, Any]]:
            if isinstance(obj, list):
                # Heuristic: If any item in this list contains another list,
                # we haven't reached the true "data rows" yet.
                for item in obj:
                    if isinstance(item, dict):
                        for value in item.values():
                            nested = find_deepest_list(value)
                            if nested:
                                return nested
                return obj

            if isinstance(obj, dict):
                for value in obj.values():
                    nested = find_deepest_list(value)
                    if nested:
                        return nested
            return []

        records = find_deepest_list(data_root)

        if not records:
            logger.warning("⚠️ No records found in the JSON payload.")

        return records

    def process_batch(self, records: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Validate records against the Pydantic schema and convert to Polars.

        Args:
            records: A list of raw dictionaries extracted from the API payload.

        Returns:
            A Polars DataFrame containing the validated and cleaned rows.
        """
        valid_items = []
        errors = 0

        for record in records:
            try:
                # Validate & Clean using Pydantic
                validated_obj = self.schema_class.model_validate(record)

                # Convert back to dict using clean Pythonic names (snake_case)
                # by_alias=False ensures we use field names, not API aliases
                valid_items.append(validated_obj.model_dump(by_alias=False))
            except Exception:
                errors += 1
                continue

        if errors > 0:
            logger.warning(f"⚠️ Skipped {errors} records due to validation failure.")

        # Convert to Polars DataFrame for efficient processing
        return pl.DataFrame(valid_items)

    def _flatten_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Recursively unnest all 'Struct' columns into top-level columns.

        Uses prefixes (e.g., 'driver_nationality') to prevent name collisions
        between nested objects (e.g., Driver nationality vs Constructor nationality).

        Args:
            df: The Polars DataFrame to flatten.

        Returns:
            A completely flat DataFrame with no Struct columns.
        """
        curr_df = df
        while True:
            # Find any columns that are structs
            struct_cols = [
                name
                for name, dtype in zip(curr_df.columns, curr_df.dtypes)
                if isinstance(dtype, pl.Struct)
            ]

            if not struct_cols:
                break

            for col in struct_cols:
                # Add prefix to the nested fields to avoid collisions
                # dtype.fields gives us access to the inner field names
                dtype = curr_df.schema[col]
                new_field_names = [f"{col}_{f.name}" for f in dtype.fields]

                curr_df = curr_df.with_columns(
                    pl.col(col).struct.rename_fields(new_field_names)
                )

            # Unnest pulls the fields to the top level
            curr_df = curr_df.unnest(struct_cols)

        return curr_df