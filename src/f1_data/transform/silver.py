
import logging
import io
import json
from typing import List, Dict, Any, Optional, Type
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import pyarrow.parquet as pq
from pydantic import TypeAdapter, ValidationError

from ..minio.object_store import F1ObjectStore
from ..config import (
    MINIO_BUCKET_BRONZE,
    MINIO_BUCKET_SILVER,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SILVER_PARTITION_COLS
)
from .schemas2 import (
    DriverRaw, ConstructorRaw, SeasonRaw, CircuitRaw, StatusRaw,
    RaceTable, StandingsTable, RootFileStructure
)

logger = logging.getLogger(__name__)

class SilverProcessor:
    """
    Process Bronze JSON data into Silver Parquet tables using Polars.
    Includes explicit Pydantic validation.
    """
    
    def __init__(self, store: Optional[F1ObjectStore] = None):
        """Initialize generic and silver stores."""
        self.bronze_store = F1ObjectStore(
            bucket_name=MINIO_BUCKET_BRONZE,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY
        )
        
        self.silver_store = store or F1ObjectStore(
            bucket_name=MINIO_BUCKET_SILVER,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY
        )

    def process_batch(self, batch_id: str, season: int, endpoint: str) -> None:
        """
        Process a specific batch of data for an endpoint/season.
        
        Flow:
        1. List Files (Bronze)
        2. Read JSON
        3. Pydantic Validation (Strict Schema Check)
        4. Polars Transformation (Unnest & Deduplicate)
        5. Write Parquet (Silver)
        """
        logger.info(f"🔨 Starting Silver processing: {endpoint} (Season {season}, Batch {batch_id})")
        
        # 1. List Files
        # Note: 'reference' endpoints might store data under season=0 or different path if we didn't unify it.
        # But ingestor uses consistent pathing: endpoint/season/batch.
        prefix = f"ergast/endpoint={endpoint}/season={season}/batch_id={batch_id}/"
        keys = self.bronze_store.list_objects(prefix=prefix)
        
        if not keys:
            logger.warning(f"⚠️  No files found for {prefix}")
            return

        logger.info(f"   Found {len(keys)} pages. Reading & Validating...")

        # 2. Read & Validate (Parallel)
        valid_records = []
        with ThreadPoolExecutor() as executor:
            future_to_key = {executor.submit(self.bronze_store.get_json, key): key for key in keys}
            
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    data = future.result()
                    if "data" in data and "MRData" in data["data"]:
                        # 3. Validation & Extraction
                        mr_data = data["data"]["MRData"]
                        extracted_and_validated = self._validate_and_extract(endpoint, mr_data)
                        valid_records.extend(extracted_and_validated)
                    else:
                        logger.warning(f"Invalid envelope in {key}")
                        
                except ValidationError as ve:
                     logger.error(f"❌ Schema Validation Failed for {key}: {ve}")
                     # In strict mode, we might want to halt. For now, log and skip.
                except Exception as e:
                    logger.error(f"❌ Failed to process {key}: {e}")
        
        if not valid_records:
            logger.warning("⚠️  No valid records extracted.")
            return

        # 4. Transform with Polars
        try:
            # Pydantic dump ensures we have clean python dicts with correct types (e.g. dates as objects)
            # Polars handles date objects well.
            df = pl.from_dicts(valid_records, infer_schema_length=1000)
            
            # Add partition columns if missing
            if "season" not in df.columns:
                df = df.with_columns(pl.lit(season).alias("season").cast(pl.Int64))
            
            # Deduplicate
            initial_count = len(df)
            df = df.unique()
            final_count = len(df)
            
            if initial_count > final_count:
                logger.info(f"   Removed {initial_count - final_count} duplicates.")
            
            # 5. Write to Silver (Parquet)
            silver_key = f"{endpoint}/season={season}/{batch_id}.parquet"
            
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression="snappy")
            buffer.seek(0)
            
            self.silver_store.put_object(
                key=silver_key,
                body=buffer,
                compress=False, 
                metadata={"rows": str(final_count), "source_batch": batch_id, "validated": "true"}
            )
            
            logger.info(f"✅ Written to Silver: {silver_key} ({final_count} rows)")
            
        except Exception as e:
            logger.error(f"❌ Transformation failed for {endpoint}: {e}", exc_info=True)
            raise

    def _validate_and_extract(self, endpoint: str, mr_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Validates JSON using Pydantic TypeAdapter and extracts flattened dicts.
        """
        # ====================
        # Reference Data
        # ====================
        if endpoint == "seasons":
            raw_list = mr_data.get("SeasonTable", {}).get("Seasons", [])
            adapter = TypeAdapter(List[SeasonRaw])
            models = adapter.validate_python(raw_list)
            return [m.model_dump() for m in models] # SnakeCase

        elif endpoint == "circuits":
            raw_list = mr_data.get("CircuitTable", {}).get("Circuits", [])
            adapter = TypeAdapter(List[CircuitRaw])
            models = adapter.validate_python(raw_list)
            return [m.model_dump() for m in models]
            
        elif endpoint == "status":
            raw_list = mr_data.get("StatusTable", {}).get("Status", [])
            adapter = TypeAdapter(List[StatusRaw])
            models = adapter.validate_python(raw_list)
            return [m.model_dump() for m in models]

        # ====================
        # Simple Lists
        # ====================
        elif endpoint == "drivers":
            raw_list = mr_data.get("DriverTable", {}).get("Drivers", [])
            adapter = TypeAdapter(List[DriverRaw])
            models = adapter.validate_python(raw_list)
            return [m.model_dump() for m in models]

        elif endpoint == "constructors":
            raw_list = mr_data.get("ConstructorTable", {}).get("Constructors", [])
            adapter = TypeAdapter(List[ConstructorRaw])
            models = adapter.validate_python(raw_list)
            return [m.model_dump() for m in models]

        # ====================
        # Nested Race Data
        # ====================
        # Strategy: Validate the RaceTable structure first, then flatten.
        elif endpoint in ["races", "results", "qualifying", "sprint", "pitstops", "laps"]:
            # Validate the whole RaceTable structure
            # This ensures all nested fields (Results, Laps etc) match the schema.
            rt = RaceTable(**mr_data.get("RaceTable", {}))
            
            flattened = []
            for race in rt.races:
                # Base race info
                race_dict = race.model_dump(exclude={'results', 'qualifying', 'sprint', 'pitstops', 'laps'})
                
                # Depending on endpoint, we extract specific list
                # Note: The RaceRaw model has Optional fields for these lists.
                
                if endpoint == "races":
                     # Just the race info
                     flattened.append(race_dict)
                
                elif endpoint == "results" and race.results:
                    for item in race.results:
                        flattened.append({**race_dict, **item.model_dump()})

                elif endpoint == "qualifying" and race.qualifying:
                    for item in race.qualifying:
                        flattened.append({**race_dict, **item.model_dump()})

                elif endpoint == "sprint" and race.sprint:
                    for item in race.sprint:
                        flattened.append({**race_dict, **item.model_dump()})
                
                elif endpoint == "pitstops" and race.pitstops:
                    for item in race.pitstops:
                        flattened.append({**race_dict, **item.model_dump()})

                elif endpoint == "laps" and race.laps:
                     # Laps are distinct because they have nested Timings
                     # Race -> Lap -> Timing
                    for lap in race.laps:
                        lap_num = lap.number
                        for timing in lap.timings:
                            row = {**race_dict, "lap": lap_num, **timing.model_dump()}
                            flattened.append(row)
            
            return flattened

        # ====================
        # Standings
        # ====================
        elif endpoint in ["driverstandings", "constructorstandings"]:
            st = StandingsTable(**mr_data.get("StandingsTable", {}))
            flattened = []
            
            for lst in st.standings_lists:
                list_info = lst.model_dump(exclude={'driver_standings', 'constructor_standings'})
                
                if endpoint == "driverstandings" and lst.driver_standings:
                    for item in lst.driver_standings:
                         flattened.append({**list_info, **item.model_dump()})
                
                elif endpoint == "constructorstandings" and lst.constructor_standings:
                    for item in lst.constructor_standings:
                         flattened.append({**list_info, **item.model_dump()})
                         
            return flattened

        return []
