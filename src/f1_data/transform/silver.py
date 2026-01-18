
import logging
import io
import json
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import pyarrow.parquet as pq

from ..minio.object_store import F1ObjectStore
from ..config import (
    MINIO_BUCKET_BRONZE,
    MINIO_BUCKET_SILVER,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SILVER_PARTITION_COLS
)
from .schemas2 import RootFileStructure

logger = logging.getLogger(__name__)

class SilverProcessor:
    """
    Process Bronze JSON data into Silver Parquet tables using Polars.
    """
    
    def __init__(self, store: Optional[F1ObjectStore] = None):
        """
        Initialize processor.
        
        Args:
            store: Optional pre-configured F1ObjectStore. 
                   If None, creates a new one pointing to Silver/Bronze.
                   (Note: F1ObjectStore manages one bucket, so we might need two instances 
                   or switch generic store to handle multiple buckets if logic allows.
                   Current F1ObjectStore is bucket-specific in init.)
        """
        # We need access to BOTH Bronze (Read) and Silver (Write) buckets.
        # Efficient way: Two store instances.
        
        self.bronze_store = F1ObjectStore(
            bucket_name=MINIO_BUCKET_BRONZE,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY
        )
        
        # If store passed in, assume it's for Silver (or generic) but we strictly need Silver writer
        self.silver_store = store or F1ObjectStore(
            bucket_name=MINIO_BUCKET_SILVER,
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY
        )

    def process_batch(self, batch_id: str, season: int, endpoint: str) -> None:
        """
        Process a specific batch of data for an endpoint/season.
        
        1. List all pages in Bronze for this batch.
        2. Read and Parse JSON.
        3. Flatten/Normalize using Polars.
        4. Write to Silver Parquet partitioned by season.
        """
        logger.info(f"🔨 Starting Silver processing: {endpoint} (Season {season}, Batch {batch_id})")
        
        # 1. List Files
        prefix = f"ergast/endpoint={endpoint}/season={season}/batch_id={batch_id}/"
        keys = self.bronze_store.list_objects(prefix=prefix)
        
        if not keys:
            logger.warning(f"⚠️  No files found for {prefix}")
            return

        logger.info(f"   Found {len(keys)} pages. Reading...")

        # 2. Read & Validate (Parallel)
        raw_records = []
        with ThreadPoolExecutor() as executor:
            future_to_key = {executor.submit(self.bronze_store.get_json, key): key for key in keys}
            
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    data = future.result()
                    # Validate envelope structure
                    # We utilize the generic RootFileStructure wrapper here if needed,
                    # or just extracting the inner data directly if we trust Bronze.
                    # For safety, let's extract the actual API data payload.
                    
                    # Envelope structure from ingestor: {"metadata": ..., "data": ...}
                    if "data" in data and "MRData" in data["data"]:
                        # Extract the inner list based on endpoint
                        inner_data = self._extract_inner_data(endpoint, data["data"]["MRData"])
                        raw_records.extend(inner_data)
                    else:
                        logger.warning(f"Invalid envelope in {key}")
                        
                except Exception as e:
                    logger.error(f"❌ Failed to process {key}: {e}")
        
        if not raw_records:
            logger.warning("⚠️  No valid records extracted.")
            return

        # 3. Transform with Polars
        try:
            df = pl.from_dicts(raw_records, infer_schema_length=1000)
            
            # Add partition columns if missing
            if "season" not in df.columns:
                df = df.with_columns(pl.lit(season).alias("season").cast(pl.Int64))
            
            # Deduplicate
            # Strategy: Deduplicate by natural keys (or all columns if no ID)
            # Polars unique() is fast.
            initial_count = len(df)
            df = df.unique()
            final_count = len(df)
            
            if initial_count > final_count:
                logger.info(f"   Removed {initial_count - final_count} duplicates.")
            
            # 4. Write to Silver (Parquet)
            # We write to a temp buffer then upload to MinIO
            
            # Define Silver Path: endpoint/season=XXXX/batch_id.parquet
            # Using Hive partitioning style in object naming
            silver_key = f"{endpoint}/season={season}/{batch_id}.parquet"
            
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression="snappy")
            buffer.seek(0)
            
            self.silver_store.put_object(
                key=silver_key,
                body=buffer,
                compress=False, # Parquet is already compressed (Snappy)
                metadata={"rows": str(final_count), "source_batch": batch_id}
            )
            
            logger.info(f"✅ Written to Silver: {silver_key} ({final_count} rows)")
            
        except Exception as e:
            logger.error(f"❌ Transformation failed for {endpoint}: {e}", exc_info=True)
            raise

    def _extract_inner_data(self, endpoint: str, mr_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract the relevant list of records from the MRData response based on endpoint.
        Uses naming conventions from Ergast API.
        """
        # Helper to unwrap known structures
        if endpoint == "drivers":
            return mr_data.get("DriverTable", {}).get("Drivers", [])
        elif endpoint == "constructors":
            return mr_data.get("ConstructorTable", {}).get("Constructors", [])
        elif endpoint == "races":
            return mr_data.get("RaceTable", {}).get("Races", [])
        elif endpoint == "results":
            # Results are nested inside Races. 
            # Structure: RaceTable -> Races (List) -> each Race has "Results" (List)
            # We want to flatten this: Each row = Result + Race Info
            races = mr_data.get("RaceTable", {}).get("Races", [])
            flattened_results = []
            for race in races:
                race_info = {k: v for k, v in race.items() if k != "Results"}
                results = race.get("Results", [])
                for result in results:
                    # Combine Race Info + Result Info
                    combined = {**race_info, **result}
                    flattened_results.append(combined)
            return flattened_results
            
        # Fallback for other endpoints (to be implemented)
            return flattened_results
            
        elif endpoint == "qualifying":
            # Structure: RaceTable -> Races -> QualifyingResults
            races = mr_data.get("RaceTable", {}).get("Races", [])
            flattened = []
            for race in races:
                race_info = {k: v for k, v in race.items() if k != "QualifyingResults"}
                results = race.get("QualifyingResults", [])
                for result in results:
                     flattened.append({**race_info, **result})
            return flattened
            
        elif endpoint == "sprint":
             # Structure: RaceTable -> Races -> SprintResults
            races = mr_data.get("RaceTable", {}).get("Races", [])
            flattened = []
            for race in races:
                race_info = {k: v for k, v in race.items() if k != "SprintResults"}
                results = race.get("SprintResults", [])
                for result in results:
                     flattened.append({**race_info, **result})
            return flattened

        elif endpoint == "pitstops":
             # Structure: RaceTable -> Races -> PitStops
            races = mr_data.get("RaceTable", {}).get("Races", [])
            flattened = []
            for race in races:
                race_info = {k: v for k, v in race.items() if k != "PitStops"}
                stops = race.get("PitStops", [])
                for stop in stops:
                     flattened.append({**race_info, **stop})
            return flattened

        elif endpoint == "laps":
             # Structure: RaceTable -> Races -> Laps -> Timings
             # This is VERY deep. Race -> Lap -> Timing (Driver)
            races = mr_data.get("RaceTable", {}).get("Races", [])
            flattened = []
            for race in races:
                race_base = {k: v for k, v in race.items() if k != "Laps"}
                laps = race.get("Laps", [])
                for lap in laps:
                    lap_number = lap.get("number")
                    timings = lap.get("Timings", [])
                    for timing in timings:
                        # Row = Race Info + Lap # + Timing Info
                        row = {**race_base, "lap": lap_number, **timing}
                        flattened.append(row)
            return flattened

        elif endpoint == "driverstandings":
            # StandingsTable -> StandingsLists -> DriverStandings
            lists = mr_data.get("StandingsTable", {}).get("StandingsLists", [])
            flattened = []
            for item in lists:
                list_info = {k: v for k, v in item.items() if k != "DriverStandings"}
                standings = item.get("DriverStandings", [])
                for standing in standings:
                     flattened.append({**list_info, **standing})
            return flattened
            
        elif endpoint == "constructorstandings":
            # StandingsTable -> StandingsLists -> ConstructorStandings
            lists = mr_data.get("StandingsTable", {}).get("StandingsLists", [])
            flattened = []
            for item in lists:
                list_info = {k: v for k, v in item.items() if k != "ConstructorStandings"}
                standings = item.get("ConstructorStandings", [])
                for standing in standings:
                     flattened.append({**list_info, **standing})
            return flattened
            
        return []
