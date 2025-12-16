# F1 API Endpoint Extraction Strategy & MinIO Path Conventions

## 1. Endpoint Classification

### 1.1 Static Reference Data (Low Change Frequency)
These endpoints provide reference data that changes infrequently. Extract **once** and update **quarterly** or when changes are detected.

| Endpoint | Description | Update Frequency | Partitioning Strategy |
|----------|-------------|------------------|----------------------|
| `seasons` | All F1 seasons | Once + yearly append | `endpoint=seasons/` |
| `circuits` | Circuit information | Quarterly | `endpoint=circuits/` |
| `status` | Result status codes | Quarterly | `endpoint=status/` |

**Storage Pattern (Bronze):**
```
bronze/ergast/
  endpoint=seasons/
    batch_id=20241216T120000/
      data.json
  endpoint=circuits/
    batch_id=20241216T120000/
      data.json
  endpoint=status/
    batch_id=20241216T120000/
      data.json
```

---

### 1.2 Season-Level Data (Medium Change Frequency)
Data that applies to an entire season but doesn't require round-level granularity.

| Endpoint | Description | Partitioning Strategy | Dependencies |
|----------|-------------|----------------------|--------------|
| `constructors` | Teams per season | `season=YYYY/` | None |
| `drivers` | Drivers per season | `season=YYYY/` | None |

**Storage Pattern (Bronze):**
```
bronze/ergast/
  endpoint=constructors/
    season=2024/
      batch_id=20241216T120000/
        data.json
  endpoint=drivers/
    season=2024/
      batch_id=20241216T120000/
        data.json
```

---

### 1.3 Race-Level Data (High Change Frequency)
Data that requires **both season AND round** for proper identification. This is your **primary incremental workload**.

| Endpoint | Description | Partitioning Strategy | Dependencies | Pagination |
|----------|-------------|----------------------|--------------|------------|
| `races` | Race calendar | `season=YYYY/` | None | No |
| `results` | Race results | `season=YYYY/round=RR/` | `races` | Yes |
| `qualifying` | Qualifying results | `season=YYYY/round=RR/` | `races` | Yes |
| `sprint` | Sprint race results | `season=YYYY/round=RR/` | `races` | Yes |
| `pitstops` | Pit stop data | `season=YYYY/round=RR/` | `races` | Yes |
| `laps` | Lap timing data | `season=YYYY/round=RR/` | `races` | Yes |

**Storage Pattern (Bronze):**
```
bronze/ergast/
  endpoint=races/
    season=2024/
      batch_id=20241216T120000/
        data.json
        
  endpoint=results/
    season=2024/
      round=05/
        batch_id=20241216T120000/
          page_001.json
          page_002.json
          
  endpoint=qualifying/
    season=2024/
      round=05/
        batch_id=20241216T120000/
          page_001.json
          
  endpoint=pitstops/
    season=2024/
      round=05/
        batch_id=20241216T120000/
          page_001.json
          
  endpoint=laps/
    season=2024/
      round=05/
        batch_id=20241216T120000/
          page_001.json
          page_002.json
          page_003.json
```

---

### 1.4 Standings Data (Season Progression)
Cumulative standings that change after each race.

| Endpoint | Description | Partitioning Strategy | Dependencies |
|----------|-------------|----------------------|--------------|
| `driverstandings` | Driver championship standings | `season=YYYY/round=RR/` | `results` |
| `constructorstandings` | Constructor standings | `season=YYYY/round=RR/` | `results` |

**Storage Pattern (Bronze):**
```
bronze/ergast/
  endpoint=driverstandings/
    season=2024/
      round=05/
        batch_id=20241216T120000/
          data.json
          
  endpoint=constructorstandings/
    season=2024/
      round=05/
        batch_id=20241216T120000/
          data.json
```

---

## 2. Complete MinIO Path Convention

### 2.1 Bronze Layer (Raw JSON)
**Format:** `bronze/ergast/endpoint={name}/[season={YYYY}/][round={RR}/]batch_id={timestamp}/[page_{NNN}.json|data.json]`

**Rules:**
- **Immutable**: Never overwrite existing files
- **Versioned**: Each extraction gets a unique `batch_id`
- **Paginated**: Multi-page results use `page_001.json`, `page_002.json`, etc.
- **Single-page**: Use `data.json` for non-paginated endpoints

**Examples:**
```
# Static reference data
bronze/ergast/endpoint=circuits/batch_id=20241216T120000/data.json

# Season-level data
bronze/ergast/endpoint=drivers/season=2024/batch_id=20241216T120000/data.json

# Race-level paginated data
bronze/ergast/endpoint=results/season=2024/round=05/batch_id=20241216T120000/page_001.json
bronze/ergast/endpoint=results/season=2024/round=05/batch_id=20241216T120000/page_002.json

# Standings (single page per round)
bronze/ergast/endpoint=driverstandings/season=2024/round=05/batch_id=20241216T120000/data.json
```

---

### 2.2 Silver Layer (Typed Parquet)
**Format:** `silver/ergast/endpoint={name}/[season={YYYY}/][round={RR}/]batch_id={timestamp}/data.parquet`

**Transformations Applied:**
- JSON → Parquet conversion
- Schema validation (Pydantic models)
- Type casting (dates, integers, floats)
- Null handling
- Nested JSON flattening
- Row deduplication (based on natural keys)

**Examples:**
```
# Static reference
silver/ergast/endpoint=circuits/batch_id=20241216T120000/data.parquet

# Season-level
silver/ergast/endpoint=drivers/season=2024/batch_id=20241216T120000/data.parquet

# Race-level (consolidated from paginated JSON)
silver/ergast/endpoint=results/season=2024/round=05/batch_id=20241216T120000/data.parquet
silver/ergast/endpoint=laps/season=2024/round=05/batch_id=20241216T120000/data.parquet
```

---

## 3. Extraction Priorities & Sequencing

### 3.1 Dependency Graph
```
1. Static Reference (Independent)
   ├── seasons
   ├── circuits
   └── status

2. Season-Level (Depends on: seasons)
   ├── constructors
   └── drivers

3. Race Calendar (Depends on: seasons)
   └── races

4. Race Results (Depends on: races)
   ├── qualifying
   ├── results
   ├── sprint
   ├── pitstops
   └── laps

5. Standings (Depends on: results)
   ├── driverstandings
   └── constructorstandings
```

### 3.2 Extraction Order (Airflow Task Sequence)
```python
# DAG Structure
static_reference = [seasons, circuits, status]  # Parallel
season_data = [constructors, drivers]            # Parallel, waits for seasons
race_calendar = [races]                          # Waits for seasons
race_results = [qualifying, results, sprint, pitstops, laps]  # Parallel, waits for races
standings = [driverstandings, constructorstandings]           # Waits for results
```

---

## 4. Pagination Handling

### 4.1 Endpoints with Pagination
These endpoints return large datasets and require pagination:
- `results`
- `qualifying`
- `sprint`
- `pitstops`
- `laps`

### 4.2 Pagination Strategy
```python
# Extract all pages sequentially
# API pattern: ?limit=30&offset=0, offset=30, offset=60...

def fetch_all_pages(endpoint, season, round):
    page = 1
    offset = 0
    limit = 30
    
    while True:
        response = fetch_page(endpoint, season, round, limit, offset)
        
        # Save page to Bronze
        save_to_minio(response, page)
        
        total = int(response['MRData']['total'])
        if offset + limit >= total:
            break
            
        offset += limit
        page += 1
```

### 4.3 Silver Consolidation
In the Transform step, **consolidate all pages** into a single Parquet file:
```
Bronze: page_001.json, page_002.json, page_003.json
   ↓
Silver: data.parquet (all pages combined)
```

---

## 5. API URL Construction

### 5.1 Endpoint Templates
```python
ENDPOINT_TEMPLATES = {
    # Static (no parameters)
    "seasons": "{base}/seasons.json",
    "circuits": "{base}/circuits.json",
    "status": "{base}/status.json",
    
    # Season-level
    "constructors": "{base}/{season}/constructors.json",
    "drivers": "{base}/{season}/drivers.json",
    "races": "{base}/{season}/races.json",
    
    # Race-level
    "results": "{base}/{season}/{round}/results.json",
    "qualifying": "{base}/{season}/{round}/qualifying.json",
    "sprint": "{base}/{season}/{round}/sprint.json",
    "pitstops": "{base}/{season}/{round}/pitstops.json",
    "laps": "{base}/{season}/{round}/laps.json",
    
    # Standings (round-specific)
    "driverstandings": "{base}/{season}/{round}/driverstandings.json",
    "constructorstandings": "{base}/{season}/{round}/constructorstandings.json",
}
```

### 5.2 Dynamic URL Builder
```python
def build_url(endpoint_name: str, season: int = None, round: int = None) -> str:
    template = ENDPOINT_TEMPLATES[endpoint_name]
    
    url = template.format(
        base="https://api.jolpi.ca/ergast/f1",
        season=season or "",
        round=round or ""
    )
    
    return url
```

---

## 6. Metadata Envelope (Updated)

Each JSON file in Bronze should include:
```json
{
  "metadata": {
    "ingestion_timestamp": "2024-12-16T12:00:00Z",
    "batch_id": "20241216T120000",
    "endpoint": "results",
    "season": 2024,
    "round": 5,
    "page": 1,
    "total_pages": 3,
    "source_url": "https://api.jolpi.ca/ergast/f1/2024/5/results.json?limit=30&offset=0",
    "api_response_total": 90,
    "file_version": "1.0"
  },
  "data": {
    "MRData": { /* actual API response */ }
  }
}
```

---

## 7. Backfill Strategy

### 7.1 Historical Backfill (1950 → 2024)
```python
# Outer loop: Seasons
for season in range(1950, 2025):
    # 1. Extract static data for season
    extract_season_data(season)  # constructors, drivers
    
    # 2. Get race calendar
    races = extract_races(season)
    
    # Inner loop: Rounds
    for round in races:
        # 3. Extract race-level data
        extract_race_data(season, round)  # results, qualifying, etc.
        extract_standings(season, round)  # standings
```

### 7.2 Incremental Updates (Current Season)
```python
# Scheduled: Daily during race season
current_season = 2024
latest_round = get_latest_round(current_season)

# Extract only new rounds
for round in range(last_processed_round + 1, latest_round + 1):
    extract_race_data(current_season, round)
    extract_standings(current_season, round)
```

---

## 8. Implementation Checklist

### Phase 1: Core Extraction
- [ ] Implement endpoint templates
- [ ] Build dynamic URL generator
- [ ] Add pagination support for race-level endpoints
- [ ] Create metadata envelope structure
- [ ] Update MinIO path builder to support new conventions

### Phase 2: Orchestration
- [ ] Create Airflow DAG with task dependencies
- [ ] Implement backfill DAG (1950-2024)
- [ ] Implement incremental DAG (current season)
- [ ] Add idempotency checks (skip if batch_id exists)

### Phase 3: Silver Transformation
- [ ] JSON → Parquet converter
- [ ] Page consolidation logic
- [ ] Schema validation (Pydantic models per endpoint)
- [ ] Type casting and null handling

### Phase 4: Gold Layer
- [ ] ClickHouse table definitions
- [ ] dbt models for facts/dimensions
- [ ] SCD2 snapshots for drivers/constructors/circuits
- [ ] Aggregation models for analytics

---

## 9. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Hive-style partitioning** | Enables partition pruning in ClickHouse/Spark |
| **Immutable Bronze** | Allows safe reprocessing without data loss |
| **Page consolidation in Silver** | Simplifies downstream queries |
| **Season + Round partitioning** | Matches F1's natural hierarchy |
| **Batch ID per extraction** | Enables version tracking and rollback |
| **Separate DAGs for backfill/incremental** | Different scheduling and retry logic |

---

## 10. Next Steps

1. **Update `ingestor.py`** to support all endpoints
2. **Create endpoint configuration file** with templates
3. **Build Airflow DAG** with proper task dependencies
4. **Define Pydantic schemas** for each endpoint
5. **Implement Bronze → Silver transformation**