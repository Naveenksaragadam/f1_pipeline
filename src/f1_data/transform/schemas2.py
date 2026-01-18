# src/f1_data/transform/schemas2.py

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Union, Any, Dict
from datetime import date

# ==========================================
# 1. SHARED MODELS
# ==========================================

class DriverRaw(BaseModel):
    driver_id: str = Field(alias="driverId")
    permanent_number: Optional[str] = Field(default=None, alias="permanentNumber")
    code: Optional[str] = None
    url: str
    given_name: str = Field(alias="givenName")
    family_name: str = Field(alias="familyName")
    date_of_birth: date = Field(alias="dateOfBirth")
    nationality: str

    model_config = {"extra": "ignore"}

    @field_validator('nationality')
    @classmethod
    def clean_nationality(cls, v: str) -> str:
        return v.title()

class ConstructorRaw(BaseModel):
    constructor_id: str = Field(alias="constructorId")
    url: str
    name: str
    nationality: str

    model_config = {"extra": "ignore"}

# ==========================================
# 2. REFERENCE ENDPOINTS
# ==========================================

class SeasonRaw(BaseModel):
    season: str
    url: str
    
class CircuitRaw(BaseModel):
    circuit_id: str = Field(alias="circuitId")
    url: str
    circuit_name: str = Field(alias="circuitName")
    location: Dict[str, Any] = Field(alias="Location") # Keep as dict for now to allow Unnesting in Polars
    
class StatusRaw(BaseModel):
    status_id: str = Field(alias="statusId")
    count: str
    status: str

# ==========================================
# 3. RACE SUB-ENDPOINTS
# ==========================================

class TimeRaw(BaseModel):
    millis: Optional[str] = None
    time: str

class FastestLapRaw(BaseModel):
    rank: str
    lap: str
    time: TimeRaw
    average_speed: Optional[dict] = Field(default=None, alias="AverageSpeed")

class ResultRaw(BaseModel):
    number: str
    position: str
    position_text: str = Field(alias="positionText")
    points: str
    driver: DriverRaw = Field(alias="Driver")
    constructor: ConstructorRaw = Field(alias="Constructor")
    grid: str
    laps: str
    status: str
    time: Optional[TimeRaw] = Field(default=None, alias="Time")
    fastest_lap: Optional[FastestLapRaw] = Field(default=None, alias="FastestLap")
    
    model_config = {"extra": "ignore"}

class QualifyingRaw(BaseModel):
    number: str
    position: str
    driver: DriverRaw = Field(alias="Driver")
    constructor: ConstructorRaw = Field(alias="Constructor")
    q1: Optional[str] = Field(default=None, alias="Q1")
    q2: Optional[str] = Field(default=None, alias="Q2")
    q3: Optional[str] = Field(default=None, alias="Q3")

class SprintRaw(BaseModel):
    number: str
    position: str
    position_text: str = Field(alias="positionText")
    points: str
    driver: DriverRaw = Field(alias="Driver")
    constructor: ConstructorRaw = Field(alias="Constructor")
    grid: str
    laps: str
    status: str
    time: Optional[TimeRaw] = Field(default=None, alias="Time")

class PitStopRaw(BaseModel):
    driver_id: str = Field(alias="driverId")
    lap: str
    stop: str
    time: str
    duration: str

class LapTimingRaw(BaseModel):
    driver_id: str = Field(alias="driverId")
    position: str
    time: str

class LapRaw(BaseModel):
    number: str
    timings: List[LapTimingRaw] = Field(alias="Timings")

# ==========================================
# 4. STANDINGS
# ==========================================

class DriverStandingRaw(BaseModel):
    position: str
    position_text: str = Field(alias="positionText")
    points: str
    wins: str
    driver: DriverRaw = Field(alias="Driver")
    constructors: List[ConstructorRaw] = Field(alias="Constructors")

class ConstructorStandingRaw(BaseModel):
    position: str
    position_text: str = Field(alias="positionText")
    points: str
    wins: str
    constructor: ConstructorRaw = Field(alias="Constructor")

# ==========================================
# 5. TABLES WRAPPERS
# ==========================================

# Generic wrapper for Race related inner lists
class RaceRaw(BaseModel):
    season: str
    round: str
    url: str
    race_name: str = Field(alias="raceName")
    circuit: dict = Field(alias="Circuit")
    date: date
    time: Optional[str] = None
    
    # Optional lists for different endpoints (only one will be populated per file)
    results: List[ResultRaw] = Field(default_factory=list, alias="Results")
    qualifying: List[QualifyingRaw] = Field(default_factory=list, alias="QualifyingResults")
    sprint: List[SprintRaw] = Field(default_factory=list, alias="SprintResults")
    pitstops: List[PitStopRaw] = Field(default_factory=list, alias="PitStops")
    laps: List[LapRaw] = Field(default_factory=list, alias="Laps")

    model_config = {"extra": "ignore"}

class RaceTable(BaseModel):
    season: str
    round: Optional[str] = None
    races: List[RaceRaw] = Field(alias="Races")

class StandingsListRaw(BaseModel):
    season: str
    round: str
    driver_standings: List[DriverStandingRaw] = Field(default_factory=list, alias="DriverStandings")
    constructor_standings: List[ConstructorStandingRaw] = Field(default_factory=list, alias="ConstructorStandings")

class StandingsTable(BaseModel):
    season: str
    round: Optional[str] = None
    standings_lists: List[StandingsListRaw] = Field(alias="StandingsLists")

# ==========================================
# 6. ROOT WRAPPER
# ==========================================

class RootFileStructure(BaseModel):
    metadata: Dict[str, Any]
    data: Dict[str, Any] = Field(alias="data") 
