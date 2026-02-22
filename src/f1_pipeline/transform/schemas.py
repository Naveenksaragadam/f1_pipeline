"""
F1 Silver Layer Schemas
Defines the Pydantic models used for data validation, cleaning, and
type enforcement during the Silver layer transformation.
"""

from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

# Elite Domain Type: Standardizes and cleans strings (e.g., Title Case nationalities)
CleanStr = Annotated[str, AfterValidator(lambda v: v.title().strip() if v else v)]


class F1BaseModel(BaseModel):
    """
    Base model for all F1 transformation schemas.
    Configures standard behavioral settings for consistency.
    """

    model_config = ConfigDict(
        populate_by_name=True,  # Allow creating models using Pythonic names (aliases)
        extra="ignore",  # Drop unknown fields found in source JSON (Fail-safe)
        frozen=True,  # Models are immutable (Functional programming style)
    )


# --- Reference Entities ---


class SeasonSchema(F1BaseModel):
    """Validation schema for Formula 1 Season entities."""

    season: int = Field(description="The calendar year of the season (e.g., 2024)")
    url: str = Field(description="Wikipedia URL for the season")


class LocationSchema(F1BaseModel):
    """Geographic coordinates and locality for an F1 venue."""

    lat: float = Field(description="Latitude of the circuit")
    long: float = Field(description="Longitude of the circuit")
    locality: str = Field(description="City or town where the circuit is located")
    country: CleanStr = Field(description="Country where the circuit is located")


class CircuitSchema(F1BaseModel):
    """Validation schema for Formula 1 Circuit entities."""

    circuit_id: str = Field(alias="circuitId", description="Technical identifier (e.g., 'spa')")
    name: str = Field(alias="circuitName", description="Official circuit name")
    url: str = Field(description="Wikipedia URL for the circuit")
    location: LocationSchema = Field(alias="Location", description="Geographic location details")


class StatusSchema(F1BaseModel):
    """Validation schema for race finishing status codes."""

    status_id: int = Field(alias="statusId", description="Unique status identifier")
    status: str = Field(description="Human-readable status (e.g., 'Finished', 'Engine')")
    count: int = Field(description="Number of occurrences (used in some endpoint aggregations)")


# --- Actor Entities ---


class DriverSchema(F1BaseModel):
    """Validation schema for Formula 1 Driver entities."""

    driver_id: str = Field(
        alias="driverId", description="Technical identifier (e.g., 'max_verstappen')"
    )
    permanent_number: int | None = Field(
        None, alias="permanentNumber", description="Official racing number"
    )
    code: str | None = Field(None, alias="code", description="Three-letter shorthand (e.g., 'VER')")
    given_name: str = Field(alias="givenName", description="Driver's first/given name")
    family_name: str = Field(alias="familyName", description="Driver's last/family name")
    date_of_birth: str = Field(alias="dateOfBirth", description="Birth date in YYYY-MM-DD format")
    nationality: CleanStr = Field(
        alias="nationality", description="Standardized nationality string"
    )
    url: str = Field(alias="url", description="Wikipedia URL for the driver")


class ConstructorSchema(F1BaseModel):
    """Validation schema for Formula 1 Constructor (Team) entities."""

    constructor_id: str = Field(
        alias="constructorId", description="Technical identifier (e.g., 'red_bull')"
    )
    name: str = Field(alias="name", description="Official team name")
    nationality: CleanStr = Field(alias="nationality", description="Standardized team nationality")
    url: str = Field(alias="url", description="Wikipedia URL for the constructor")


# --- Performance Entities (Shared) ---


class TimeSchema(F1BaseModel):
    """Represents a time duration or timestamp in race sessions."""

    time: str = Field(alias="time", description="Human-readable duration (e.g., 1:29:18.303)")
    millis: int | None = Field(
        None, alias="millis", description="Duration in milliseconds for calculations"
    )


class AverageSpeedSchema(F1BaseModel):
    """Metric for the average speed achieved during a session or lap."""

    units: str = Field(alias="units", description="Unit of measurement (e.g., kph)")
    speed: float = Field(alias="speed", description="The actual speed value")


class FastestLapSchema(F1BaseModel):
    """Details regarding the single fastest lap recorded by a driver during a race."""

    rank: int = Field(
        alias="rank", description="Ranking relative to other drivers (1 = Fastest overall)"
    )
    lap: int = Field(alias="lap", description="The lap number on which the fastest time was set")
    time: TimeSchema = Field(alias="Time", description="Duration details of the lap")
    average_speed: AverageSpeedSchema | None = Field(
        None, alias="AverageSpeed", description="Speed details for the lap"
    )


# --- Event Schemas ---


class RaceSchema(F1BaseModel):
    """Validation schema for a Race schedule/event entity."""

    season: int = Field(description="The calendar year of the season")
    round: int = Field(description="The round number within the season")
    url: str = Field(description="Wikipedia URL for the race")
    race_name: str = Field(alias="raceName", description="Name of the race event")
    circuit: CircuitSchema = Field(alias="Circuit", description="Nested circuit profile")
    date: str = Field(description="Race date in YYYY-MM-DD format")
    time: str | None = Field(None, description="Race start time in UTC")


class ResultSchema(F1BaseModel):
    """Validation schema for a single Race Result record."""

    number: int | None = Field(None, description="The car number used in the race")
    position: int | None = Field(None, description="Final race finishing position")
    points: float | None = Field(None, description="Championship points awarded")
    grid: int | None = Field(None, description="Starting grid position")
    laps: int | None = Field(None, description="Total laps completed")
    driver: DriverSchema = Field(alias="Driver", description="Nested driver profile")
    constructor: ConstructorSchema = Field(alias="Constructor", description="Nested team profile")
    status: str = Field(description="Finishing status (e.g., 'Finished', '+1 Lap', 'Engine')")
    time: TimeSchema | None = Field(None, alias="Time", description="Total race time for finishers")
    fastest_lap: FastestLapSchema | None = Field(
        None, alias="FastestLap", description="Details for the fastest lap, if available"
    )


class QualifyingSchema(F1BaseModel):
    """Validation schema for Qualifying session results."""

    number: int = Field(description="The car number")
    position: int = Field(description="Qualifying rank (1 = Pole Position)")
    driver: DriverSchema = Field(alias="Driver", description="Driver profile")
    constructor: ConstructorSchema = Field(alias="Constructor", description="Constructor profile")
    q1: str | None = Field(None, alias="Q1", description="Fastest lap in Q1 session")
    q2: str | None = Field(None, alias="Q2", description="Fastest lap in Q2 session")
    q3: str | None = Field(None, alias="Q3", description="Fastest lap in Q3 session")


class SprintSchema(F1BaseModel):
    """Validation schema for Sprint race results."""

    number: int = Field(description="The car number")
    position: int = Field(description="Final finishing position in the Sprint")
    points: float = Field(description="Points awarded for the Sprint finish")
    grid: int = Field(description="Starting grid position for the Sprint")
    laps: int = Field(description="Laps completed in the Sprint")
    driver: DriverSchema = Field(alias="Driver", description="Driver profile")
    constructor: ConstructorSchema = Field(alias="Constructor", description="Constructor profile")
    status: str = Field(description="Finishing status")
    time: TimeSchema | None = Field(None, alias="Time", description="Total time for finishers")


class PitStopSchema(F1BaseModel):
    """Validation schema for a single Pit Stop event."""

    driver_id: str = Field(alias="driverId", description="Identifier of the driver stopping")
    lap: int = Field(description="The lap number on which the stop occurred")
    stop: int = Field(description="The sequence number for this stop (e.g., 1st stop, 2nd stop)")
    time: str = Field(description="The time of day when the stop occurred (e.g., 15:02:11)")
    duration: float = Field(description="The time spent in the pits (in seconds)")


class LapTimingSchema(F1BaseModel):
    """Validation schema for a single driver's timing during a specific lap."""

    driver_id: str = Field(alias="driverId", description="Identifier of the driver")
    position: int = Field(description="Track position on this lap")
    time: str = Field(description="Lap time (e.g., '1:21.345')")


class LapSchema(F1BaseModel):
    """Higher-order schema for a full lap's worth of timings."""

    number: int = Field(description="The lap number")
    timings: list[LapTimingSchema] = Field(
        alias="Timings", description="List of driver timings for this lap"
    )


# --- Standing Schemas ---


class DriverStandingSchema(F1BaseModel):
    """Validation schema for Driver Championship standings."""

    position: int = Field(description="Current standing position")
    points: float = Field(description="Total points accumulated")
    wins: int = Field(description="Total race wins in the season")
    driver: DriverSchema = Field(alias="Driver", description="Driver profile")
    constructors: list[ConstructorSchema] = Field(
        alias="Constructors", description="The team(s) the driver has represented"
    )


class ConstructorStandingSchema(F1BaseModel):
    """Validation schema for Constructor Championship standings."""

    position: int = Field(description="Current standing position")
    points: float = Field(description="Total points accumulated")
    wins: int = Field(description="Total race wins in the season")
    constructor: ConstructorSchema = Field(alias="Constructor", description="Constructor profile")
