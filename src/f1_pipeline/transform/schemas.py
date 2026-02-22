"""
F1 Silver Layer Schemas
Defines the Pydantic models used for data validation, cleaning, and 
type enforcement during the Silver layer transformation.
"""

from typing import Annotated, Optional

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


class DriverSchema(F1BaseModel):
    """Validation schema for Formula 1 Driver entities."""

    driver_id: str = Field(alias="driverId", description="Technical identifier (e.g., 'max_verstappen')")
    permanent_number: Optional[int] = Field(
        None, alias="permanentNumber", description="Official racing number"
    )
    code: Optional[str] = Field(None, alias="code", description="Three-letter shorthand (e.g., 'VER')")
    given_name: str = Field(alias="givenName", description="Driver's first/given name")
    family_name: str = Field(alias="familyName", description="Driver's last/family name")
    date_of_birth: str = Field(alias="dateOfBirth", description="Birth date in YYYY-MM-DD format")
    nationality: CleanStr = Field(alias="nationality", description="Standardized nationality string")
    url: str = Field(alias="url", description="Wikipedia URL for the driver")


class ConstructorSchema(F1BaseModel):
    """Validation schema for Formula 1 Constructor (Team) entities."""

    constructor_id: str = Field(alias="constructorId", description="Technical identifier (e.g., 'red_bull')")
    name: str = Field(alias="name", description="Official team name")
    nationality: CleanStr = Field(alias="nationality", description="Standardized team nationality")
    url: str = Field(alias="url", description="Wikipedia URL for the constructor")


class TimeSchema(F1BaseModel):
    """Represents a time duration or timestamp in race sessions."""

    time: str = Field(alias="time", description="Human-readable duration (e.g., 1:29:18.303)")
    millis: Optional[int] = Field(None, alias="millis", description="Duration in milliseconds for calculations")


class AverageSpeedSchema(F1BaseModel):
    """Metric for the average speed achieved during a session or lap."""

    units: str = Field(alias="units", description="Unit of measurement (e.g., kph)")
    speed: float = Field(alias="speed", description="The actual speed value")


class FastestLapSchema(F1BaseModel):
    """Details regarding the single fastest lap recorded by a driver during a race."""

    rank: int = Field(alias="rank", description="Ranking relative to other drivers (1 = Fastest overall)")
    lap: int = Field(alias="lap", description="The lap number on which the fastest time was set")
    time: TimeSchema = Field(alias="Time", description="Duration details of the lap")
    average_speed: Optional[AverageSpeedSchema] = Field(
        None, alias="AverageSpeed", description="Speed details for the lap"
    )


class ResultSchema(F1BaseModel):
    """
    Validation schema for a single Race Result record.
    This model acts as the root for a single row in the Results final table.
    """

    number: Optional[int] = Field(None, description="The car number used in the race")
    position: Optional[int] = Field(None, description="Final race finishing position")
    points: Optional[float] = Field(None, description="Championship points awarded")
    grid: Optional[int] = Field(None, description="Starting grid position")
    laps: Optional[int] = Field(None, description="Total laps completed")
    driver: DriverSchema = Field(alias="Driver", description="Nested driver profile")
    constructor: ConstructorSchema = Field(alias="Constructor", description="Nested team profile")
    status: str = Field(description="Finishing status (e.g., 'Finished', '+1 Lap', 'Engine')")
    time: Optional[TimeSchema] = Field(None, alias="Time", description="Total race time for finishers")
    fastest_lap: Optional[FastestLapSchema] = Field(
        None, alias="FastestLap", description="Details for the fastest lap, if available"
    )