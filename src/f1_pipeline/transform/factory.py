"""
Transformer Factory
Registers and retrieves schemas for various F1 API endpoints.
Provides a central mapping to decouple endpoint names from their technical schemas.
"""


from f1_pipeline.transform.schemas import (
    CircuitSchema,
    ConstructorSchema,
    ConstructorStandingSchema,
    DriverSchema,
    DriverStandingSchema,
    F1BaseModel,
    LapSchema,
    PitStopSchema,
    QualifyingSchema,
    ResultSchema,
    SeasonSchema,
    SprintSchema,
    StatusSchema,
)

# Registry: Maps Ergast API endpoint strings to their validated Pydantic models.
# This serves as the central orchestration point for the generic transformation engine.
TRANSFORM_FACTORY: dict[str, type[F1BaseModel]] = {
    # Reference Data
    "seasons": SeasonSchema,
    "circuits": CircuitSchema,
    "status": StatusSchema,
    # Actor Data
    "drivers": DriverSchema,
    "constructors": ConstructorSchema,
    # Performance/Event Data
    "results": ResultSchema,
    "qualifying": QualifyingSchema,
    "sprint": SprintSchema,
    "pitstops": PitStopSchema,
    "laps": LapSchema,
    # Standings
    "driverstandings": DriverStandingSchema,
    "constructorstandings": ConstructorStandingSchema,
}


def get_schema_for_endpoint(endpoint: str) -> type[F1BaseModel]:
    """
    Retrieves the target Pydantic schema for a specific API endpoint.

    Args:
        endpoint: The name of the Ergast endpoint (e.g., 'results').

    Returns:
        The Pydantic class mapped to that endpoint.

    Raises:
        ValueError: If the endpoint is not registered in the factory.
    """
    schema = TRANSFORM_FACTORY.get(endpoint.lower())
    if not schema:
        raise ValueError(
            f"❌ No transformation schema registered for endpoint: '{endpoint}'. "
            f"Supported endpoints: {list(TRANSFORM_FACTORY.keys())}"
        )
    return schema
