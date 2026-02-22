"""
Transformer Factory
Registers and retrieves schemas for various F1 API endpoints.
Provides a central mapping to decouple endpoint names from their technical schemas.
"""

from typing import Dict, Type

from f1_pipeline.transform.schemas import (
    ConstructorSchema,
    DriverSchema,
    F1BaseModel,
    ResultSchema,
)

# Registry: Maps Ergast API endpoint strings to their validated Pydantic models.
# This makes the pipeline easily extensible for new endpoints (e.g., 'lap_times').
TRANSFORM_FACTORY: Dict[str, Type[F1BaseModel]] = {
    "drivers": DriverSchema,
    "constructors": ConstructorSchema,
    "results": ResultSchema,
}


def get_schema_for_endpoint(endpoint: str) -> Type[F1BaseModel]:
    """
    Retrieves the target Pydantic schema for a specific API endpoint.

    Args:
        endpoint: The name of the Ergast endpoint (e.g., 'results').

    Returns:
        The Pydantic class mapped to that endpoint.

    Raises:
        ValueError: If the endpoint is not registered in the factory.
    """
    schema = TRANSFORM_FACTORY.get(endpoint)
    if not schema:
        raise ValueError(
            f"❌ No transformation schema registered for endpoint: '{endpoint}'. "
            f"Supported endpoints: {list(TRANSFORM_FACTORY.keys())}"
        )
    return schema
