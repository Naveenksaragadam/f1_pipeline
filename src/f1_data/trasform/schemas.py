# transform/schemas.py
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import date

# --- 1. The Core Driver Object (Deepest Level) ---
class DriverRaw(BaseModel):
    driver_id: str = Field(alias="driverId")
    permanent_number: Optional[str] = Field(default=None, alias="permanentNumber")
    code: Optional[str] = None
    url: str
    given_name: str = Field(alias="givenName")
    family_name: str = Field(alias="familyName")
    date_of_birth: date = Field(alias="dateOfBirth")  # Pydantic auto-parses "YYYY-MM-DD"
    nationality: str

    @field_validator('nationality')
    @classmethod
    def clean_nationality(cls, v: str) -> str:
        return v.title()
    
# --- 2. The API Structure Wrappers ---
class DriverTable(BaseModel):
    season: str
    drivers: List[DriverRaw] = Field(alias="Drivers")

class ErgastResponse(BaseModel):
    limit: str
    offset: str
    total: str
    driver_table: DriverTable = Field(alias="DriverTable")

# --- 3. The Top-Level File Structure ---
# This matches our file: { "metadata": {...}, "data": { "MRData": ... } }

class FileMetadata(BaseModel):
    ingestion_timestamp: str
    batch_id: str
    season: int 
    page: int
    source_url: str

class RootFileStructure(BaseModel):
    metadata: FileMetadata
    data: dict = Field(alias="data") # We will parse 'MRData' manually to handle the nesting safely

    @property
    def mr_data(self) -> ErgastResponse:
        """Helper to access the inner API response safely"""
        return ErgastResponse(**self.data["MRData"])