from typing import List

from pydantic import BaseModel, Field, field_validator


class RunConfiguration(BaseModel):
    """
    Configuration class for MLP pipeline runs. Uses Pydantic data class to enable JSON
    serialization/deserialization of configuration.
    """
    firm_crd_numbers: List[int]
    input_dir: str
    output_dir: str
    db_path: str
    raise_error: bool = Field(default=False, description="Raise error upon failure")
    verbose: bool = Field(default=False, description="Enable verbose logging")

    @field_validator('firm_crd_numbers')
    def check_firm_crd_numbers(cls, v):
        if not v:
            raise ValueError("At least one firm CRD number must be provided")
        return v
