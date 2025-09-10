"""
Configuration management for the stocks data processing pipeline.
"""

import os
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


class PipelineConfig(BaseModel):
    """Configuration class for the stocks data processing pipeline."""

    # Input and output paths
    input_s3_path: str = Field(..., description="S3 path to input CSV file")
    output_base: str = Field(..., description="S3 base path for output data")
    bronze_table_name: str = Field(default="stocks_raw", description="Bronze layer table name")
    silver_table_name: str = Field(default="stocks_silver", description="Silver layer table name")
    
    # Glue job specific settings
    glue_job_name: Optional[str] = Field(default=None, description="AWS Glue job name")
    glue_role: Optional[str] = Field(default=None, description="AWS Glue IAM role")
    
    # Performance settings
    spark_executor_memory: str = Field(default="2g", description="Spark executor memory")
    spark_executor_cores: int = Field(default=2, ge=1, le=8, description="Spark executor cores")
    spark_driver_memory: str = Field(default="1g", description="Spark driver memory")
    
    @field_validator('spark_executor_memory', 'spark_driver_memory')
    @classmethod
    def validate_memory_format(cls, v):
        """Validate memory format (e.g., '2g', '512m')."""
        if not v.endswith(('g', 'm', 'k')):
            raise ValueError('Memory must end with g, m, or k (e.g., "2g", "512m")')
        return v
    
    @field_validator('input_s3_path', 'output_base')
    @classmethod
    def validate_s3_path(cls, v):
        """Validate S3 path format."""
        if not v.startswith('s3://'):
            raise ValueError('S3 path must start with s3://')
        return v
    
    @model_validator(mode='after')
    def validate_required_fields(self):
        """Validate that required fields are not empty."""
        if not self.input_s3_path:
            raise ValueError('input_s3_path is required')
        if not self.output_base:
            raise ValueError('output_base is required')
        return self
    
    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Create configuration from environment variables."""
        env_data = {
            "input_s3_path": os.getenv("INPUT_S3_PATH", ""),
            "output_base": os.getenv("OUTPUT_S3_PATH", ""),
            "glue_job_name": os.getenv("GLUE_JOB_NAME"),
            "glue_role": os.getenv("GLUE_ROLE"),
            "spark_executor_memory": os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
            "spark_executor_cores": int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
            "spark_driver_memory": os.getenv("SPARK_DRIVER_MEMORY", "1g"),
        }
        return cls(**env_data)
    
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True
    )
