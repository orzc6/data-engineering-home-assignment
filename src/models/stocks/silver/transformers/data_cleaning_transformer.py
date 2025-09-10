from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame, functions as F
from src.modules.transformer import Transformer
from src.common.logging_utils import get_logger
import logging


class DataCleaningTransformer(Transformer):
    """Transformer for cleaning and validating stocks data."""
    
    def __init__(self, name: str = "data_cleaning_transformer", 
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(name, logger)
    
    def transform(self, dataframes: List[DataFrame]) -> DataFrame:
        """Clean and validate data."""
        if not dataframes:
            raise ValueError("No dataframes provided")
        
        df = dataframes[0]
        self.logger.info("Starting data cleaning transformation")
        
        # Remove rows with null critical fields
        df = df.filter(
            F.col("date").isNotNull() & 
            F.col("ticker").isNotNull() & 
            F.col("close").isNotNull()
        )
        
        # Handle null volume as 0
        df = df.withColumn("volume", 
            F.when(F.col("volume").isNull(), F.lit(0.0))
            .otherwise(F.col("volume"))
        )
        
        # Ensure positive values
        df = df.filter(F.col("close") > 0)
        
        self.logger.info("Data cleaning transformation completed")
        return df
