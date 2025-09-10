from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame, functions as F
from src.modules.transformer import Transformer
from src.common.logging_utils import get_logger
import logging


class WorthCalculationTransformer(Transformer):
    """Transformer for calculating worth (close * volume) for each row."""
    
    def __init__(self, name: str = "worth_calculation_transformer", 
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(name, logger)
    
    def transform(self, dataframes: List[DataFrame]) -> DataFrame:
        """Calculate worth (close * volume) for each row."""
        if not dataframes:
            raise ValueError("No dataframes provided")
        
        df = dataframes[0]
        self.logger.info("Starting worth calculation")
        
        df = df.withColumn("worth", F.col("close") * F.col("volume"))
        
        self.logger.info("Worth calculation completed")
        return df
