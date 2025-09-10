from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame, functions as F
from src.modules.transformer import Transformer
from src.common.logging_utils import get_logger
import logging


class HighestWorthTransformer(Transformer):
    """Transformer for computing stock with highest average worth (close * volume)."""
    
    def __init__(self, name: str = "highest_worth_transformer", 
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(name, logger)
    
    def transform(self, dataframes: List[DataFrame]) -> DataFrame:
        """Compute stock with highest average worth."""
        if not dataframes:
            raise ValueError("No dataframes provided")
        
        df = dataframes[0]
        self.logger.info("Computing highest worth stock")
        
        result = (df.groupBy("ticker")
                 .agg(F.avg("worth").alias("value"))
                 .orderBy(F.col("value").desc())
                 .limit(1))
        
        self.logger.info("Highest worth computation completed")
        return result

