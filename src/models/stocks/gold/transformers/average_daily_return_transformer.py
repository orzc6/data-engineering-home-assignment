from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame, functions as F
from src.modules.transformer import Transformer
from src.common.logging_utils import get_logger
import logging


class AverageDailyReturnTransformer(Transformer):
    """Transformer for computing average daily return across all stocks per date."""
    
    def __init__(self, name: str = "average_daily_return_transformer", 
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(name, logger)
    
    def transform(self, dataframes: List[DataFrame]) -> DataFrame:
        """Compute average daily return across all stocks per date."""
        if not dataframes:
            raise ValueError("No dataframes provided")
        
        df = dataframes[0]
        self.logger.info("Computing average daily return")
        
        result = (df.filter(F.col("daily_return").isNotNull())
                 .groupBy("date")
                 .agg(F.avg("daily_return").alias("average_return"))
                 .orderBy("date"))
        
        self.logger.info("Average daily return computation completed")
        return result

