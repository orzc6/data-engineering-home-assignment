from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from src.modules.transformer import Transformer
from src.common.logging_utils import get_logger
import logging


class DailyReturnsTransformer(Transformer):
    """Transformer for calculating daily returns using window functions."""
    
    def __init__(self, name: str = "daily_returns_transformer", 
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(name, logger)
    
    def transform(self, dataframes: List[DataFrame]) -> DataFrame:
        """Calculate daily returns using window functions."""
        if not dataframes:
            raise ValueError("No dataframes provided")
        
        df = dataframes[0]
        self.logger.info("Starting daily returns calculation")
        
        # Window function for previous day's close
        window_spec = Window.partitionBy("ticker").orderBy("date")
        
        df = df.withColumn("prev_close", F.lag("close").over(window_spec))
        df = df.withColumn("daily_return", 
            (F.col("close") - F.col("prev_close")) / F.col("prev_close")
        )
        
        self.logger.info("Daily returns calculation completed")
        return df
