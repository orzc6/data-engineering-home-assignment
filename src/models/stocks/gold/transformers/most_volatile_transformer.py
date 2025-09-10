from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame, functions as F
from src.modules.transformer import Transformer
from src.common.logging_utils import get_logger
import logging


class MostVolatileTransformer(Transformer):
    """Transformer for computing most volatile stock by annualized standard deviation."""
    
    def __init__(self, name: str = "most_volatile_transformer", 
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(name, logger)
    
    def transform(self, dataframes: List[DataFrame]) -> DataFrame:
        """Compute most volatile stock by annualized standard deviation."""
        if not dataframes:
            raise ValueError("No dataframes provided")
        
        df = dataframes[0]
        self.logger.info("Computing most volatile stock")
        
        # Calculate daily return standard deviation per ticker
        volatility = (df.filter(F.col("daily_return").isNotNull())
                     .groupBy("ticker")
                     .agg(F.stddev_samp("daily_return").alias("stddev_daily")))
        
        # Annualize the standard deviation (252 trading days)
        volatility = volatility.withColumn(
            "annualized_volatility", 
            F.col("stddev_daily") * F.sqrt(F.lit(252.0))
        )
        
        # Get the most volatile stock
        result = (volatility.select("ticker", "annualized_volatility")
                 .withColumnRenamed("annualized_volatility", "standard_deviation")
                 .orderBy(F.col("standard_deviation").desc())
                 .limit(1))
        
        self.logger.info("Most volatile stock computation completed")
        return result

