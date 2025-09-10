from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from src.modules.transformer import Transformer
from src.common.logging_utils import get_logger
import logging


class Top30DayReturnsTransformer(Transformer):
    """Transformer for computing top 3 stocks with highest 30-day returns."""
    
    def __init__(self, name: str = "top_30day_returns_transformer", 
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(name, logger)
    
    def transform(self, dataframes: List[DataFrame]) -> DataFrame:
        """Compute top 3 stocks with highest 30-day returns."""
        if not dataframes:
            raise ValueError("No dataframes provided")
        
        df = dataframes[0]
        self.logger.info("Computing top 30-day returns")
        
        # Create dense calendar for forward-filling
        bounds = df.agg(F.min("date").alias("min_d"), F.max("date").alias("max_d")).collect()[0]
        min_d, max_d = bounds["min_d"], bounds["max_d"]
        
        # Generate date series
        calendar = (df.sql_ctx.range(0, (max_d - min_d).days + 1)
                   .withColumn("date", F.expr(f"date_add(to_date('{min_d}'), cast(id as int))"))
                   .select("date"))
        
        # Cross join with tickers
        tickers = df.select("ticker").distinct()
        dense = tickers.crossJoin(calendar)
        
        # Join and forward-fill
        joined = dense.join(df.select("ticker", "date", "close"), ["ticker", "date"], "left")
        
        # Forward-fill missing closes
        w_fill = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        ff = joined.withColumn("close_ff", F.last("close", ignorenulls=True).over(w_fill))
        
        # Calculate 30-day returns
        ff = ff.withColumn("date_minus_30", F.date_sub(F.col("date"), 30))
        
        # Join with 30 days prior
        prior = ff.select(
            F.col("ticker").alias("t2"), 
            F.col("date").alias("d2"), 
            F.col("close_ff").alias("close_ff_2")
        )
        
        ff = (ff.join(prior, (ff.ticker == prior.t2) & (ff.date_minus_30 == prior.d2), "left")
              .drop("t2", "d2"))
        
        ff = ff.withColumn("ret_30d", 
                          (F.col("close_ff") - F.col("close_ff_2")) / F.col("close_ff_2"))
        
        # Get top 3
        result = (ff.filter(F.col("ret_30d").isNotNull())
                 .select("ticker", "date")
                 .orderBy(F.col("ret_30d").desc())
                 .limit(3))
        
        self.logger.info("Top 30-day returns computation completed")
        return result

