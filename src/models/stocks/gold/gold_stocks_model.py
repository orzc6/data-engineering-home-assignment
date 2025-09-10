from __future__ import annotations
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.modules.layer import DataLayer
from src.models.stocks.gold.transformers.transformers import transformers
from src.common.logging_utils import get_logger
from src.common.io import write_parquet, layer_path
import logging


class GoldStocksLayer(DataLayer):
    """Gold layer that applies transformers and persists results."""
    
    def __init__(self, spark: SparkSession, output_base: str,
                 logger: logging.Logger = get_logger(__name__)) -> None:
        
        super().__init__(
            spark=spark,
            output_base=output_base,
            layer_name="gold",
            transformers=transformers,
            logger=logger
        )
    
    def run(self, input_df: DataFrame) -> Dict[str, DataFrame]:
        """Apply transformers explicitly and persist final results."""
        self.logger.info("Starting gold layer processing")
        
        results = {}
        
        # Apply average daily return calculation
        self.logger.info("Applying average daily return transformer")
        avg_daily_return_df = self.transformers["average_daily_return"].transform([input_df])
        results["average_daily_return"] = avg_daily_return_df
        
        # Persist average daily return
        output_path = layer_path(self.output_base, self.layer_name, "average_daily_return")
        self.logger.info(f"Persisting average_daily_return to {output_path}")
        write_parquet(avg_daily_return_df, output_path, partitions=["date"])
        
        # Apply highest worth calculation
        self.logger.info("Applying highest worth transformer")
        highest_worth_df = self.transformers["highest_worth"].transform([input_df])
        results["highest_worth"] = highest_worth_df
        
        # Persist highest worth
        output_path = layer_path(self.output_base, self.layer_name, "highest_worth")
        self.logger.info(f"Persisting highest_worth to {output_path}")
        write_parquet(highest_worth_df, output_path, partitions=[])  # No partitioning for aggregated results
        
        # Apply most volatile calculation
        self.logger.info("Applying most volatile transformer")
        most_volatile_df = self.transformers["most_volatile"].transform([input_df])
        results["most_volatile"] = most_volatile_df
        
        # Persist most volatile
        output_path = layer_path(self.output_base, self.layer_name, "most_volatile")
        self.logger.info(f"Persisting most_volatile to {output_path}")
        write_parquet(most_volatile_df, output_path, partitions=[])  # No partitioning for aggregated results
        
        # Apply top 30-day returns calculation
        self.logger.info("Applying top 30-day returns transformer")
        top_30day_returns_df = self.transformers["top_30day_returns"].transform([input_df])
        results["top_30day_returns"] = top_30day_returns_df
        
        # Persist top 30-day returns
        output_path = layer_path(self.output_base, self.layer_name, "top_30day_returns")
        self.logger.info(f"Persisting top_30day_returns to {output_path}")
        write_parquet(top_30day_returns_df, output_path, partitions=[])  # No partitioning for aggregated results
        
        self.logger.info("Gold layer processing completed")
        return results
