from __future__ import annotations
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.modules.layer import DataLayer
from src.models.stocks.silver.transformers.transformers import transformers
from src.common.logging_utils import get_logger
from src.common.io import write_parquet, layer_path
import logging


class SilverStocksLayer(DataLayer):
    """Silver layer that applies transformers and persists results."""
    
    def __init__(self, spark: SparkSession, output_base: str,
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(
            spark=spark,
            output_base=output_base,
            layer_name="silver",
            transformers=transformers,
            logger=logger
        )
    
    def run(self, input_df: DataFrame) -> Dict[str, DataFrame]:
        """Apply transformers explicitly and persist final result only."""
        self.logger.info("Starting silver layer processing")
        
        results = {}
        current_df = input_df
        
        # Apply data cleaning
        self.logger.info("Applying data cleaning transformer")
        current_df = self.transformers["data_cleaning"].transform([current_df])
        results["data_cleaning"] = current_df
        
        # Apply daily returns calculation
        self.logger.info("Applying daily returns transformer")
        current_df = self.transformers["daily_returns"].transform([current_df])
        results["daily_returns"] = current_df
        
        # Apply worth calculation
        self.logger.info("Applying worth calculation transformer")
        current_df = self.transformers["worth_calculation"].transform([current_df])
        results["worth_calculation"] = current_df
        
        # Persist final result
        output_path = layer_path(self.output_base, self.layer_name, "stocks_silver")
        self.logger.info(f"Persisting final silver layer to {output_path}")
        write_parquet(current_df, output_path, partitions=["date"])  # Partition by date for consistency
        
        self.logger.info("Silver layer processing completed")
        return results
