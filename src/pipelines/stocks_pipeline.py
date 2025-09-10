from __future__ import annotations
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from src.models.stocks.bronze.bronze_stocks_model import BronzeStocksCsvLoader
from src.models.stocks.silver.silver_stocks_model import SilverStocksLayer
from src.models.stocks.gold.gold_stocks_model import GoldStocksLayer
from src.modules.pipeline.pipeline_config import PipelineConfig
from src.common.logging_utils import get_logger
from src.common.const import BRONZE_LAYER
import logging


class StocksPipeline:
    """Complete medallion architecture pipeline for stocks data processing."""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig,
                 logger: Optional[logging.Logger] = None) -> None:
        self.spark = spark
        self.config = config
        self.logger = logger or get_logger(__name__)
        
        # Initialize layers
        self.bronze_loader = BronzeStocksCsvLoader(
            spark=spark,
            input_s3_path=config.input_s3_path,
            output_base=config.output_base,
            bronze_table_name=BRONZE_LAYER,
            logger=self.logger
        )
        
        self.silver_layer = SilverStocksLayer(
            spark=spark,
            output_base=config.output_base,
            logger=self.logger
        )
        
        self.gold_layer = GoldStocksLayer(
            spark=spark,
            output_base=config.output_base,
            logger=self.logger
        )
    
    def run(self) -> Dict[str, Any]:
        """Execute the complete medallion pipeline."""
        self.logger.info("Starting Medallion Pipeline execution")
        
        try:
            # Bronze Layer: Load and clean raw data
            self.logger.info("=== BRONZE LAYER ===")
            bronze_df = self.bronze_loader.load()
            self.logger.info(f"Bronze layer completed. Records: {bronze_df.count()}")
            
            # Silver Layer: Apply business transformations
            self.logger.info("=== SILVER LAYER ===")
            silver_results = self.silver_layer.run(bronze_df)
            silver_df = silver_results["worth_calculation"]  # Final silver result
            self.logger.info(f"Silver layer completed. Records: {silver_df.count()}")
            
            # Gold Layer: Compute business metrics
            self.logger.info("=== GOLD LAYER ===")
            gold_results = self.gold_layer.run(silver_df)
            self.logger.info("Gold layer completed")
            
            # Summary
            pipeline_results = {
                "bronze_records": bronze_df.count(),
                "silver_records": silver_df.count(),
                "gold_results": {
                    "average_daily_return_records": gold_results["average_daily_return"].count(),
                    "highest_worth_records": gold_results["highest_worth"].count(),
                    "most_volatile_records": gold_results["most_volatile"].count(),
                    "top_30day_returns_records": gold_results["top_30day_returns"].count(),
                },
                "status": "SUCCESS"
            }
            
            self.logger.info("=== PIPELINE COMPLETED SUCCESSFULLY ===")
            self.logger.info(f"Pipeline Results: {pipeline_results}")
            
            return pipeline_results
            
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {str(e)}")
            raise
    