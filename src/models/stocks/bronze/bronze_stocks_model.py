from __future__ import annotations
from typing import List
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType
from src.loaders.csv_loader import CsvS3Loader
from src.models.stocks.bronze.bronze_stock_schema import bronze_stocks_schema
from src.common.logging_utils import get_logger
import logging


class BronzeStocksCsvLoader(CsvS3Loader):
    """Bronze layer CSV loader specifically for stocks data."""
    
    def __init__(self, spark: SparkSession, input_s3_path: str, output_base: str,
                 bronze_table_name: str = "stocks_raw",
                 partition_columns: List[str] = bronze_stocks_schema.get_partition_columns(),
                 schema: StructType = bronze_stocks_schema.get_schema(),
                 logger: logging.Logger = get_logger(__name__)) -> None:
        super().__init__(spark, input_s3_path, output_base, bronze_table_name, partition_columns, schema, logger)

    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """Apply transformations specific to stocks data."""
        df = df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))
        return df.select("date", "ticker", "close", "volume")
