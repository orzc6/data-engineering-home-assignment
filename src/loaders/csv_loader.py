from __future__ import annotations
from abc import abstractmethod
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType
from src.modules.loader import DataLoader
from src.common.io import write_parquet, layer_path
import logging


class CsvS3Loader(DataLoader):
    """Abstract CSV loader that provides common CSV loading functionality."""
    
    def __init__(self, spark: SparkSession, input_path: str, output_base: str,
                 table_name: str, partition_columns: List[str], schema: StructType,
                 logger: logging.Logger) -> None:
        super().__init__(spark, logger)
        self.input_path = input_path
        self.output_base = output_base.rstrip("/")
        self.table_name = table_name
        self.partition_columns = partition_columns
        self.schema = schema

    def read_csv(self, options: Dict[str, Any] = None) -> DataFrame:
        default_options = {"header": True}
        if options:
            default_options.update(options)
        
        reader = self.spark.read
        for key, value in default_options.items():
            reader = reader.option(key, value)
        
        reader = reader.schema(self.schema)
        
        return reader.csv(self.input_path)
    
    @abstractmethod
    def transform_data(self, df: DataFrame) -> DataFrame:
        """Apply any necessary transformations to the loaded data."""
        raise NotImplementedError

    def persist_data(self, df: DataFrame) -> None:
        path = layer_path(self.output_base, self.layer, self.table_name)
        self.logger.info(f"Writing {self.table_name} to {path}")
        write_parquet(df, path, partitions=self.partition_columns)
    
    def load(self) -> DataFrame:
        self.logger.info(f"Loading CSV from {self.input_path}")
        df = self.read_csv()
        df = self.transform_data(df)
        self.persist_data(df)
        
        return df
