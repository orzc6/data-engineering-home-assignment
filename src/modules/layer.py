from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
import logging
from src.modules.transformer import Transformer


class DataLayer(ABC):
    """Abstract layer that applies transformers and persists results as parquet."""
    def __init__(self, spark: SparkSession, output_base: str, layer_name: str,
                 transformers: Dict[str, Transformer],
                 logger: Optional[logging.Logger] = None) -> None:
        self.spark = spark
        self.output_base = output_base.rstrip("/")
        self.layer_name = layer_name
        self.transformers = transformers
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def run(self, input_dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Apply transformers to input_df, persist each, return a dict of {transformer_name: df}."""
        raise NotImplementedError