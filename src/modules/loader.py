from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
import logging
from src.common.const import BRONZE_LAYER


class DataLoader(ABC):
    """Abstract data loader that ingests data from some source and can persist it as parquet."""
    def __init__(self, spark: SparkSession, logger: Optional[logging.Logger] = None) -> None:
        self.spark = spark
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.layer = BRONZE_LAYER

    @abstractmethod
    def load(self) -> DataFrame:
        """Return a Spark DataFrame loaded from the source."""
        raise NotImplementedError