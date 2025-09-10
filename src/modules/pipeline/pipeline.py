from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
from pyspark.sql import SparkSession
import logging
from src.loaders.csv_loader import CsvS3Loader
from src.modules.layer import DataLayer


class DataPipe(ABC):
    """Abstract pipeline that wires loader + layers and returns success boolean."""
    def __init__(self, spark: SparkSession, loader: CsvS3Loader,
                 silver_layer: DataLayer, gold_layer: DataLayer,
                 logger: Optional[logging.Logger] = None) -> None:
        self.spark = spark
        self.loader = loader
        self.silver_layer = silver_layer
        self.gold_layer = gold_layer
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def run(self) -> bool:
        """Orchestrate the full run, return True/False for status."""
        raise NotImplementedError