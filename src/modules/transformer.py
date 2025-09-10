from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List
from pyspark.sql import DataFrame
import logging

class Transformer(ABC):
    """Abstract transformer: get df → return df (pure, deterministic if input is)."""
    def __init__(self, name: str, logger: logging.Logger) -> None:
        self.name = name
        self.logger = logger

    @abstractmethod
    def transform(self, dataframes: List[DataFrame]) -> DataFrame:
        """Apply transformation and return a DataFrame."""
        raise NotImplementedError