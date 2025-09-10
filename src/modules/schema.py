from abc import ABC, abstractmethod
from pyspark.sql.types import StructType
from typing import List

class Schema:
    def __init__(self, table_schema: StructType, partition_columns: List[str]):
        self.table_schema = table_schema
        self.partition_columns = partition_columns

    def get_schema(self) -> StructType:
        return self.table_schema

    def get_partition_columns(self) -> List[str]:
        return self.partition_columns