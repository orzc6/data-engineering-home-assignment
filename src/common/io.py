# core/io.py
from __future__ import annotations
from typing import Iterable, Optional, List
from pyspark.sql import DataFrame

def layer_path(base: str, layer: str, table: str) -> str:
    base = base.rstrip("/")
    return f"{base}/{layer}/{table}/"

def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partitions: Optional[List[str]] = None,
    coalesce_n: Optional[int] = None,
    repartition_n: Optional[int] = None,
) -> None:
    w = df
    if repartition_n:
        w = w.repartition(repartition_n)
    if coalesce_n:
        w = w.coalesce(coalesce_n)
    writer = w.write.mode(mode)
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.parquet(path)