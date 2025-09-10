from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.modules.schema import Schema

bronze_stocks_schema = Schema(
    table_schema=StructType([
        StructField("date", StringType(), True),      # Date column
        StructField("open", DoubleType(), True),      # Open price
        StructField("high", DoubleType(), True),     # High price  
        StructField("low", DoubleType(), True),      # Low price
        StructField("close", DoubleType(), True),    # Close price
        StructField("volume", DoubleType(), True),   # Volume
        StructField("ticker", StringType(), True),   # Ticker symbol
    ]),
    partition_columns=["date"],  # Partition by date for better performance
)