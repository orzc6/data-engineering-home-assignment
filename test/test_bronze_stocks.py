import pytest
import tempfile
import os
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from src.models.stocks.bronze.bronze_stocks_model import BronzeStocksCsvLoader
from src.models.stocks.bronze.bronze_stock_schema import bronze_stocks_schema


@pytest.fixture(scope="session")
def spark():
    """Spark session for tests"""
    spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def loader(spark):
    """BronzeStocksCsvLoader instance"""
    return BronzeStocksCsvLoader(
        spark=spark,
        input_s3_path="s3://test/test.csv",
        output_base="s3://test-output"
    )


def test_initialization(loader):
    """Test loader initialization"""
    assert loader.table_name == "stocks_raw"
    assert loader.partition_columns == ["ticker"]


def test_transform_data(loader, spark):
    """Test data transformation"""
    # Create test data
    data = [("2023-01-01", "AAPL", 150.0, 1000000.0)]
    df = spark.createDataFrame(data, bronze_stocks_schema)
    
    # Transform data
    result = loader.transform_data(df)
    
    # Check date column type
    assert isinstance(result.schema["date"].dataType, DateType)
    assert result.collect()[0]["date"] is not None


def test_persist_data(loader, spark):
    """Test data persistence"""
    data = [("2023-01-01", "AAPL", 150.0, 1000000.0)]
    df = spark.createDataFrame(data, bronze_stocks_schema)
    
    with patch('loaders.csv_loader.write_parquet') as mock_write, \
         patch('loaders.csv_loader.layer_path') as mock_path:
        
        mock_path.return_value = "s3://test-output/bronze/stocks_raw"
        
        loader.persist_data(df)
        
        mock_write.assert_called_once()
        mock_path.assert_called_once()


def test_csv_reading(loader, spark):
    """Test CSV reading"""
    # Create temp CSV file
    csv_content = "date,ticker,close,volume\n2023-01-01,AAPL,150.0,1000000.0"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        temp_path = f.name
    
    try:
        # Create loader with temp file
        test_loader = BronzeStocksCsvLoader(
            spark=spark,
            input_s3_path=temp_path,
            output_base="s3://test-output"
        )
        
        # Read CSV
        df = test_loader.read_csv()
        
        # Verify
        assert df.count() == 1
        assert df.collect()[0]["ticker"] == "AAPL"
        
    finally:
        os.unlink(temp_path)
