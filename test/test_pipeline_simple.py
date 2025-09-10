import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.pipelines.stocks_pipeline import StocksPipeline
from src.modules.pipeline.pipeline_config import PipelineConfig


@pytest.fixture(scope="session")
def spark():
    """Spark session for tests"""
    spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def pipeline_config():
    """Pipeline configuration for tests"""
    return PipelineConfig(
        input_s3_path="s3://test-bucket/input.csv",
        output_base="s3://test-bucket/output",
        bronze_table_name="test_stocks_raw",
    )


@pytest.fixture
def stocks_pipeline(spark, pipeline_config):
    """StocksPipeline instance"""
    return StocksPipeline(spark, pipeline_config)


@pytest.fixture
def sample_bronze_data(spark):
    """Sample bronze data for testing"""
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
    ])
    
    data = [
        ("2023-01-01", "AAPL", 150.0, 1000000.0),
        ("2023-01-02", "AAPL", 155.0, 1200000.0),
        ("2023-01-01", "GOOGL", 2800.0, 500000.0),
        ("2023-01-02", "GOOGL", 2850.0, 600000.0),
    ]
    
    return spark.createDataFrame(data, schema)


def test_pipeline_initialization(stocks_pipeline):
    """Test pipeline creates all required components"""
    assert stocks_pipeline.spark is not None
    assert stocks_pipeline.config is not None
    assert stocks_pipeline.bronze_loader is not None
    assert stocks_pipeline.silver_layer is not None
    assert stocks_pipeline.gold_layer is not None


def test_pipeline_run_method(stocks_pipeline, sample_bronze_data):
    """Test the pipeline run method"""
    with patch.object(stocks_pipeline.bronze_loader, 'load', return_value=sample_bronze_data), \
         patch.object(stocks_pipeline.silver_layer, 'run') as mock_silver_run, \
         patch.object(stocks_pipeline.gold_layer, 'run') as mock_gold_run:
        
        # Mock silver layer to return expected structure
        mock_silver_df = MagicMock()
        mock_silver_df.count.return_value = 4
        mock_silver_run.return_value = {"worth_calculation": mock_silver_df}
        
        # Mock gold layer to return expected structure
        mock_gold_results = {
            "average_daily_return": MagicMock(),
            "highest_worth": MagicMock(),
            "most_volatile": MagicMock(),
            "top_30day_returns": MagicMock()
        }
        for df in mock_gold_results.values():
            df.count.return_value = 2
        mock_gold_run.return_value = mock_gold_results
        
        results = stocks_pipeline.run()
        
        # Check that all layers were called
        stocks_pipeline.bronze_loader.load.assert_called_once()
        mock_silver_run.assert_called_once_with(sample_bronze_data)
        mock_gold_run.assert_called_once_with(mock_silver_df)
        
        # Check results structure
        assert results["status"] == "SUCCESS"
        assert "bronze_records" in results
        assert "silver_records" in results
        assert "gold_results" in results


def test_config_validation():
    """Test that invalid config raises errors"""
    from pydantic import ValidationError
    
    # Test invalid S3 path
    with pytest.raises(ValidationError):
        PipelineConfig(
            input_s3_path="invalid_path",
            output_base="s3://test-bucket/output"
        )
