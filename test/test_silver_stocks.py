import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.models.stocks.silver.silver_stocks_model import SilverStocksLayer
from src.models.stocks.bronze.bronze_stock_schema import bronze_stocks_schema


@pytest.fixture(scope="session")
def spark():
    """Spark session for tests"""
    spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def silver_layer(spark):
    """SilverStocksLayer instance"""
    return SilverStocksLayer(
        spark=spark,
        output_base="s3://test-output"
    )


@pytest.fixture
def sample_bronze_data(spark):
    """Sample bronze data for testing"""
    data = [
        ("2023-01-01", "AAPL", 150.0, 1000000.0),
        ("2023-01-02", "AAPL", 155.0, 1200000.0),
        ("2023-01-01", "GOOGL", 2800.0, 500000.0),
        ("2023-01-02", "GOOGL", 2850.0, 600000.0),
    ]
    return spark.createDataFrame(data, bronze_stocks_schema.table_schema)


def test_initialization(silver_layer):
    """Test silver layer initialization"""
    assert silver_layer.layer_name == "silver"
    assert silver_layer.output_base == "s3://test-output"
    assert "data_cleaning" in silver_layer.transformers
    assert "daily_returns" in silver_layer.transformers
    assert "worth_calculation" in silver_layer.transformers


def test_transformers_initialized(silver_layer):
    """Test that all transformers are properly initialized"""
    transformers = silver_layer.transformers
    
    # Check that transformers are instances of the correct classes
    assert transformers["data_cleaning"].name == "data_cleaning_transformer"
    assert transformers["daily_returns"].name == "daily_returns_transformer"
    assert transformers["worth_calculation"].name == "worth_calculation_transformer"


def test_run_method_structure(silver_layer, sample_bronze_data):
    """Test the run method applies all transformers in sequence"""
    with patch('src.models.stocks.silver.silver_stocks_model.write_parquet') as mock_write, \
         patch('src.models.stocks.silver.silver_stocks_model.layer_path') as mock_path:
        
        mock_path.return_value = "s3://test-output/silver/stocks_silver"
        
        # Mock the transformers to return the input DataFrame
        for transformer in silver_layer.transformers.values():
            transformer.transform = MagicMock(return_value=sample_bronze_data)
        
        results = silver_layer.run(sample_bronze_data)
        
        # Check that all transformers were called
        assert silver_layer.transformers["data_cleaning"].transform.called
        assert silver_layer.transformers["daily_returns"].transform.called
        assert silver_layer.transformers["worth_calculation"].transform.called
        
        # Check that results contain all transformer outputs
        assert "data_cleaning" in results
        assert "daily_returns" in results
        assert "worth_calculation" in results
        
        # Check that final result was persisted
        mock_write.assert_called_once()
        mock_path.assert_called_once()


def test_run_method_persistence(silver_layer, sample_bronze_data):
    """Test that the run method persists data correctly"""
    with patch('src.models.stocks.silver.silver_stocks_model.write_parquet') as mock_write, \
         patch('src.models.stocks.silver.silver_stocks_model.layer_path') as mock_path:
        
        mock_path.return_value = "s3://test-output/silver/stocks_silver"
        
        # Mock transformers
        for transformer in silver_layer.transformers.values():
            transformer.transform = MagicMock(return_value=sample_bronze_data)
        
        silver_layer.run(sample_bronze_data)
        
        # Verify persistence call
        mock_write.assert_called_once_with(
            sample_bronze_data, 
            "s3://test-output/silver/stocks_silver", 
            partitions=["ticker"]
        )
        mock_path.assert_called_once_with(
            "s3://test-output", 
            "silver", 
            "stocks_silver"
        )


def test_run_method_data_flow(silver_layer, sample_bronze_data):
    """Test that data flows correctly through transformers"""
    # Create mock transformers that modify data
    mock_df1 = MagicMock()
    mock_df2 = MagicMock()
    mock_df3 = MagicMock()
    
    silver_layer.transformers["data_cleaning"].transform = MagicMock(return_value=mock_df1)
    silver_layer.transformers["daily_returns"].transform = MagicMock(return_value=mock_df2)
    silver_layer.transformers["worth_calculation"].transform = MagicMock(return_value=mock_df3)
    
    with patch('src.common.io.write_parquet'), \
         patch('src.common.io.layer_path'):
        
        results = silver_layer.run(sample_bronze_data)
        
        # Check that each transformer receives the output of the previous one
        silver_layer.transformers["data_cleaning"].transform.assert_called_once_with([sample_bronze_data])
        silver_layer.transformers["daily_returns"].transform.assert_called_once_with([mock_df1])
        silver_layer.transformers["worth_calculation"].transform.assert_called_once_with([mock_df2])
        
        # Check that results contain the correct DataFrames
        assert results["data_cleaning"] == mock_df1
        assert results["daily_returns"] == mock_df2
        assert results["worth_calculation"] == mock_df3


def test_run_method_error_handling(silver_layer, sample_bronze_data):
    """Test error handling in run method"""
    # Make one transformer fail
    silver_layer.transformers["data_cleaning"].transform = MagicMock(side_effect=Exception("Test error"))
    
    with pytest.raises(Exception, match="Test error"):
        silver_layer.run(sample_bronze_data)


def test_run_method_empty_input(silver_layer):
    """Test run method with empty DataFrame"""
    empty_df = silver_layer.spark.createDataFrame([], bronze_stocks_schema.table_schema)
    
    with patch('src.models.stocks.silver.silver_stocks_model.write_parquet'), \
         patch('src.models.stocks.silver.silver_stocks_model.layer_path'):
        
        # Mock transformers to handle empty data
        for transformer in silver_layer.transformers.values():
            transformer.transform = MagicMock(return_value=empty_df)
        
        results = silver_layer.run(empty_df)
        
        # Should still complete successfully
        assert "data_cleaning" in results
        assert "daily_returns" in results
        assert "worth_calculation" in results


def test_run_method_logging(silver_layer, sample_bronze_data):
    """Test that run method logs appropriately"""
    with patch('src.models.stocks.silver.silver_stocks_model.write_parquet'), \
         patch('src.models.stocks.silver.silver_stocks_model.layer_path'), \
         patch.object(silver_layer.logger, 'info') as mock_logger:
        
        # Mock transformers
        for transformer in silver_layer.transformers.values():
            transformer.transform = MagicMock(return_value=sample_bronze_data)
        
        silver_layer.run(sample_bronze_data)
        
        # Check that appropriate log messages were called
        assert mock_logger.call_count >= 5  # At least 5 log messages
        log_calls = [call[0][0] for call in mock_logger.call_args_list]
        
        assert "Starting silver layer processing" in log_calls
        assert "Applying data cleaning transformer" in log_calls
        assert "Applying daily returns transformer" in log_calls
        assert "Applying worth calculation transformer" in log_calls
        assert "Silver layer processing completed" in log_calls
