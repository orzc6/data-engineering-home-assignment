import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.models.stocks.gold.gold_stocks_model import GoldStocksLayer
from src.models.stocks.bronze.bronze_stock_schema import bronze_stocks_schema


@pytest.fixture(scope="session")
def spark():
    """Spark session for tests"""
    spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def gold_layer(spark):
    """GoldStocksLayer instance"""
    return GoldStocksLayer(
        spark=spark,
        output_base="s3://test-output"
    )


@pytest.fixture
def sample_silver_data(spark):
    """Sample silver data for testing (includes daily_return and worth columns)"""
    data = [
        ("2023-01-01", "AAPL", 150.0, 1000000.0, 0.05, 150000000.0),
        ("2023-01-02", "AAPL", 155.0, 1200000.0, 0.033, 186000000.0),
        ("2023-01-01", "GOOGL", 2800.0, 500000.0, 0.02, 1400000000.0),
        ("2023-01-02", "GOOGL", 2850.0, 600000.0, 0.018, 1710000000.0),
        ("2023-01-01", "MSFT", 300.0, 800000.0, 0.03, 240000000.0),
        ("2023-01-02", "MSFT", 310.0, 900000.0, 0.033, 279000000.0),
    ]
    
    # Create schema with additional columns for silver data
    silver_schema = StructType([
        StructField("date", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("daily_return", DoubleType(), True),
        StructField("worth", DoubleType(), True),
    ])
    
    return spark.createDataFrame(data, silver_schema)


def test_initialization(gold_layer):
    """Test gold layer initialization"""
    assert gold_layer.layer_name == "gold"
    assert gold_layer.output_base == "s3://test-output"
    assert "average_daily_return" in gold_layer.transformers
    assert "highest_worth" in gold_layer.transformers
    assert "most_volatile" in gold_layer.transformers
    assert "top_30day_returns" in gold_layer.transformers


def test_transformers_initialized(gold_layer):
    """Test that all transformers are properly initialized"""
    transformers = gold_layer.transformers
    
    # Check that transformers are instances of the correct classes
    assert transformers["average_daily_return"].name == "average_daily_return_transformer"
    assert transformers["highest_worth"].name == "highest_worth_transformer"
    assert transformers["most_volatile"].name == "most_volatile_transformer"
    assert transformers["top_30day_returns"].name == "top_30day_returns_transformer"


def test_run_method_structure(gold_layer, sample_silver_data):
    """Test the run method applies all transformers"""
    with patch('src.models.stocks.gold.gold_stocks_model.write_parquet') as mock_write, \
         patch('src.models.stocks.gold.gold_stocks_model.layer_path') as mock_path:
        
        mock_path.return_value = "s3://test-output/gold/test_table"
        
        # Mock the transformers to return sample DataFrames
        for transformer in gold_layer.transformers.values():
            transformer.transform = MagicMock(return_value=sample_silver_data)
        
        results = gold_layer.run(sample_silver_data)
        
        # Check that all transformers were called
        assert gold_layer.transformers["average_daily_return"].transform.called
        assert gold_layer.transformers["highest_worth"].transform.called
        assert gold_layer.transformers["most_volatile"].transform.called
        assert gold_layer.transformers["top_30day_returns"].transform.called
        
        # Check that results contain all transformer outputs
        assert "average_daily_return" in results
        assert "highest_worth" in results
        assert "most_volatile" in results
        assert "top_30day_returns" in results
        
        # Check that all results were persisted (4 calls)
        assert mock_write.call_count == 4
        assert mock_path.call_count == 4


def test_run_method_persistence(gold_layer, sample_silver_data):
    """Test that the run method persists data correctly"""
    with patch('src.models.stocks.gold.gold_stocks_model.write_parquet') as mock_write, \
         patch('src.models.stocks.gold.gold_stocks_model.layer_path') as mock_path:
        
        mock_path.return_value = "s3://test-output/gold/test_table"
        
        # Mock transformers
        for transformer in gold_layer.transformers.values():
            transformer.transform = MagicMock(return_value=sample_silver_data)
        
        gold_layer.run(sample_silver_data)
        
        # Verify persistence calls
        assert mock_write.call_count == 4
        
        # Check that layer_path was called with correct parameters
        expected_calls = [
            ("s3://test-output", "gold", "average_daily_return"),
            ("s3://test-output", "gold", "highest_worth"),
            ("s3://test-output", "gold", "most_volatile"),
            ("s3://test-output", "gold", "top_30day_returns"),
        ]
        
        actual_calls = [call[0] for call in mock_path.call_args_list]
        for expected_call in expected_calls:
            assert expected_call in actual_calls


def test_run_method_data_flow(gold_layer, sample_silver_data):
    """Test that data flows correctly through transformers"""
    # Create mock transformers that return different DataFrames
    mock_df1 = MagicMock()
    mock_df2 = MagicMock()
    mock_df3 = MagicMock()
    mock_df4 = MagicMock()
    
    gold_layer.transformers["average_daily_return"].transform = MagicMock(return_value=mock_df1)
    gold_layer.transformers["highest_worth"].transform = MagicMock(return_value=mock_df2)
    gold_layer.transformers["most_volatile"].transform = MagicMock(return_value=mock_df3)
    gold_layer.transformers["top_30day_returns"].transform = MagicMock(return_value=mock_df4)
    
    with patch('src.models.stocks.gold.gold_stocks_model.write_parquet'), \
         patch('src.models.stocks.gold.gold_stocks_model.layer_path'):
        
        results = gold_layer.run(sample_silver_data)
        
        # Check that each transformer receives the input DataFrame
        gold_layer.transformers["average_daily_return"].transform.assert_called_once_with([sample_silver_data])
        gold_layer.transformers["highest_worth"].transform.assert_called_once_with([sample_silver_data])
        gold_layer.transformers["most_volatile"].transform.assert_called_once_with([sample_silver_data])
        gold_layer.transformers["top_30day_returns"].transform.assert_called_once_with([sample_silver_data])
        
        # Check that results contain the correct DataFrames
        assert results["average_daily_return"] == mock_df1
        assert results["highest_worth"] == mock_df2
        assert results["most_volatile"] == mock_df3
        assert results["top_30day_returns"] == mock_df4


def test_run_method_error_handling(gold_layer, sample_silver_data):
    """Test error handling in run method"""
    # Make one transformer fail
    gold_layer.transformers["average_daily_return"].transform = MagicMock(side_effect=Exception("Test error"))
    
    with pytest.raises(Exception, match="Test error"):
        gold_layer.run(sample_silver_data)


def test_run_method_empty_input(gold_layer):
    """Test run method with empty DataFrame"""
    empty_df = gold_layer.spark.createDataFrame([], bronze_stocks_schema.table_schema)
    
    with patch('src.models.stocks.gold.gold_stocks_model.write_parquet'), \
         patch('src.models.stocks.gold.gold_stocks_model.layer_path'):
        
        # Mock transformers to handle empty data
        for transformer in gold_layer.transformers.values():
            transformer.transform = MagicMock(return_value=empty_df)
        
        results = gold_layer.run(empty_df)
        
        # Should still complete successfully
        assert "average_daily_return" in results
        assert "highest_worth" in results
        assert "most_volatile" in results
        assert "top_30day_returns" in results


def test_run_method_logging(gold_layer, sample_silver_data):
    """Test that run method logs appropriately"""
    with patch('src.models.stocks.gold.gold_stocks_model.write_parquet'), \
         patch('src.models.stocks.gold.gold_stocks_model.layer_path'), \
         patch.object(gold_layer.logger, 'info') as mock_logger:
        
        # Mock transformers
        for transformer in gold_layer.transformers.values():
            transformer.transform = MagicMock(return_value=sample_silver_data)
        
        gold_layer.run(sample_silver_data)
        
        # Check that appropriate log messages were called
        assert mock_logger.call_count >= 9  # At least 9 log messages
        log_calls = [call[0][0] for call in mock_logger.call_args_list]
        
        assert "Starting gold layer processing" in log_calls
        assert "Applying average daily return transformer" in log_calls
        assert "Applying highest worth transformer" in log_calls
        assert "Applying most volatile transformer" in log_calls
        assert "Applying top 30-day returns transformer" in log_calls
        assert "Gold layer processing completed" in log_calls

