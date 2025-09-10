# Data Engineering Assignment - Stocks Processing Pipeline

A complete data engineering pipeline built with PySpark, AWS Glue, and CloudFormation that processes stock market data through a medallion architecture (Bronze → Silver → Gold layers).

## 🎯 Project Objectives

This pipeline answers 4 key questions about stock market data:

1. **Average Daily Return**: What is the average daily return of all stocks per date?
2. **Highest Worth**: Which stock has the highest average worth?
3. **Most Volatile**: Which stock is the most volatile by annualized standard deviation?
4. **Top 30-Day Returns**: What are the top 3 stocks with highest 30-day returns?

## 📁 Project Structure

```
data-engineering-home-assignment-or-z/
├── src/                                    # Source code
│   ├── common/                            # Common utilities
│   │   ├── const.py                       # Constants and layer definitions
│   │   ├── io.py                          # I/O utilities for parquet files
│   │   └── logging_utils.py               # Logging configuration
│   ├── jobs/                              # AWS Glue job entry points
│   │   └── stocks_glue_job.py             # Main Glue job script
│   ├── loaders/                           # Data loaders
│   │   └── csv_loader.py                  # CSV data loader
│   ├── models/                            # Data models and transformations
│   │   └── stocks/                        # Stocks-specific models
│   │       ├── bronze/                    # Bronze layer (raw data)
│   │       │   ├── bronze_stock_schema.py # Bronze schema definition
│   │       │   └── bronze_stocks_model.py # Bronze data processing
│   │       ├── silver/                    # Silver layer (cleaned data)
│   │       │   ├── silver_stocks_model.py # Silver data processing
│   │       │   └── transformers/          # Silver transformations
│   │       │       ├── daily_returns_transformer.py
│   │       │       ├── data_cleaning_transformer.py
│   │       │       └── worth_calculation_transformer.py
│   │       └── gold/                       # Gold layer (aggregated data)
│   │           ├── gold_stocks_model.py   # Gold data processing
│   │           └── transformers/           # Gold transformations
│   │               ├── average_daily_return_transformer.py
│   │               ├── highest_worth_transformer.py
│   │               ├── most_volatile_transformer.py
│   │               └── top_30day_returns_transformer.py
│   ├── modules/                           # Core framework modules
│   │   ├── layer.py                       # Base layer class
│   │   ├── loader.py                      # Base loader class
│   │   ├── pipeline/                      # Pipeline framework
│   │   │   ├── pipeline_config.py         # Configuration management
│   │   │   └── pipeline.py                # Base pipeline class
│   │   ├── schema.py                      # Schema management
│   │   └── transformer.py                 # Base transformer class
│   └── pipelines/                         # Pipeline implementations
│       └── stocks_pipeline.py             # Main stocks pipeline
├── test/                                  # Test files
│   ├── test_bronze_stocks.py              # Bronze layer tests
│   ├── test_silver_stocks.py              # Silver layer tests
│   ├── test_gold_stocks.py                # Gold layer tests
│   └── test_pipeline_config.py            # Configuration tests
├── stocks_data.csv                        # Input data file
├── stack.yml                              # CloudFormation template
├── requirements.txt                       # Python dependencies
├── create-update-stack.sh                 # Deployment script
└── README.md                              # This file
```

## 🏗️ Architecture Overview

### Medallion Architecture
- **Bronze Layer**: Raw data ingestion and basic validation
- **Silver Layer**: Data cleaning, enrichment, and business logic
- **Gold Layer**: Aggregated metrics and final results

### Technology Stack
- **PySpark**: Data processing engine
- **AWS Glue**: Serverless ETL service
- **AWS S3**: Data storage
- **AWS Athena**: Query engine for results
- **CloudFormation**: Infrastructure as Code
- **Pydantic**: Configuration management

## 🚀 Quick Start

### Prerequisites
- AWS CLI configured with appropriate credentials
- Python 3.11+
- Access to AWS services (Glue, S3, Athena, CloudFormation)

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest test/

# Run pipeline locally (if needed)
python src/jobs/stocks_glue_job.py
```

### AWS Deployment
1. **Set up environment variables**:
   ```bash
   # Create .env file
   echo "AWS_ACCESS_KEY_ID=your_access_key" >> .env
   echo "AWS_SECRET_ACCESS_KEY=your_secret_key" >> .env
   echo "STACK_NAME=data-engineer-assignment-or-z" >> .env
   ```

2. **Deploy infrastructure**:
   ```bash
   chmod +x create-update-stack.sh
   ./create-update-stack.sh
   ```

3. **Run the pipeline**:
   ```bash
   aws glue start-job-run --job-name stocks-data-processing
   ```

4. **Query results in Athena**:
   ```sql
   -- Average daily returns
   SELECT * FROM stocks_database.average_daily_return 
   ORDER BY date DESC LIMIT 10;
   
   -- Highest worth stock
   SELECT * FROM stocks_database.highest_worth;
   
   -- Most volatile stock
   SELECT * FROM stocks_database.most_volatile;
   
   -- Top 30-day returns
   SELECT * FROM stocks_database.top_30day_returns;
   ```

## 📊 Results

The pipeline generates 4 result tables in the Gold layer:

| **Table** | **Description** | **Columns** |
|-----------|-----------------|-------------|
| `average_daily_return` | Average daily return per date | `date`, `average_return` |
| `highest_worth` | Stock with highest average worth | `ticker`, `value` |
| `most_volatile` | Most volatile stock by std dev | `ticker`, `standard_deviation` |
| `top_30day_returns` | Top 3 stocks with best 30-day returns | `ticker`, `date` |

## 🔧 Configuration

### Pipeline Configuration
The pipeline uses Pydantic for robust configuration management:

```python
class PipelineConfig(BaseModel):
    input_s3_path: str
    output_base: str
    bronze_table_name: str = "stocks_bronze"
```

### AWS Resources
- **Glue Job**: `stocks-data-processing`
- **Database**: `stocks_database`
- **S3 Bucket**: `data-engineer-assignment-or-z`
- **Tables**: 4 Gold layer tables + crawlers

## 🧪 Testing

Run the test suite:
```bash
pytest test/ -v
```

Tests cover:
- Bronze layer data ingestion
- Silver layer transformations
- Gold layer aggregations
- Configuration validation

## 📈 Performance Optimization

### Partitioning Strategy
- **Bronze**: Partitioned by `date` (1,027 partitions)
- **Silver**: Partitioned by `date` for consistency
- **Gold**: No partitioning for aggregated results (except `average_daily_return`)

### Resource Configuration
- **Glue Job**: 10 workers, G.1X worker type
- **Execution Time**: ~10 minutes for full pipeline
- **Data Volume**: ~5,136 rows processed

## 🔍 Monitoring

### CloudWatch Logs
- Log Group: `/aws-glue/jobs/logs-v2`
- Real-time monitoring available
- Detailed execution logs

### Job Status
```bash
aws glue get-job-run --job-name stocks-data-processing --run-id <run-id>
```

## 🛠️ Troubleshooting

### Common Issues
1. **Schema Mismatch**: Ensure CloudFormation table schemas match transformer outputs
2. **Import Errors**: Verify `src/` directory structure in deployment package
3. **Partitioning Issues**: Check partition column configuration
4. **Resource Limits**: Monitor Glue job capacity and timeout settings

### Debug Commands
```bash
# Check job logs
aws logs tail /aws-glue/jobs/logs-v2 --follow

# Verify S3 data
aws s3 ls s3://data-engineer-assignment-or-z/output/ --recursive

# Test Athena queries
aws athena start-query-execution --query-string "SELECT * FROM stocks_database.highest_worth"
```

## 📝 Key Features

- ✅ **Medallion Architecture**: Bronze → Silver → Gold data flow
- ✅ **Infrastructure as Code**: Complete CloudFormation deployment
- ✅ **Schema Validation**: Pydantic-based configuration management
- ✅ **Comprehensive Testing**: Unit tests for all layers
- ✅ **Performance Optimized**: Efficient partitioning and resource allocation
- ✅ **Production Ready**: Error handling, logging, and monitoring
- ✅ **Queryable Results**: Athena integration for data analysis

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is part of a data engineering assignment and follows the original repository's licensing terms.

---

**Note**: Remember to exclude `.env` files containing AWS credentials from version control.
