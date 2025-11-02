#!/usr/bin/env python3

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from src.pipelines.stocks_pipeline import StocksPipeline
from src.modules.pipeline.pipeline_config import PipelineConfig
from src.common.logging_utils import get_logger


def main():
    """Main entry point for the Glue job."""
    
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = get_logger('main')
    
    try:
        # Get job arguments
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'INPUT_S3_PATH',
            'OUTPUT_S3_PATH',
            'BRONZE_TABLE_NAME'
        ])
        job_name = args['JOB_NAME']
        
        # Initialize Glue job
        job = Job(glueContext)
        job.init(job_name, {})
        
        # Create configuration from Glue job arguments
        config = PipelineConfig(
            input_s3_path=args['INPUT_S3_PATH'],
            output_base=args['OUTPUT_S3_PATH'],
            bronze_table_name=args['BRONZE_TABLE_NAME']
        )
        
        # Create and run pipeline
        pipeline = StocksPipeline(spark, config)
        pipeline.run()
        
        # Commit job
        job.commit()
        
    except Exception as e:
        logger.info(f"Execution of job '{job_name}' failed.")
        raise


if __name__ == "__main__":
    main()
