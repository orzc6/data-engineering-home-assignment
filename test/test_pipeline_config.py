#!/usr/bin/env python3
"""
Test script to demonstrate Pydantic configuration features.
"""

import os
from pydantic import ValidationError
from src.modules.pipeline.pipeline_config import PipelineConfig


def test_pydantic_config():
    """Test Pydantic configuration features."""
    print("🧪 Testing Pydantic Configuration Features")
    print("=" * 50)
    
    # Test 1: Valid configuration
    print("\n✅ Test 1: Valid Configuration")
    try:
        config = PipelineConfig(
            input_s3_path="s3://my-bucket/input.csv",
            output_base="s3://my-bucket/output"
        )
        print(f"✅ Valid config created: {config.bronze_table_name}")
        print(f"✅ Config dict: {config.to_dict()}")
    except ValidationError as e:
        print(f"❌ Validation error: {e}")
    
    # Test 2: Invalid S3 path
    print("\n❌ Test 2: Invalid S3 Path")
    try:
        config = PipelineConfig(
            input_s3_path="invalid-path",
            output_base="s3://my-bucket/output"
        )
        print("❌ Should have failed!")
    except ValidationError as e:
        print(f"✅ Correctly caught validation error: {e}")
    
    # Test 3: Invalid memory format
    print("\n❌ Test 3: Invalid Memory Format")
    try:
        config = PipelineConfig(
            input_s3_path="s3://my-bucket/input.csv",
            output_base="s3://my-bucket/output",
            spark_executor_memory="invalid-memory"
        )
        print("❌ Should have failed!")
    except ValidationError as e:
        print(f"✅ Correctly caught validation error: {e}")
    
    # Test 4: Invalid executor cores
    print("\n❌ Test 4: Invalid Executor Cores")
    try:
        config = PipelineConfig(
            input_s3_path="s3://my-bucket/input.csv",
            output_base="s3://my-bucket/output",
            spark_executor_cores=10  # Max is 8
        )
        print("❌ Should have failed!")
    except ValidationError as e:
        print(f"✅ Correctly caught validation error: {e}")
    
    # Test 5: Missing required fields
    print("\n❌ Test 5: Missing Required Fields")
    try:
        config = PipelineConfig(
            input_s3_path="",  # Empty required field
            output_base="s3://my-bucket/output"
        )
        print("❌ Should have failed!")
    except ValidationError as e:
        print(f"✅ Correctly caught validation error: {e}")
    
    # Test 6: Environment variables
    print("\n🌍 Test 6: Environment Variables")
    os.environ["INPUT_S3_PATH"] = "s3://env-bucket/input.csv"
    os.environ["OUTPUT_S3_PATH"] = "s3://env-bucket/output"
    os.environ["SPARK_EXECUTOR_CORES"] = "4"
    
    try:
        config = PipelineConfig.from_env()
        print(f"✅ Config from env: {config.input_s3_path}")
        print(f"✅ Executor cores: {config.spark_executor_cores}")
    except ValidationError as e:
        print(f"❌ Environment config error: {e}")
    
    # Test 7: Field descriptions
    print("\n📝 Test 7: Field Descriptions")
    print("Field descriptions:")
    for field_name, field_info in PipelineConfig.__fields__.items():
        print(f"  {field_name}: {field_info.description}")
    
    print("\n🎉 All Pydantic configuration tests completed!")


if __name__ == "__main__":
    test_pydantic_config()

