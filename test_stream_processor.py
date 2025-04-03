import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from stream_processor import StreamProcessor
import os
from datetime import datetime
import tempfile
import shutil

class TestStreamProcessor:
    @pytest.fixture(scope="class")
    def spark(self):
        """Create a Spark session for testing"""
        spark = (SparkSession.builder
                .appName("test-stream-processor")
                .master("local[1]")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())
        yield spark
        spark.stop()

    @pytest.fixture
    def test_data_schema(self):
        """Define test data schema"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

    @pytest.fixture
    def test_data(self, spark, test_data_schema):
        """Create test data"""
        data = [
            ("1", 100, datetime.now()),
            ("2", 200, datetime.now()),
            ("3", 300, datetime.now())
        ]
        return spark.createDataFrame(data, schema=test_data_schema)

    @pytest.fixture
    def delta_table_data(self, spark, test_data_schema):
        """Create test Delta table data"""
        data = [
            ("1", 1000, datetime.now()),
            ("2", 2000, datetime.now()),
            ("4", 4000, datetime.now())
        ]
        return spark.createDataFrame(data, schema=test_data_schema)

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_stream_processor_initialization(self, temp_dir):
        """Test StreamProcessor initialization"""
        os.environ["ADLS_KEY"] = "test-key"
        processor = StreamProcessor(
            storage_account="test-account",
            container="test-container",
            delta_table_path=temp_dir
        )
        assert processor.storage_account == "test-account"
        assert processor.container == "test-container"
        assert processor.delta_table_path == temp_dir

    def test_process_stream(self, spark, test_data, delta_table_data, temp_dir):
        """Test stream processing with mock data"""
        # Write delta table data
        delta_table_data.write.format("delta").save(temp_dir)

        # Initialize processor
        processor = StreamProcessor(
            storage_account="test-account",
            container="test-container",
            delta_table_path=temp_dir
        )

        # Process stream
        processed_df = processor.process_stream(test_data)

        # Verify join results
        result = processed_df.collect()
        assert len(result) == 3  # Should have 3 rows from streaming data

        # Verify specific join results
        result_dict = {row.id: row.value for row in result}
        assert result_dict["1"] == 100  # From streaming data
        assert result_dict["2"] == 200  # From streaming data
        assert result_dict["3"] == 300  # From streaming data

    def test_stream_processor_error_handling(self, temp_dir):
        """Test error handling in StreamProcessor"""
        processor = StreamProcessor(
            storage_account="test-account",
            container="test-container",
            delta_table_path=temp_dir
        )

        # Test with invalid input path
        with pytest.raises(Exception):
            processor.start_streaming(
                input_path="invalid/path",
                output_path=temp_dir,
                checkpoint_path=temp_dir
            )

    def test_spark_session_configuration(self, temp_dir):
        """Test Spark session configuration"""
        os.environ["ADLS_KEY"] = "test-key"
        processor = StreamProcessor(
            storage_account="test-account",
            container="test-container",
            delta_table_path=temp_dir
        )

        spark = processor.spark
        assert spark.conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"
        assert spark.conf.get("spark.sql.catalog.spark_catalog") == "org.apache.spark.sql.delta.catalog.DeltaCatalog"

if __name__ == "__main__":
    pytest.main([__file__]) 