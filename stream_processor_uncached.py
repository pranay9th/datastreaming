from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self, storage_account, container, delta_table_path):
        self.storage_account = storage_account
        self.container = container
        self.delta_table_path = delta_table_path
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        """Create and configure Spark session"""
        return (SparkSession.builder
                .appName("ADLS-Delta-Stream-Processor")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("fs.azure.account.key." + self.storage_account + ".dfs.core.windows.net", os.getenv("ADLS_KEY"))
                .getOrCreate())

    def read_streaming_data(self, input_path):
        """Read streaming data from ADLS"""
        return (self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/schema")
                .load(f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{input_path}"))

    def process_stream(self, streaming_df):
        """Process the streaming data and join with Delta table"""
        # Read the Delta table (no caching)
        delta_table = self.spark.read.format("delta").load(self.delta_table_path)
        
        # Perform the join operation
        joined_df = streaming_df.join(
            delta_table,
            streaming_df.id == delta_table.id,
            "left"
        )
        
        return joined_df

    def start_streaming(self, input_path, output_path, checkpoint_path):
        """Start the streaming process"""
        try:
            # Read streaming data
            streaming_df = self.read_streaming_data(input_path)
            
            # Process the stream
            processed_df = self.process_stream(streaming_df)
            
            # Write the stream
            query = (processed_df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", checkpoint_path)
                    .start(output_path))
            
            logger.info("Streaming query started successfully")
            return query
            
        except Exception as e:
            logger.error(f"Error in streaming process: {str(e)}")
            raise

def main():
    # Configuration
    STORAGE_ACCOUNT = "your-storage-account"
    CONTAINER = "your-container"
    DELTA_TABLE_PATH = "abfss://your-container@your-storage-account.dfs.core.windows.net/delta-table"
    INPUT_PATH = "input/streaming-data"
    OUTPUT_PATH = "abfss://your-container@your-storage-account.dfs.core.windows.net/output"
    CHECKPOINT_PATH = "abfss://your-container@your-storage-account.dfs.core.windows.net/checkpoints"
    
    # Initialize and start streaming
    processor = StreamProcessor(STORAGE_ACCOUNT, CONTAINER, DELTA_TABLE_PATH)
    query = processor.start_streaming(INPUT_PATH, OUTPUT_PATH, CHECKPOINT_PATH)
    
    # Wait for the streaming query to terminate
    query.awaitTermination()

if __name__ == "__main__":
    main() 