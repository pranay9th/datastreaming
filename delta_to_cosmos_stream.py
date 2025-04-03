import os
import time
from datetime import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DeltaToCosmosStream:
    def __init__(self, delta_table_path, cosmos_endpoint, cosmos_key, cosmos_database, cosmos_container):
        """
        Initialize the Delta to Cosmos DB streaming processor.
        
        Args:
            delta_table_path (str): Path to the Delta table
            cosmos_endpoint (str): Cosmos DB endpoint URL
            cosmos_key (str): Cosmos DB access key
            cosmos_database (str): Cosmos DB database name
            cosmos_container (str): Cosmos DB container name
        """
        self.delta_table_path = delta_table_path
        self.cosmos_endpoint = cosmos_endpoint
        self.cosmos_key = cosmos_key
        self.cosmos_database = cosmos_database
        self.cosmos_container = cosmos_container
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Cache for Delta table
        self._cached_delta_table = None
        self._last_refresh_time = None
        self._cache_duration = 3600  # 1 hour in seconds

    def _create_spark_session(self):
        """Create and configure Spark session."""
        return (SparkSession.builder
                .appName("DeltaToCosmosStream")
                .config("spark.jars.packages", 
                       "com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:1.4.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())

    def _get_delta_table(self):
        """Get Delta table with caching."""
        current_time = time.time()
        
        if (self._cached_delta_table is None or 
            self._last_refresh_time is None or 
            current_time - self._last_refresh_time > self._cache_duration):
            
            logger.info("Refreshing Delta table cache")
            self._cached_delta_table = self.spark.read.format("delta").load(self.delta_table_path)
            self._last_refresh_time = current_time
            
        return self._cached_delta_table

    def _write_to_cosmos(self, batch_df):
        """Write batch to Cosmos DB with upsert."""
        if batch_df.rdd.isEmpty():
            logger.info("No records to write in this batch")
            return

        # Configure Cosmos DB connection
        cosmos_config = {
            "Endpoint": self.cosmos_endpoint,
            "Masterkey": self.cosmos_key,
            "Database": self.cosmos_database,
            "Collection": self.cosmos_container,
            "Upsert": "true",
            "WritingMode": "Upsert",
            "idField": "id",  # Specify the ID field for upsert
            "partitionKey": "id",  # Use the same field as partition key
            "bulkImport": "true",  # Enable bulk import for better performance
            "checkpointLocation": "/tmp/cosmos_checkpoint"  # Add checkpoint location for recovery
        }

        # Ensure the ID field exists and is properly formatted
        if "id" not in batch_df.columns:
            logger.warning("No 'id' field found in the DataFrame. Upsert may not work correctly.")
            return

        # Write to Cosmos DB
        try:
            batch_df.write \
                .format("com.microsoft.azure.cosmosdb.spark") \
                .options(**cosmos_config) \
                .mode("overwrite") \
                .save()
            
            logger.info(f"Successfully upserted {batch_df.count()} records to Cosmos DB")
        except Exception as e:
            logger.error(f"Error writing to Cosmos DB: {str(e)}")
            raise

    def start(self, trigger_interval="1 minute"):
        """
        Start the streaming process.
        
        Args:
            trigger_interval (str): Trigger interval for streaming (default: "1 minute")
        """
        logger.info("Starting Delta to Cosmos DB streaming process")
        
        try:
            # Create streaming query
            query = self.spark.readStream \
                .format("delta") \
                .load(self.delta_table_path) \
                .withColumn("processed_timestamp", current_timestamp()) \
                .writeStream \
                .foreachBatch(lambda batch_df, batch_id: self._write_to_cosmos(batch_df)) \
                .trigger(processingTime=trigger_interval) \
                .start()
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming process: {str(e)}")
            raise
        finally:
            self.spark.stop()

def main():
    # Configuration
    delta_table_path = os.getenv("DELTA_TABLE_PATH", "path/to/delta/table")
    cosmos_endpoint = os.getenv("COSMOS_ENDPOINT")
    cosmos_key = os.getenv("COSMOS_KEY")
    cosmos_database = os.getenv("COSMOS_DATABASE")
    cosmos_container = os.getenv("COSMOS_CONTAINER")
    
    # Validate configuration
    required_env_vars = {
        "COSMOS_ENDPOINT": cosmos_endpoint,
        "COSMOS_KEY": cosmos_key,
        "COSMOS_DATABASE": cosmos_database,
        "COSMOS_CONTAINER": cosmos_container
    }
    
    missing_vars = [var for var, value in required_env_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    # Initialize and start streaming
    processor = DeltaToCosmosStream(
        delta_table_path=delta_table_path,
        cosmos_endpoint=cosmos_endpoint,
        cosmos_key=cosmos_key,
        cosmos_database=cosmos_database,
        cosmos_container=cosmos_container
    )
    
    processor.start()

if __name__ == "__main__":
    main() 