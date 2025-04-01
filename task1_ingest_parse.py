from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Task 1 - Ingest and Parse Ride Data") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming ride events
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", FloatType()) \
    .add("fare_amount", FloatType()) \
    .add("timestamp", StringType())

# Read streaming data from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON from socket
parsed_stream = raw_stream \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write parsed data to CSV with header
query = parsed_stream.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "output/task1_parsed_rides") \
    .option("checkpointLocation", "checkpoints/task1_parsed_rides") \
    .option("header", "true") \
    .start()

query.awaitTermination()
