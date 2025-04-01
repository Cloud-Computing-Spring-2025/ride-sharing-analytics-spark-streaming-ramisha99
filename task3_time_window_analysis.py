from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, to_timestamp, window
from pyspark.sql.types import StructType, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Task 3 - Time Window Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming JSON
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", FloatType()) \
    .add("fare_amount", FloatType()) \
    .add("timestamp", StringType())

# Read from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse and convert timestamp
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")
stream_with_time = parsed_stream.withColumn("event_time", to_timestamp("timestamp"))

# Apply watermark and windowed aggregation
windowed_stream = stream_with_time \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute")
    ).agg(
        sum("fare_amount").alias("total_fare")
    )

# Flatten window struct into separate columns
flattened_stream = windowed_stream \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# Write to CSV in append mode
csv_query = flattened_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/task3_windowed_fares") \
    .option("checkpointLocation", "checkpoints/task3_windowed_fares") \
    .option("header", "true") \
    .start()

csv_query.awaitTermination()
