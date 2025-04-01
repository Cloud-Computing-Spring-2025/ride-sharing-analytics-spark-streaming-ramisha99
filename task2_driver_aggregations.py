from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, to_timestamp, window
from pyspark.sql.types import StructType, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Task 2 - Driver Aggregations with Time Window") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
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

# Parse JSON and convert timestamp
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed_stream = parsed_stream.withColumn("event_time", to_timestamp("timestamp"))

# Aggregate with window and watermark
aggregated_stream = parsed_stream \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("driver_id")
    ) \
    .agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# Flatten the window struct to two columns: window_start and window_end
flattened_stream = aggregated_stream \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# Write to CSV in append mode
csv_query = flattened_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/task2_driver_aggregates") \
    .option("checkpointLocation", "checkpoints/task2_driver_aggregates") \
    .option("header", "true") \
    .start()

csv_query.awaitTermination()
