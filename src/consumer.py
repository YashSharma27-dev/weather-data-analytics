import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

# Initialize Spark session with Kafka and Hadoop configurations
spark = SparkSession.builder \
    .appName("WeatherKafkaHDFSConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema matching the producer payload
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("temperature_c", DoubleType()) \
    .add("humidity_pct", DoubleType()) \
    .add("wind_speed", DoubleType()) \
    .add("location", StringType())

# Read streaming data from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-stream") \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write stream continuously to HDFS sink
query = parsed_df.writeStream \
    .format("csv") \
    .option("path", "hdfs://localhost:9000/weather/data") \
    .option("checkpointLocation", "hdfs://localhost:9000/weather/checkpoint") \
    .outputMode("append") \
    .start()

print("PySpark Structured Streaming is running and writing to HDFS...")
query.awaitTermination()
