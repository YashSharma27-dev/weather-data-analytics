"""
PySpark Structured Streaming Pipeline: Kafka to HDFS
=====================================================
Consumes real-time weather observations from Kafka topic 'weather-stream',
parses JSON payloads, computes real-time streaming analytics, and writes
continuously to Hadoop HDFS (Parquet format) and local analytics sink.

Cluster Execution:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
               --master spark://192.168.1.100:7077 \
               streaming/kafka_to_hdfs_stream.py 192.168.1.100:9092 hdfs://192.168.1.100:9000

Standalone / Local Execution:
  python streaming/kafka_to_hdfs_stream.py
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, max as spark_max, min as spark_min,
    count, when, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

def run_streaming_pipeline(kafka_broker, hdfs_namenode, spark_master):
    print("=" * 60)
    print("  Starting PySpark Structured Streaming Pipeline")
    print(f"  Kafka Broker:  {kafka_broker}")
    print(f"  HDFS NameNode: {hdfs_namenode}")
    print(f"  Spark Master:  {spark_master}")
    print("=" * 60)

    # Initialize Spark Session with Kafka connector package
    spark_builder = SparkSession.builder \
        .appName("WeatherKafkaHDFSStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", f"{hdfs_namenode}/weather/checkpoint")

    if spark_master:
        spark_builder = spark_builder.master(spark_master)

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Full Meteorological Schema matching weather_producer.py
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("heat_index", DoubleType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("collected_at", StringType(), True)
    ])

    # 1. Read Stream from Kafka Broker
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", "weather-stream") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Parse JSON Value Field into Typed Columns
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))

    # 3. Add Weather Anomaly Alerts
    enriched_df = parsed_df.withColumn(
        "alert_level",
        when(col("temperature") > 40.0, "HEATWAVE_WARNING")
        .when(col("wind_speed") > 45.0, "STORM_ALERT")
        .when((col("humidity") > 85.0) & (col("temperature") > 32.0), "DANGEROUS_HEAT_INDEX")
        .when(col("precipitation") > 15.0, "FLOOD_RISK")
        .otherwise("NORMAL")
    )

    # 4. Sink 1: Write Raw Enriched Stream to Hadoop HDFS (Parquet format)
    hdfs_raw_path = f"{hdfs_namenode}/weather/streaming_output"
    hdfs_checkpoint = f"{hdfs_namenode}/weather/checkpoint/raw"

    hdfs_query = enriched_df.writeStream \
        .format("parquet") \
        .option("path", hdfs_raw_path) \
        .option("checkpointLocation", hdfs_checkpoint) \
        .outputMode("append") \
        .start()

    print(f"[OK] Stream Sink 1 initialized: writing raw Parquet to {hdfs_raw_path}")

    # 5. Sink 2: Sliding Window Real-Time Aggregations (Console & Analytics)
    windowed_aggregates = enriched_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("city")
        ).agg(
            avg("temperature").alias("avg_temp"),
            spark_max("temperature").alias("max_temp"),
            spark_min("temperature").alias("min_temp"),
            avg("humidity").alias("avg_humidity"),
            spark_max("wind_speed").alias("max_wind"),
            count("*").alias("observation_count")
        )

    console_query = windowed_aggregates.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .start()

    print("[OK] Stream Sink 2 initialized: 5-minute sliding window aggregations active.")
    print("[*] Streaming pipeline is running. Awaiting data...")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    broker = sys.argv[1] if len(sys.argv) > 1 else os.getenv("KAFKA_BROKER", "localhost:9092")
    hdfs = sys.argv[2] if len(sys.argv) > 2 else os.getenv("HDFS_NAMENODE", "hdfs://localhost:9000")
    master = sys.argv[3] if len(sys.argv) > 3 else os.getenv("SPARK_MASTER", "local[*]")

    try:
        run_streaming_pipeline(broker, hdfs, master)
    except KeyboardInterrupt:
        print("\nStreaming terminated by user.")
