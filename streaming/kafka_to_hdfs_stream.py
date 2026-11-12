from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark Session with Kafka package dependency
spark = SparkSession.builder \
    .appName("KafkaHDFSStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema matching incoming JSON weather payloads
schema = StructType([
    StructField("city", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read stream from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-stream") \
    .load()

# Parse the binary value field into JSON columns
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write streaming data continuously into Hadoop HDFS (Parquet/CSV format with checkpointing)
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/weather/streaming_output") \
    .option("checkpointLocation", "hdfs://localhost:9000/weather/checkpoint") \
    .outputMode("append") \
    .start()

print("PySpark Structured Streaming pipeline started. Writing to HDFS...")
query.awaitTermination()
