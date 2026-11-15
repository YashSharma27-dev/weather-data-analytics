"""
PySpark Weather Data Exploration & Verification
================================================
Reads weather data from Hadoop HDFS (or local filesystem fallback),
verifies schema, and displays basic distributed aggregations.

Usage:
  spark-submit spark/weather_analysis.py [HDFS_OR_LOCAL_PATH]
Example on Cluster:
  spark-submit --master spark://192.168.1.100:7077 spark/weather_analysis.py hdfs://192.168.1.100:9000/weather/data/weather_data.csv
"""

import os
import sys

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg, min as spark_min, max as spark_max, count
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

def run_analysis(data_path):
    print("=" * 60)
    print("  PySpark HDFS Weather Data Analysis")
    print(f"  Target Source: {data_path}")
    print("=" * 60)

    if not PYSPARK_AVAILABLE:
        print("[!] PySpark is not installed in the current environment.")
        print("[i] To install PySpark: pip install pyspark")
        return

    # Check local fallback if path starts with hdfs but hadoop is not running
    spark = SparkSession.builder \
        .appName("WeatherAnalyticsHDFS") \
        .master(os.getenv("SPARK_MASTER", "local[*]")) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
    except Exception as e:
        print(f"[!] Could not read from {data_path}: {e}")
        fallback = "data/weather_data.csv"
        print(f"[*] Falling back to local dataset: {fallback}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(fallback)

    print("\n====== Weather Dataset Schema from HDFS / Storage ======")
    df.printSchema()

    print("\n====== Sample Weather Records (Top 5) ======")
    df.show(5, truncate=False)

    total_records = df.count()
    print(f"\n[OK] Total Records in Dataset: {total_records}")

    print("\n====== City-wise Summary Statistics ======")
    df.groupBy("city").agg(
        count("*").alias("count"),
        avg("temperature").alias("avg_temp_c"),
        spark_min("temperature").alias("min_temp_c"),
        spark_max("temperature").alias("max_temp_c"),
        avg("humidity").alias("avg_humidity_pct"),
        avg("wind_speed").alias("avg_wind_speed_kmh")
    ).show(20, truncate=False)

    spark.stop()
    print("[OK] PySpark Analysis completed successfully.")

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else os.getenv(
        "HDFS_PATH", "hdfs://localhost:9000/weather/data/weather_data.csv"
    )
    run_analysis(target)
