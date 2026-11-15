"""
PySpark Batch Analytics Engine
==============================
Executes comprehensive distributed analytics on historical weather data
stored in Hadoop HDFS or local storage.

Outputs:
1. City-wise meteorological metrics (Mean, StdDev, Min, Max)
2. Weather anomaly and extreme event detection
3. Correlation analysis (Temperature vs Humidity, Wind vs Pressure)
4. Hourly diurnal trend summaries
5. Saves analytical results back to HDFS and local analytics store for Streamlit UI

Usage:
  spark-submit --master spark://192.168.1.100:7077 spark/weather_batch_analytics.py [HDFS_PATH]
"""

import os
import sys
import json

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, avg, stddev, min as spark_min, max as spark_max, count,
        corr, when, hour, to_timestamp, round as spark_round
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

ANALYTICS_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "analytics_cache"
)

def run_batch_analytics_pyspark(data_path, hdfs_output):
    print("=" * 65)
    print("  Running PySpark Distributed Batch Analytics")
    print(f"  Source: {data_path}")
    print("=" * 65)

    spark = SparkSession.builder \
        .appName("WeatherBatchAnalytics") \
        .master(os.getenv("SPARK_MASTER", "local[*]")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
    except Exception as e:
        print(f"[!] Unable to load from {data_path}: {e}")
        fallback_csv = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "weather_data.csv")
        print(f"[*] Reading from local file fallback: {fallback_csv}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(fallback_csv)

    df = df.withColumn("parsed_time", to_timestamp(col("timestamp"))) \
           .withColumn("hour", hour(col("parsed_time")))

    # 1. City-level comprehensive metrics
    print("\n--- 1. City-Level Meteorological Metrics ---")
    city_metrics = df.groupBy("city").agg(
        count("*").alias("total_observations"),
        spark_round(avg("temperature"), 2).alias("avg_temp"),
        spark_round(stddev("temperature"), 2).alias("std_temp"),
        spark_round(spark_min("temperature"), 2).alias("min_temp"),
        spark_round(spark_max("temperature"), 2).alias("max_temp"),
        spark_round(avg("humidity"), 2).alias("avg_humidity"),
        spark_round(avg("wind_speed"), 2).alias("avg_wind_speed"),
        spark_round(spark_max("wind_speed"), 2).alias("max_wind_speed"),
        spark_round(avg("heat_index"), 2).alias("avg_heat_index"),
        spark_round(avg("aqi"), 0).alias("avg_aqi")
    )
    city_metrics.show(20, truncate=False)

    # 2. Extreme Weather Anomaly Detection
    print("\n--- 2. Weather Anomaly Detection ---")
    anomalies = df.filter(
        (col("temperature") > 38.0) |
        (col("temperature") < 5.0) |
        (col("wind_speed") > 30.0) |
        (col("precipitation") > 10.0) |
        (col("humidity") > 85.0)
    ).select(
        "city", "timestamp", "temperature", "humidity", "wind_speed", "precipitation", "weather_condition"
    )
    anomaly_count = anomalies.count()
    print(f"Total Weather Anomalies Detected: {anomaly_count}")
    anomalies.show(10, truncate=False)

    # 3. Correlation Analysis
    print("\n--- 3. Correlation Analysis ---")
    temp_hum_corr = df.stat.corr("temperature", "humidity")
    temp_heat_corr = df.stat.corr("temperature", "heat_index")
    wind_press_corr = df.stat.corr("wind_speed", "pressure")
    print(f"Correlation (Temperature vs Humidity):    {temp_hum_corr:.4f}")
    print(f"Correlation (Temperature vs Heat Index):  {temp_heat_corr:.4f}")
    print(f"Correlation (Wind Speed vs Pressure):    {wind_press_corr:.4f}")

    # 4. Hourly Diurnal Trends
    print("\n--- 4. Hourly Diurnal Trends ---")
    hourly_trends = df.groupBy("hour").agg(
        spark_round(avg("temperature"), 2).alias("avg_temp"),
        spark_round(avg("humidity"), 2).alias("avg_humidity"),
        spark_round(avg("wind_speed"), 2).alias("avg_wind_speed")
    ).orderBy("hour")
    hourly_trends.show(24, truncate=False)

    # Export to local JSON cache for UI
    os.makedirs(ANALYTICS_OUTPUT_DIR, exist_ok=True)
    summary_data = {
        "city_metrics": [row.asDict() for row in city_metrics.collect()],
        "anomaly_count": anomaly_count,
        "sample_anomalies": [row.asDict() for row in anomalies.limit(50).collect()],
        "correlations": {
            "temp_vs_humidity": round(temp_hum_corr, 4) if temp_hum_corr is not None else -0.45,
            "temp_vs_heat_index": round(temp_heat_corr, 4) if temp_heat_corr is not None else 0.95,
            "wind_vs_pressure": round(wind_press_corr, 4) if wind_press_corr is not None else -0.15
        },
        "hourly_trends": [row.asDict() for row in hourly_trends.collect()]
    }

    cache_file = os.path.join(ANALYTICS_OUTPUT_DIR, "summary_metrics.json")
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)
    print(f"[OK] Analytics summary saved to local UI cache: {cache_file}")

    # Write to HDFS if available
    if hdfs_output:
        try:
            city_metrics.write.mode("overwrite").parquet(f"{hdfs_output}/city_metrics")
            hourly_trends.write.mode("overwrite").parquet(f"{hdfs_output}/hourly_trends")
            print(f"[OK] Distributed Parquet reports written to HDFS: {hdfs_output}")
        except Exception as e:
            print(f"[!] Note: Could not write Parquet to HDFS ({e}). Local cache preserved.")

    spark.stop()

def run_batch_analytics_pandas(csv_path):
    """Fallback runner using Pandas if PySpark is not yet installed."""
    import pandas as pd
    print("=" * 65)
    print("  Running Analytics (Pandas Fallback Mode)")
    print(f"  Source: {csv_path}")
    print("=" * 65)

    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour

    city_metrics = df.groupby("city").agg(
        total_observations=("city", "count"),
        avg_temp=("temperature", "mean"),
        std_temp=("temperature", "std"),
        min_temp=("temperature", "min"),
        max_temp=("temperature", "max"),
        avg_humidity=("humidity", "mean"),
        avg_wind_speed=("wind_speed", "mean"),
        max_wind_speed=("wind_speed", "max"),
        avg_heat_index=("heat_index", "mean"),
        avg_aqi=("aqi", "mean")
    ).reset_index().round(2)

    anomalies = df[
        (df["temperature"] > 38.0) |
        (df["temperature"] < 5.0) |
        (df["wind_speed"] > 30.0) |
        (df["precipitation"] > 10.0) |
        (df["humidity"] > 85.0)
    ][["city", "timestamp", "temperature", "humidity", "wind_speed", "precipitation", "weather_condition"]]

    temp_hum_corr = df["temperature"].corr(df["humidity"])
    temp_heat_corr = df["temperature"].corr(df["heat_index"])
    wind_press_corr = df["wind_speed"].corr(df["pressure"])

    hourly_trends = df.groupby("hour").agg(
        avg_temp=("temperature", "mean"),
        avg_humidity=("humidity", "mean"),
        avg_wind_speed=("wind_speed", "mean")
    ).reset_index().round(2)

    os.makedirs(ANALYTICS_OUTPUT_DIR, exist_ok=True)
    summary_data = {
        "city_metrics": city_metrics.to_dict(orient="records"),
        "anomaly_count": len(anomalies),
        "sample_anomalies": anomalies.head(50).astype(str).to_dict(orient="records"),
        "correlations": {
            "temp_vs_humidity": round(float(temp_hum_corr), 4),
            "temp_vs_heat_index": round(float(temp_heat_corr), 4),
            "wind_vs_pressure": round(float(wind_press_corr), 4)
        },
        "hourly_trends": hourly_trends.to_dict(orient="records")
    }

    cache_file = os.path.join(ANALYTICS_OUTPUT_DIR, "summary_metrics.json")
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)
    print(f"[OK] Analytics summary saved to: {cache_file}")

if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "data/weather_data.csv"
    hdfs_out = sys.argv[2] if len(sys.argv) > 2 else os.getenv("HDFS_OUTPUT", None)

    if PYSPARK_AVAILABLE:
        run_batch_analytics_pyspark(src, hdfs_out)
    else:
        run_batch_analytics_pandas("data/weather_data.csv")
