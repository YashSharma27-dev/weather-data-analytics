# Apache Spark / PySpark Configuration & Jobs

This directory contains the PySpark distributed processing scripts and cluster configurations for the 3-computer cluster.

---

## 1. Cluster Roles

- **Master PC (`master` / `192.168.1.100`)**: Spark Master (`spark://master:7077`, Web UI: `:8080`)
- **Worker 1 (`worker1` / `192.168.1.101`)**: Spark Worker (`:8081`)
- **Worker 2 (`worker2` / `192.168.1.102`)**: Spark Worker (`:8081`)

---

## 2. Starting Spark on the 3-Computer Cluster

### On Master PC:
```bash
# 1. Start Spark Master
$SPARK_HOME/sbin/start-master.sh

# 2. Start all workers automatically across worker1 and worker2 via SSH:
$SPARK_HOME/sbin/start-workers.sh
```

### Alternatively, Start Workers Manually:
- On **Worker 1**:
  ```bash
  $SPARK_HOME/sbin/start-worker.sh spark://master:7077
  ```
- On **Worker 2**:
  ```bash
  $SPARK_HOME/sbin/start-worker.sh spark://master:7077
  ```

Check the Spark Web UI at `http://master:8080` to verify both workers are listed with active cores and memory.

---

## 3. Running PySpark Jobs

### A. Quick Dataset Verification & Exploration
```bash
spark-submit --master spark://master:7077 spark/weather_analysis.py hdfs://master:9000/weather/data/weather_data.csv
```

### B. Comprehensive Batch Analytics Engine
Computes multi-city metrics, correlations, hourly trends, and anomaly detection:
```bash
spark-submit --master spark://master:7077 spark/weather_batch_analytics.py hdfs://master:9000/weather/data/weather_data.csv hdfs://master:9000/weather/analytics
```

### C. MLlib Weather Prediction Model
Trains regression & classification models on historical observations:
```bash
spark-submit --master spark://master:7077 spark/weather_ml_model.py
```

### D. Real-Time Structured Streaming (Kafka -> HDFS)
Streams live weather data from Kafka topic into HDFS Parquet tables:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
             --master spark://master:7077 \
             streaming/kafka_to_hdfs_stream.py master:9092 hdfs://master:9000
```
