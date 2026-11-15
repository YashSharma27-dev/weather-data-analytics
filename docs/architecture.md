# Weather Data Analytics: End-to-End Big Data Architecture

This document describes the complete architecture, pipeline flow, distributed design, and network port mapping for the **Distributed Weather Data Analytics** semester project.

---

## 1. High-Level Architectural Flow

```
[Open-Meteo REST API / IoT Simulator]
                  │
                  ▼
      [Kafka Producer (Python)]
                  │
                  ▼  (JSON Over TCP Port 9092)
     [Apache Kafka Broker / Zookeeper]
                  │  (Topic: weather-stream, 3 Partitions, 2 Replicas)
                  ▼
    [PySpark Structured Streaming Engine]
      ├── 1. JSON Schema Validation & Watermarking
      ├── 2. Sliding Window Aggregations (5-Min Tumbling / 1-Min Slide)
      ├── 3. Extreme Weather Alert Engine (Heatwaves, Storms)
      │
      ├───► [Hadoop Distributed File System (HDFS)]
      │        (Path: /weather/streaming_output - Parquet, 2x Replication)
      │
      └───► [Analytics & Real-Time Lake Cache]
                  ▲
                  │  (Queries historical Parquet/CSV)
      [PySpark Batch Processing & MLlib]
      ├── City-level descriptive statistics
      ├── Pearson correlation matrix
      └── Linear Regression & Random Forest temperature predictor
                  │
                  ▼
       [Streamlit Web UI Dashboard]
      ├── Live Kafka Stream Gauges & Line Charts
      ├── HDFS Distributed Analytics Explorer
      ├── Anomaly & Extreme Event Geo-Map
      ├── Interactive PySpark ML Forecast Sandbox
      └── 3-Computer Cluster Topology & Socket Probe
```

---

## 2. 3-Computer Physical Cluster Topology

| Computer | Role | Hostname | IP Address | Distributed Daemons |
| :--- | :--- | :--- | :--- | :--- |
| **PC 1** | **Master** | `master` | `192.168.1.100` | • Hadoop NameNode<br>• YARN ResourceManager<br>• Zookeeper (`:2181`)<br>• Kafka Broker (`:9092`)<br>• Spark Master (`:7077`)<br>• Streamlit Dashboard (`:8501`) |
| **PC 2** | **Worker 1** | `worker1` | `192.168.1.101` | • Hadoop DataNode<br>• YARN NodeManager<br>• Spark Worker 1 |
| **PC 3** | **Worker 2** | `worker2` | `192.168.1.102` | • Hadoop DataNode<br>• YARN NodeManager<br>• Spark Worker 2 |

---

## 3. Data Ingestion & Streaming Layer (Kafka)

- **Source**: Real-time REST endpoints from Open-Meteo API (or multi-city simulation fallback).
- **Cities Monitored**: Delhi, Mumbai, Bengaluru, Kolkata, Chennai, New York, London, Tokyo.
- **Kafka Topic**: `weather-stream`
- **Partitions**: 3 (distributed across cluster brokers)
- **Replication Factor**: 2 (tolerates 1 node failure)
- **Message Payload Schema**:
  ```json
  {
    "city": "Delhi",
    "latitude": 28.6139,
    "longitude": 77.2090,
    "timestamp": "2026-11-15 15:00:00",
    "temperature": 27.5,
    "humidity": 55.2,
    "wind_speed": 12.8,
    "wind_direction": 302,
    "pressure": 1012.3,
    "precipitation": 0.0,
    "weather_condition": "Partly Cloudy",
    "heat_index": 27.96,
    "aqi": 185,
    "collected_at": "2026-11-15T15:00:00.123456"
  }
  ```

---

## 4. Distributed Storage Layer (Hadoop HDFS)

- **File System URI**: `hdfs://master:9000`
- **Default Replication**: `dfs.replication = 2`
- **Block Size**: 128 MB
- **Storage Directory Tree**:
  - `/weather/data/weather_data.csv`: Master raw historical dataset.
  - `/weather/streaming_output/`: Partitioned Parquet files written by PySpark Structured Streaming.
  - `/weather/checkpoint/`: Spark Streaming fault-recovery state store.
  - `/weather/analytics/`: Pre-aggregated analytical Parquet datasets.

---

## 5. Distributed Processing Layer (Apache Spark / PySpark)

1. **Structured Streaming Engine (`streaming/kafka_to_hdfs_stream.py`)**:
   - Subscribed to `weather-stream`.
   - Sliding window aggregations with watermark (`event_time`, 10 minutes) to handle late-arriving network data.
   - Outputs partitioned Parquet records to HDFS and console logs.
2. **Batch Analytics Engine (`spark/weather_batch_analytics.py`)**:
   - Computes City-level Mean, StdDev, Min, Max for all metrics.
   - Computes 24-hour diurnal temperature cycles.
   - Generates Pearson correlation coefficients across atmospheric variables.
3. **Machine Learning Pipeline (`spark/weather_ml_model.py`)**:
   - Features: `humidity`, `wind_speed`, `pressure`, `hour`.
   - Feature Scaling: `VectorAssembler` + `StandardScaler`.
   - Model: Multivariate Linear Regression + Random Forest Regressor.
   - Evaluated on test split: RMSE, MAE, and $R^2$ Score.

---

## 6. Web Dashboard Layer (Streamlit)

- Built with Streamlit and Plotly Express / Graph Objects.
- Responsive, dark-themed interface with 5 dedicated functional tabs.
- Auto-refresh mechanism polling live Kafka buffer and HDFS cache.
- Embedded TCP socket health checker verifying port connectivity across all 3 computers.

---

## 7. Cluster Network Port Reference

| Component | Default Port | Protocol | Purpose |
| :--- | :--- | :--- | :--- |
| **HDFS NameNode** | `9000` | IPC / RPC | File system operations |
| **HDFS NameNode Web UI** | `9870` | HTTP | Cluster browser & data node health |
| **HDFS DataNode Web UI** | `9864` | HTTP | DataNode block inspection |
| **YARN ResourceManager** | `8032` / `8088` | IPC / HTTP | Job scheduling & resource dashboard |
| **Apache Kafka Broker** | `9092` | TCP | Producer and Consumer streaming |
| **Apache Zookeeper** | `2181` | TCP | Cluster metadata and leader election |
| **Spark Master** | `7077` | RPC | Spark standalone worker registration |
| **Spark Master Web UI** | `8080` | HTTP | Active Spark workers & job execution UI |
| **Spark Worker Web UI** | `8081` | HTTP | Worker executors & thread logs |
| **Streamlit Dashboard** | `8501` | HTTP | Web user interface |
