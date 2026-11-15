# 🌦️ Distributed Weather Data Analytics Pipeline
### Big Data Semester Project Report

---

## 👥 Group Members & Project Details

| S.No | Student Name | Roll Number | Primary Contribution |
| :---: | :--- | :--- | :--- |
| 1 | **Yash Sharma** | 231210127 | Hadoop Cluster, HDFS Architecture & Kafka Ingestion |
| 2 | **Tanmay bharadwaj** | 231210114 | Streamlit UI Dashboard & Visualization Engine |
| 3 | **Sunny Kumar** | 231210107 | PySpark Distributed Batch Processing & Aggregations |
| 4 | **Vedant dubey** | 231210119 | PySpark MLlib Predictive Modeling & Data Collection |

- **Course:** Big Data Analytics / Distributed Systems
- **Evaluator:** Kritika Gahlawat
- **Repository URL:** https://github.com/YashSharma27-dev/weather-data-analytics

---

## 1. Abstract & Executive Summary

Modern meteorological networks produce high-velocity, high-volume sensor observations that require distributed ingestion and processing systems. This project presents an end-to-end distributed Big Data pipeline for real-time and historical weather data analysis. 

The system leverages:
1. **Apache Kafka** for decoupled, fault-tolerant message streaming across multiple worldwide and domestic cities.
2. **PySpark Structured Streaming** for windowed stream transformations, watermark-driven event-time processing, and extreme weather anomaly detection.
3. **Apache Hadoop (HDFS)** as a distributed, fault-tolerant data lake with configurable block replication.
4. **PySpark Batch & MLlib** for distributed historical aggregations, correlation matrix computation, and multivariate regression modeling for temperature forecasting.
5. **Streamlit & Plotly** for a real-time web dashboard displaying live streaming telemetry, interactive maps, and predictive sandboxes.

The project is designed to run both on a single standalone machine and across a physical **multi-computer distributed cluster** (Master and Worker nodes).

---

## 2. Problem Statement & Objectives

### Problem Statement
Traditional monolithic database systems struggle with concurrent, high-frequency time-series weather updates and complex historical aggregations. There is a need for a distributed framework capable of handling streaming ingestion, persistent block-level storage, and parallel machine learning over large atmospheric datasets.

### Objectives
- Ingest live meteorological parameters (temperature, humidity, wind speed, pressure, precipitation, and air quality) from the Open-Meteo REST API.
- Buffer streaming data reliably using an Apache Kafka broker with multi-partition topic architecture.
- Persist structured records into Hadoop Distributed File System (HDFS) with fault tolerance.
- Execute PySpark distributed transformations to compute city-level metrics, diurnal cycles, and anomaly flags.
- Train a machine learning model using PySpark MLlib to predict temperature based on atmospheric features.
- Develop an interactive web interface for real-time monitoring and analytical exploration.

---

## 3. System Architecture & Tech Stack
[Open-Meteo REST API / Telemetry Source] │ ▼ [Kafka Weather Producer] (streaming/weather_producer.py) │ ▼ (TCP Port :9092) [Apache Kafka Broker / Zookeeper] (Topic: weather-stream) │ ▼ [PySpark Structured Streaming] (streaming/kafka_to_hdfs_stream.py) ├── JSON Schema Validation & Event-Time Watermarking ├── 5-Minute Sliding Window Aggregations ├── Extreme Anomaly Alerts (Heatwaves, Storms) │ ├───► [Hadoop HDFS Lake] (hdfs://master:9000/weather/) │ └───► [PySpark Batch & MLlib Analytics Engine] (spark/) ├── City-level Descriptive Statistics (Mean, Min, Max, StdDev) ├── Meteorological Pearson Correlation Matrix └── Linear Regression & Random Forest Forecasting Model │ ▼ [Interactive Streamlit Dashboard] (dashboard/app.py)


### Technology Stack
- **Ingestion & Messaging:** Apache Kafka 3.x, Zookeeper
- **Distributed Processing:** Apache Spark 3.5 / PySpark (Structured Streaming, Spark SQL, MLlib)
- **Distributed Storage:** Apache Hadoop 3.x (HDFS, YARN)
- **Frontend & Visualization:** Streamlit, Plotly Express, Plotly Graph Objects
- **Language & Runtime:** Python 3.9+, OpenJDK 11 / 17

---

## 4. Cluster Deployment Architecture

| Node Role | Machine | Default IP | Daemons / Services |
| :--- | :--- | :--- | :--- |
| **Master Node** | Laptop 1 | `192.168.1.100` | NameNode, ResourceManager, Zookeeper, Kafka Broker, Spark Master, Streamlit UI |
| **Worker Node** | Laptop 2 | `192.168.1.101` | DataNode, NodeManager, Spark Worker |

- **HDFS Replication:** Configured to `1` (for 1 worker) or `2` (for 2 workers) ensuring zero data loss upon single-node failure.
- **Spark Resource Allocation:** Dynamically allocated executor cores and memory across active cluster workers.

---

## 5. Implementation Details

### 5.1 Real-Time Ingestion (Kafka)
The script `streaming/weather_producer.py` collects live weather updates for major metropolitan cities (Delhi, Mumbai, Bengaluru, Kolkata, Chennai, New York, London, Tokyo) every 2 seconds and publishes JSON messages to topic `weather-stream`.

### 5.2 Distributed Storage (Hadoop HDFS)
Raw and streaming datasets are stored in HDFS under `/weather/`:
- `/weather/data/weather_data.csv`: Master historical lake dataset.
- `/weather/streaming_output/`: Partitioned Parquet storage generated by PySpark streaming.
- `/weather/checkpoint/`: Fault-recovery checkpointing for streaming state recovery.

### 5.3 Distributed Analytics & Machine Learning (PySpark)
1. **Batch Analytics (`spark/weather_batch_analytics.py`):**
   - Distributed aggregations compute city-wise Mean, Standard Deviation, Min, Max, Heat Index, and AQI.
   - Pearson correlation coefficients determine atmospheric dependencies (Temperature vs. Humidity, Wind vs. Pressure).
   - Diurnal temperature curves map the 24-hour meteorological variations.
2. **Machine Learning Model (`spark/weather_ml_model.py`):**
   - **Features:** `humidity`, `wind_speed`, `pressure`, `hour`.
   - **Pipeline:** `VectorAssembler` + `StandardScaler` + `LinearRegression`.
   - **Performance:** Evaluated on test split producing RMSE, MAE, and $R^2$ scores.

### 5.4 Web User Interface (Streamlit)
The dashboard (`dashboard/app.py`) provides 5 dedicated tabs:
1. **Real-Time Kafka Stream Monitor:** Live auto-updating telemetry table, temperature line graphs, and KPI metric dials.
2. **HDFS & PySpark Analytics:** Historical comparative bar charts, quartile box-plots, and correlation heatmaps.
3. **Extreme Weather Alerts:** Automated anomaly flags (Heatwave > 38°C, Storm > 25 km/h, Extreme Humidity > 85%) plotted on a geospatial map.
4. **PySpark ML Weather Predictor:** Interactive sliders for real-time temperature and comfort index inference.
5. **Cluster Topology Monitor:** Real-time port and daemon connectivity probe.

---

## 6. Project Images : 
<img width="1917" height="920" alt="image" src="https://github.com/user-attachments/assets/c029e192-7824-4adc-9bf0-0a99afebcecb" />
<img width="1522" height="642" alt="image" src="https://github.com/user-attachments/assets/53009fc4-6294-46ab-9b60-13e54ccb5db2" />
<img width="1475" height="667" alt="image" src="https://github.com/user-attachments/assets/bce6eefe-83b8-475c-9960-098367989349" />
<img width="1468" height="677" alt="image" src="https://github.com/user-attachments/assets/05a35074-436b-496f-b498-91376ba73e32" />
<img width="1625" height="736" alt="image" src="https://github.com/user-attachments/assets/3fa7124e-d2cc-4e6f-a0e7-c75c79bf30a6" />
<img width="1580" height="740" alt="image" src="https://github.com/user-attachments/assets/c67b2fb4-9034-4284-bd6a-2f9e09676109" />
<img width="1596" height="587" alt="image" src="https://github.com/user-attachments/assets/98cf5451-147c-4143-87b6-caca1850cb5e" />





## 7. Results & Key Findings
Correlation: An inverse relationship is observed between daytime temperature and relative humidity, while temperature and heat index exhibit a positive correlation (
𝑟
≈
0.98
r≈0.98).
Streaming Throughput: Apache Kafka safely buffered streaming data at sub-second latency with zero dropped messages.
Predictive Accuracy: The PySpark regression pipeline effectively modeled diurnal temperature patterns based on atmospheric pressure and relative humidity.

## 8. Conclusion

This project successfully demonstrates an enterprise-grade Big Data pipeline integrating Apache Kafka, Apache Hadoop HDFS, Apache Spark, and Streamlit. The architecture proves that distributed streaming and analytical processing can provide scalable, fault-tolerant meteorological monitoring and forecasting.

