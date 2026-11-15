# 🌦️ Distributed Weather Data Analytics (Big Data Semester Project)

[![Python 3.9+](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&logoColor=white)](https://python.org)
[![Hadoop 3.x](https://img.shields.io/badge/Apache%20Hadoop-3.x-66CCFF?logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)

A complete, enterprise-grade distributed Big Data analytics pipeline for real-time and historical meteorological observations. Built for semester evaluations and configured to run across a **3-computer distributed cluster** (Master + 2 Workers) as well as standalone environments.

---

## 🏗️ System Architecture

```
[Open-Meteo REST API / Simulator]
              │
              ▼
  [Kafka Streaming Producer]
              │
              ▼ (TCP :9092)
   [Apache Kafka Broker] ──── (Topic: weather-stream, 3 Partitions, 2 Replicas)
              │
              ▼
 [PySpark Structured Streaming] ────► [Hadoop HDFS Lake] (hdfs://master:9000/weather/)
  • 5-min Windowed Aggregations             │ (2x Replication across Worker 1 & Worker 2)
  • Extreme Anomaly Alert Engine            ▼
                                  [PySpark Batch & MLlib Engine]
                                    • City Statistical Profiles
                                    • Pearson Correlation Matrices
                                    • Linear Regression Temperature Predictor
                                            │
                                            ▼
                              [Interactive Streamlit Dashboard]
                                • Live Kafka Stream Gauges
                                • HDFS Analytics & Distributions
                                • Anomaly Alerts & Geo-Map
                                • ML Prediction Sandbox
                                • 3-Node Cluster Health Monitor
```

---

## 🖥️ 3-Computer Cluster Deployment Layout

| Node Role | Machine | Default IP | Daemons Running | Web Consoles |
| :--- | :--- | :--- | :--- | :--- |
| **Master Node** | Computer 1 | `192.168.1.100` | NameNode, ResourceManager, Zookeeper, Kafka, Spark Master, Streamlit UI | • NameNode: `:9870`<br>• YARN: `:8088`<br>• Spark Master: `:8080`<br>• Streamlit: `:8501` |
| **Worker Node 1** | Computer 2 | `192.168.1.101` | DataNode, NodeManager, Spark Worker 1 | • Spark Worker: `:8081` |
| **Worker Node 2** | Computer 3 | `192.168.1.102` | DataNode, NodeManager, Spark Worker 2 | • Spark Worker: `:8081` |

> 📖 **Full 3-Computer Setup Guide**: See [`docs/cluster_setup_3_nodes.md`](docs/cluster_setup_3_nodes.md) for step-by-step IP networking, `/etc/hosts`, passwordless SSH, and cluster startup.

---

## 📁 Repository Directory Structure

```text
weather-data-analytics/
├── README.md                      # Master project documentation
├── requirements.txt               # Python package dependencies
│
├── data/
│   ├── weather_data.csv           # Master historical multi-city dataset (1200+ observations)
│   ├── generate_dataset.py        # Realistic multi-city dataset generator
│   └── stream_buffer.jsonl        # Sliding buffer for live UI stream ingestion
│
├── streaming/
│   ├── weather_producer.py        # Real-time multi-city Kafka producer (API + simulation mode)
│   └── kafka_to_hdfs_stream.py    # PySpark Structured Streaming consumer writing to HDFS Parquet
│
├── spark/
│   ├── weather_analysis.py        # Basic HDFS PySpark verification script
│   ├── weather_batch_analytics.py # PySpark batch analytics (city stats, correlations, diurnal curves)
│   ├── weather_ml_model.py        # PySpark MLlib regression & classification models
│   ├── README.md                  # Spark documentation
│   └── conf/                      # Spark cluster templates (spark-env.sh, workers)
│
├── hadoop/
│   ├── hdfs_manager.py            # HDFS CLI helper (init dirs, upload datasets, health checks)
│   ├── README.md                  # Hadoop documentation
│   └── conf/                      # Production cluster XML files (core-site, hdfs-site, yarn-site, workers)
│
├── kafka/
│   ├── server.properties.template # Broker configuration for 3-node cluster
│   ├── create_topic.sh / .bat     # Topic creation utility scripts
│   ├── kafka_consumer_test.py     # Verification script to test stream delivery
│   └── README.md                  # Kafka documentation
│
├── dashboard/
│   ├── app.py                     # High-impact Streamlit Web Dashboard (5 Tabs)
│   ├── utils.py                   # Cluster socket probes and data connectors
│   └── README.md                  # UI documentation
│
├── app/
│   └── app.py                     # Streamlit entry point redirect
│
├── docker/
│   ├── docker-compose.yml         # 1-Click simulated 3-node cluster
│   └── hadoop.env                 # Container environment parameters
│
├── docs/
│   ├── architecture.md            # Detailed Big Data architecture & schemas
│   ├── cluster_setup_3_nodes.md   # Step-by-step physical 3-computer cluster deployment
│   └── viva_presentation_guide.md# Viva examination cheat sheet & Q&A
│
├── run_dashboard.bat / .sh        # 1-Click dashboard launcher
├── run_producer.bat / .sh         # 1-Click streaming producer launcher
└── run_spark_batch.bat / .sh      # 1-Click batch analytics launcher
```

---

## ⚡ Quick Start: Running the Project

### 1. Installation
Activate your virtual environment (`.venv`) and install dependencies:
```bash
pip install -r requirements.txt
```

### 2. Run Locally (Single Machine / Development Mode)
You can run all components locally even without starting distributed daemons:

1. **Start the Streaming Ingestion:**
   ```bash
   python streaming/weather_producer.py
   # Or double-click: run_producer.bat
   ```
2. **Run PySpark Batch Analytics & MLlib Model:**
   ```bash
   python spark/weather_batch_analytics.py
   python spark/weather_ml_model.py
   # Or double-click: run_spark_batch.bat
   ```
3. **Launch the Streamlit Web UI:**
   ```bash
   streamlit run dashboard/app.py
   # Or double-click: run_dashboard.bat
   ```
   Open `http://localhost:8501` in your browser.

---

## 🚀 Running on the 3-Computer Cluster (Exam Demo)

Follow this sequence across your 3 connected laptops:

### Step 1: On Master Computer (`192.168.1.100`)
```bash
# 1. Start Hadoop HDFS & YARN
start-dfs.sh && start-yarn.sh

# 2. Start Kafka & Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

# 3. Create Kafka Topic
bash kafka/create_topic.sh 192.168.1.100:9092

# 4. Start Spark Master & Workers
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh

# 5. Initialize HDFS and upload historical data
python hadoop/hdfs_manager.py init hdfs://192.168.1.100:9000
python hadoop/hdfs_manager.py upload data/weather_data.csv /weather/data/weather_data.csv

# 6. Start the Kafka Weather Producer
python streaming/weather_producer.py 192.168.1.100:9092

# 7. Submit PySpark Streaming Job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
             --master spark://192.168.1.100:7077 \
             streaming/kafka_to_hdfs_stream.py 192.168.1.100:9092 hdfs://192.168.1.100:9000

# 8. Start Streamlit UI (Accessible across the entire local network!)
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
```

---

## 🎓 What to Display on Each Screen for Examiners

1. **Computer 1 (Master)**:
   - Primary browser on **Streamlit UI** (`http://192.168.1.100:8501`), demonstrating the 5 interactive tabs:
     * **Tab 1**: Live Kafka Stream monitor with auto-updating metrics and Plotly charts.
     * **Tab 2**: PySpark HDFS analytics, temperature distribution box-plots, and correlation heatmap.
     * **Tab 3**: Extreme weather alerts (heatwaves, storms) pinpointed on a global scatter-geo map.
     * **Tab 4**: PySpark MLlib predictive sandbox — adjust sliders to predict temperature live!
     * **Tab 5**: 3-Computer cluster topology status and live socket health probes.
2. **Computer 2 (Worker 1)**:
   - Open **Hadoop NameNode Web UI** (`http://192.168.1.100:9870`).
   - Show `Live DataNodes = 2`, block replication factor of 2, and browse the HDFS filesystem at `/weather/streaming_output`.
3. **Computer 3 (Worker 2)**:
   - Open **Apache Spark Master UI** (`http://192.168.1.100:8080`).
   - Show 2 active workers, 8 CPU cores, 6 GB cluster RAM, and active running streaming applications.

---

## 💡 Troubleshooting & Fallback
- If the lab Wi-Fi has strict isolation preventing computers from talking to each other, use a **mobile phone hotspot** or spin up the instant simulated 3-node cluster using Docker:
  ```bash
  cd docker && docker-compose up -d
  ```
- Review [`docs/viva_presentation_guide.md`](docs/viva_presentation_guide.md) for expected questions from external examiners.
