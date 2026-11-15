# 🌦️ Distributed Weather Data Analytics 

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




## 💡 Troubleshooting & Fallback
- If the lab Wi-Fi has strict isolation preventing computers from talking to each other, use a **mobile phone hotspot** or spin up the instant simulated 3-node cluster using Docker:
  ```bash
  cd docker && docker-compose up -d
  ```
- Review [`docs/viva_presentation_guide.md`](docs/viva_presentation_guide.md) for expected questions from external examiners.
