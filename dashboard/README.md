# Streamlit Weather Analytics UI

This directory contains the interactive web dashboard for the Weather Data Analytics Big Data project.

---

## Features
- **Real-Time Kafka Stream Monitor**: Live KPI metrics, streaming charts, auto-refresh, and live message log.
- **Hadoop HDFS & PySpark Analytics**: Aggregated historical data from HDFS, city-wise comparisons, distributions, and correlation matrices.
- **Weather Anomaly & Alert Engine**: Extreme event detection (heatwaves, storms, high humidity, flash rains) with geographic mapping.
- **PySpark MLlib Predictive Sandbox**: Real-time slider inputs to forecast temperature and classify weather conditions.
- **3-Computer Cluster Topology View**: Interactive visual status of Master (Node 1) and Workers (Nodes 2 & 3), port health checker, and quick-links.

---

## How to Run

### Local Standalone Mode:
```bash
streamlit run dashboard/app.py
```

### Cluster Master Mode (Exposing to all 3 computers on LAN):
```bash
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
```
Then access from any computer on the local network at:
`http://192.168.1.100:8501`
