# 3-Node Cluster Simulation via Docker Compose

This Docker Compose environment simulates the complete 3-computer Big Data architecture (Master + Worker 1 + Worker 2 + Kafka + Spark + Streamlit UI) in an isolated container network.

---

## What It Spins Up

1. **Master Node**:
   - Hadoop NameNode (`hdfs://master:9000`, Web UI: `http://localhost:9870`)
   - YARN ResourceManager (`http://localhost:8088`)
   - Zookeeper (`2181`) & Kafka Broker (`localhost:9092`)
   - Spark Master (`spark://spark-master:7077`, Web UI: `http://localhost:8080`)
2. **Worker Node 1**:
   - Hadoop DataNode 1
   - Spark Worker 1
3. **Worker Node 2**:
   - Hadoop DataNode 2
   - Spark Worker 2
4. **Streamlit Dashboard**:
   - Web UI (`http://localhost:8501`)

---

## Quick Start

```bash
cd docker
docker-compose up -d
```

Verify running containers:
```bash
docker-compose ps
```

Stop the cluster:
```bash
docker-compose down
```
