# Apache Kafka Configuration (3-Computer Cluster)

This directory contains configuration templates and execution scripts for setting up Apache Kafka and Zookeeper across the 3-computer cluster.

---

## 1. Setup Architecture

- **Zookeeper**: Runs on **Master PC** (`master:2181`)
- **Kafka Broker**: Runs on **Master PC** (`master:9092`), with optional extra brokers on Worker1 (`worker1:9093`) and Worker2 (`worker2:9094`) for a full 3-broker cluster.
- **Topic**: `weather-stream` (Partitions: 3, Replication Factor: 2)

---

## 2. Starting Zookeeper and Kafka (on Master PC)

### Step A: Start Zookeeper
```bash
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```

### Step B: Start Kafka Broker
```bash
kafka-server-start.sh kafka/server.properties.template
```
*(Or place `server.properties.template` into `$KAFKA_HOME/config/server.properties` and start with standard command)*

---

## 3. Creating the Weather Topic

Run the helper script:
```bash
# On Linux / Cluster:
bash kafka/create_topic.sh master:9092

# On Windows:
kafka\create_topic.bat localhost:9092
```

---

## 4. Verifying Kafka Stream

1. **Start the Producer** (publishes weather events every 2 seconds):
   ```bash
   python streaming/weather_producer.py master:9092
   ```

2. **Verify with the Consumer** (in a separate terminal):
   ```bash
   python kafka/kafka_consumer_test.py master:9092
   ```
