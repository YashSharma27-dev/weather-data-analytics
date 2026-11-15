# Big Data Project Viva & Presentation Q&A Guide

Prepare for your semester viva, external examination, and lab demo with these frequently asked questions and high-scoring answers.

---

### Q1: What is the end-to-end data pipeline flow of this project?
**Answer:**
1. **Ingestion**: Real-time multi-city weather data is ingested from the Open-Meteo REST API by a Python producer and published as JSON messages into Apache Kafka topic `weather-stream`.
2. **Buffering & Decoupling**: Kafka buffers high-throughput streaming events across 3 partitions with a replication factor of 2.
3. **Stream Processing**: PySpark Structured Streaming consumes from Kafka, enforces a structured schema, performs 5-minute sliding window aggregations, detects weather anomalies, and persists partitioned Parquet files into Hadoop HDFS (`hdfs://master:9000/weather/streaming_output`).
4. **Batch Analytics & MLlib**: PySpark batch jobs process the historical lake data on HDFS to calculate correlation matrices, diurnal curves, and train Linear Regression / Random Forest models for temperature forecasting.
5. **Presentation**: A Streamlit UI displays live streaming gauges, historical analytical graphs, interactive ML predictions, and cluster topology monitoring.

---

### Q2: Why did you choose a 3-computer cluster? What is the role of each node?
**Answer:**
A 3-node cluster demonstrates true distributed storage and parallel computing:
- **Node 1 (Master)**: Coordinates the cluster. Runs the Hadoop NameNode (metadata), YARN ResourceManager (job scheduling), Spark Master (task coordinator), and Kafka Broker.
- **Node 2 (Worker 1)** & **Node 3 (Worker 2)**: Execute the actual distributed work. Each runs a Hadoop DataNode (stores HDFS data blocks) and a Spark Worker (runs Spark executors with assigned CPU cores and RAM).

---

### Q3: Why is HDFS replication factor set to 2 instead of 3?
**Answer:**
In our 3-computer setup, there are 2 worker nodes hosting DataNodes. A replication factor of 2 stores exactly one copy on Worker 1 and one copy on Worker 2. If one of the worker machines fails or is disconnected, HDFS continues serving data from the other replica without data loss, providing 100% fault tolerance.

---

### Q4: Why use Apache Kafka instead of sending data directly to PySpark or HDFS?
**Answer:**
1. **Decoupling**: Decouples the ingestion rate of weather sensors/APIs from the processing rate of PySpark.
2. **Backpressure Handling**: If PySpark experiences high cluster load, Kafka safely buffers messages in distributed disk partitions.
3. **Multi-Consumer Support**: Multiple applications (PySpark Streaming, archival consumers, live alert bots) can independently consume the same weather topic at their own pace without burdening the data source.

---

### Q5: What is the difference between Spark Structured Streaming and legacy DStreams?
**Answer:**
1. **API Abstraction**: Structured Streaming uses the modern DataFrame/Dataset API, whereas DStreams used low-level RDDs.
2. **Event-Time Processing**: Structured Streaming natively supports event-time processing and watermarking (handling delayed network records based on timestamp of collection rather than arrival time).
3. **Execution Engine**: Structured Streaming utilizes Spark SQL's Catalyst Optimizer and Tungsten engine for superior speed and memory efficiency.

---

### Q6: How does the PySpark MLlib model work in this project?
**Answer:**
We trained a multivariate regression pipeline in PySpark MLlib:
- **Features**: Relative Humidity, Atmospheric Pressure, Wind Speed, and Hour of Day.
- **Transformations**: `VectorAssembler` transforms feature columns into dense vectors, and `StandardScaler` standardizes the features to zero mean and unit variance.
- **Evaluator**: Evaluated using `RegressionEvaluator` on an 80/20 train-test split, outputting Root Mean Squared Error (RMSE), Mean Absolute Error (MAE), and $R^2$ Score.
- **Interactive UI**: The trained coefficients are exposed to Streamlit sliders so examiners can test real-time inference on demand.

---

### Q7: What happens if one Worker Computer is shut down during the demonstration?
**Answer:**
1. **HDFS Fault Tolerance**: Since `dfs.replication=2`, every block exists on the remaining DataNode. The NameNode detects the missing heartbeat within 10 minutes and flags the node as dead, but all data remains fully readable.
2. **Spark Fault Tolerance**: Spark Master detects lost executor heartbeats and reschedules failed tasks onto the remaining active worker without terminating the overall streaming application.
