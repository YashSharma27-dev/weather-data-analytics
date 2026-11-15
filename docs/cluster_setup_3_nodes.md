# Complete Guide: Deploying and Running on a 3-Computer Cluster

This is the definitive, step-by-step manual to set up, configure, and demonstrate the **Weather Data Analytics Big Data Project** across **3 physical computers or laptops** (or 3 Virtual Machines / WSL2 instances) for your semester evaluation.

---

## 1. Cluster Roles & Topology

| Computer | Physical Role | Hostname | Example IP | Services Running |
| :--- | :--- | :--- | :--- | :--- |
| **Computer 1** | **Master Node** | `master` | `192.168.1.100` | • NameNode<br>• ResourceManager<br>• Zookeeper & Kafka Broker<br>• Spark Master<br>• Streamlit Dashboard |
| **Computer 2** | **Worker Node 1** | `worker1` | `192.168.1.101` | • DataNode<br>• NodeManager<br>• Spark Worker 1 |
| **Computer 3** | **Worker 2** | `worker2` | `192.168.1.102` | • DataNode<br>• NodeManager<br>• Spark Worker 2 |

> [!TIP]
> Connect all 3 computers to the **same Wi-Fi network, mobile hotspot, or Ethernet switch**.

---

## 2. Step-by-Step Prerequisites on ALL 3 Computers

### Step 2.1: Find Each Computer's IP Address
- **On Windows (PowerShell/CMD):**
  ```cmd
  ipconfig
  ```
  Look for `IPv4 Address` under your active Wi-Fi or Ethernet adapter (e.g., `192.168.1.100`).
- **On Linux / Ubuntu / Mac:**
  ```bash
  hostname -I
  ```

### Step 2.2: Configure the `hosts` File on ALL 3 Computers
Map the hostnames to the IP addresses so the machines can resolve each other by name.

- **On Linux / Ubuntu / WSL (`/etc/hosts`):**
  ```bash
  sudo nano /etc/hosts
  ```
- **On Windows (`C:\Windows\System32\drivers\etc\hosts` as Administrator):**
  Add the following lines (replace with your actual 3 IPs):
  ```text
  192.168.1.100 master
  192.168.1.101 worker1
  192.168.1.102 worker2
  ```

Verify connectivity by pinging from Computer 1:
```bash
ping worker1
ping worker2
```

### Step 2.3: Java JDK 8 or 11 & Python Installation
Hadoop, Spark, and Kafka require Java.
- **Install Java:**
  - Ubuntu/Debian: `sudo apt update && sudo apt install openjdk-11-jdk -y`
  - Windows: Install Oracle JDK or Eclipse Temurin OpenJDK 11.
- **Install Python 3.9+ & Libraries:**
  ```bash
  pip install -r requirements.txt
  ```

### Step 2.4: Set Environment Variables
Add to `~/.bashrc` (Linux) or Windows System Environment Variables:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$KAFKA_HOME/bin
```

---

## 3. Passwordless SSH Setup (Master to Workers)

Hadoop and Spark on the Master computer need passwordless SSH access to `worker1` and `worker2` to start distributed daemons simultaneously.

1. On **Master Computer (Computer 1)**, generate an RSA key pair:
   ```bash
   ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
   ```
2. Authorize the key locally on Master:
   ```bash
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   ```
3. Copy the public key to Worker 1 and Worker 2:
   ```bash
   ssh-copy-id hadoop@worker1
   ssh-copy-id hadoop@worker2
   ```
4. Test logging in without a password prompt:
   ```bash
   ssh worker1
   exit
   ssh worker2
   exit
   ```

---

## 4. Hadoop (HDFS & YARN) Setup for 3 Nodes

### Step 4.1: Deploy XML Configurations
We have pre-configured all cluster XML files inside `hadoop/conf/`.
Copy them to your Hadoop installation on **all 3 computers**:
```bash
cp hadoop/conf/* $HADOOP_HOME/etc/hadoop/
```

### Step 4.2: Format the NameNode (Master PC Only — First Time Only!)
On **Computer 1 (Master)**:
```bash
hdfs namenode -format
```
*(Look for: `Storage directory ... has been successfully formatted`)*

### Step 4.3: Start HDFS and YARN Daemons
On **Computer 1 (Master)**:
```bash
start-dfs.sh
start-yarn.sh
```

### Step 4.4: Verify Daemons with `jps`
- On **Master (Computer 1)**, run `jps`:
  ```
  NameNode
  SecondaryNameNode
  ResourceManager
  Jps
  ```
- On **Worker 1 (Computer 2)** and **Worker 2 (Computer 3)**, run `jps`:
  ```
  DataNode
  NodeManager
  Jps
  ```

---

## 5. Apache Kafka Setup

### Step 5.1: Start Zookeeper & Kafka Broker
On **Computer 1 (Master)**:
```bash
# Terminal 1: Start Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

# Terminal 2: Start Kafka Broker
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
```

### Step 5.2: Create the Topic `weather-stream`
On **Computer 1 (Master)**:
```bash
bash kafka/create_topic.sh master:9092
```
*(Creates topic with 3 partitions and replication factor 2)*

---

## 6. Apache Spark Setup

### Step 6.1: Deploy Spark Configurations
Copy template files to your Spark installation:
```bash
cp spark/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
cp spark/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
cp spark/conf/workers $SPARK_HOME/conf/workers
```

### Step 6.2: Start Spark Master and Workers
On **Computer 1 (Master)**:
```bash
# Start Master
$SPARK_HOME/sbin/start-master.sh

# Start Workers on worker1 and worker2 via SSH:
$SPARK_HOME/sbin/start-workers.sh
```
*(Or manually run `$SPARK_HOME/sbin/start-worker.sh spark://master:7077` on Worker 1 and Worker 2)*

Verify workers in the **Spark Web UI**: open `http://192.168.1.100:8080` in your browser.
You should see:
- `Alive Workers: 2`
- Total Cores: 8 (4 + 4)
- Total Memory: 6 GB (3 GB + 3 GB)

---

## 7. Execution Order: Running the Live Project Demonstration

Open the project directory `weather-data-analytics` and execute the steps in sequence:

### Step 7.1: Initialize HDFS Folders & Upload Historical Data (Master PC)
```bash
# Create HDFS directories
python hadoop/hdfs_manager.py init hdfs://master:9000

# Upload dataset to HDFS
python hadoop/hdfs_manager.py upload data/weather_data.csv /weather/data/weather_data.csv
```

### Step 7.2: Start the Real-Time Kafka Weather Producer (Master PC or Any Node)
```bash
python streaming/weather_producer.py master:9092
```
*Output: Streams live weather observations across 8 cities every 2 seconds.*

### Step 7.3: Submit PySpark Structured Streaming to HDFS (Master PC)
In a new terminal:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
             --master spark://master:7077 \
             streaming/kafka_to_hdfs_stream.py master:9092 hdfs://master:9000
```
*Output: Structured streaming ingests messages from Kafka, parses JSON, computes windowed aggregates, and writes Parquet files to `hdfs://master:9000/weather/streaming_output`.*

### Step 7.4: Run PySpark Batch Analytics & MLlib Model (Master PC)
In a new terminal:
```bash
# Distributed batch analysis
spark-submit --master spark://master:7077 spark/weather_batch_analytics.py hdfs://master:9000/weather/data/weather_data.csv hdfs://master:9000/weather/analytics

# Train machine learning prediction model
spark-submit --master spark://master:7077 spark/weather_ml_model.py
```

### Step 7.5: Launch the Streamlit Web UI (Master PC)
```bash
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
```

---

## 8. What to Show on Each Computer Screen During Viva/Presentation

This layout creates the strongest impression on the external examiners:

### Computer 1 (Master Node):
1. **Primary Screen**: The **Streamlit Web Dashboard** (`http://192.168.1.100:8501`):
   - **Tab 1**: Show live Kafka stream events arriving every 2 seconds and real-time Plotly charts updating.
   - **Tab 2**: Show PySpark HDFS analytics, temperature distributions, and correlation heatmap.
   - **Tab 3**: Show extreme weather alert badges (heatwaves, storms) on the global geo-map.
   - **Tab 4**: Move the interactive sliders to predict temperature live using the PySpark MLlib model!
   - **Tab 5**: Show the 3-Computer cluster topology card.
2. **Terminal Split**:
   - Left terminal running `weather_producer.py`.
   - Right terminal running `kafka_to_hdfs_stream.py`.

### Computer 2 (Worker Node 1):
1. **Browser**: Open Hadoop NameNode Web UI:
   `http://192.168.1.100:9870/dfshealth.html#tab-datanode`
   - Point out **Live DataNodes = 2** and block replication = 2.
   - Click `Utilities ➔ Browse the file system` to show `/weather/streaming_output` receiving live Parquet blocks!
2. **Terminal**:
   - Run `jps` to show `DataNode` and `Spark Worker` running on this physical machine.

### Computer 3 (Worker Node 2):
1. **Browser**: Open Apache Spark Master Web UI:
   `http://192.168.1.100:8080`
   - Show the 2 registered workers, executor memory, CPU core distribution, and active streaming applications.
2. **Terminal**:
   - Run `hdfs dfs -tail /weather/data/weather_data.csv` to demonstrate distributed read access directly from Worker 2!
