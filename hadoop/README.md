# Apache Hadoop Configuration (3-Computer Cluster)

This directory contains the production-grade XML configurations and management scripts for deploying Hadoop (HDFS & YARN) on a 3-computer distributed cluster.

---

## 1. Cluster Architecture Overview

| Node Role | Hostname | Default IP | Daemons Running |
| :--- | :--- | :--- | :--- |
| **Master Node (Computer 1)** | `master` | `192.168.1.100` | NameNode, SecondaryNameNode, ResourceManager |
| **Worker 1 (Computer 2)** | `worker1` | `192.168.1.101` | DataNode, NodeManager |
| **Worker 2 (Computer 3)** | `worker2` | `192.168.1.102` | DataNode, NodeManager |

---

## 2. Configuration Files in `hadoop/conf/`

1. **`core-site.xml`**: Defines `fs.defaultFS = hdfs://master:9000` and Hadoop temporary directories.
2. **`hdfs-site.xml`**: Sets replication factor `dfs.replication = 2` (matching the 2 worker data nodes) and NameNode/DataNode data paths.
3. **`yarn-site.xml`**: Configures YARN ResourceManager host (`master`) and NodeManager memory limits.
4. **`mapred-site.xml`**: Routes MapReduce jobs through YARN (`mapreduce.framework.name = yarn`).
5. **`workers`**: Lists `worker1` and `worker2` so the Master can start/stop daemons across all computers simultaneously.

---

## 3. Deployment Steps on the 3 Computers

### Step A: Configure `/etc/hosts` on ALL 3 Computers
Add these lines to `/etc/hosts` (or `C:\Windows\System32\drivers\etc\hosts` on Windows):
```text
192.168.1.100 master
192.168.1.101 worker1
192.168.1.102 worker2
```

### Step B: Copy Configuration Files
Copy all XML files and `workers` from `hadoop/conf/` into your Hadoop configuration folder:
```bash
cp hadoop/conf/* $HADOOP_HOME/etc/hadoop/
```
*(Repeat this on Master, Worker1, and Worker2)*

### Step C: Format NameNode (Master PC only - First Time Only!)
```bash
hdfs namenode -format
```

### Step D: Start Distributed File System (HDFS) and YARN
On **Master PC**:
```bash
start-dfs.sh
start-yarn.sh
```

### Step E: Verify Daemons with `jps`
- On **Master**:
  ```bash
  jps
  # Should show: NameNode, SecondaryNameNode, ResourceManager, Jps
  ```
- On **Worker 1** & **Worker 2**:
  ```bash
  jps
  # Should show: DataNode, NodeManager, Jps
  ```

---

## 4. Initializing HDFS Directories for Weather Data

Run the included HDFS Manager utility on the Master PC:
```bash
# 1. Initialize project directory structure
python hadoop/hdfs_manager.py init hdfs://master:9000

# 2. Upload initial historical dataset
python hadoop/hdfs_manager.py upload data/weather_data.csv /weather/data/weather_data.csv

# 3. Verify files on HDFS
python hadoop/hdfs_manager.py ls /weather
```

---

## 5. Web Dashboards
- **Hadoop NameNode UI**: `http://master:9870` or `http://192.168.1.100:9870`
- **YARN Cluster Manager**: `http://master:8088` or `http://192.168.1.100:8088`
