"""
Hadoop HDFS Cluster Utility Manager
====================================
Helper utility to initialize directories, upload raw datasets,
and inspect file contents on HDFS across the 3-computer cluster.

Usage:
  python hadoop/hdfs_manager.py init [HDFS_URL]
  python hadoop/hdfs_manager.py upload [LOCAL_FILE] [HDFS_DEST]
  python hadoop/hdfs_manager.py ls [HDFS_PATH]
  python hadoop/hdfs_manager.py check [NAMENODE_HOST]
"""

import os
import sys
import subprocess
import requests

DEFAULT_HDFS = os.getenv("HDFS_NAMENODE", "hdfs://master:9000")
NAMENODE_WEB_PORT = 9870

def run_cmd(cmd):
    print(f"[*] Executing: {cmd}")
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if res.stdout:
        print(res.stdout.strip())
    if res.stderr and res.returncode != 0:
        print(f"[!] Stderr: {res.stderr.strip()}")
    return res.returncode == 0

def check_namenode_health(host="master"):
    url = f"http://{host}:{NAMENODE_WEB_PORT}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
    print(f"[*] Probing NameNode JMX endpoint at {url}...")
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            beans = data.get("beans", [{}])[0]
            capacity = beans.get("CapacityTotalGB", "N/A")
            live_nodes = beans.get("NumLiveDataNodes", "N/A")
            dead_nodes = beans.get("NumDeadDataNodes", "N/A")
            print(f"[OK] Hadoop NameNode is ONLINE and healthy!")
            print(f"     Live DataNodes: {live_nodes}")
            print(f"     Dead DataNodes: {dead_nodes}")
            print(f"     Total Capacity: {capacity} GB")
            return True
        else:
            print(f"[!] NameNode returned HTTP {r.status_code}")
            return False
    except Exception as e:
        print(f"[!] NameNode probe failed: {e}")
        print("[i] Ensure Hadoop daemons are started on Master: start-dfs.sh && start-yarn.sh")
        return False

def init_hdfs_dirs(hdfs_base=DEFAULT_HDFS):
    print(f"[*] Initializing HDFS Project Directory Tree at {hdfs_base}...")
    dirs = [
        f"{hdfs_base}/weather",
        f"{hdfs_base}/weather/data",
        f"{hdfs_base}/weather/streaming_output",
        f"{hdfs_base}/weather/checkpoint",
        f"{hdfs_base}/weather/analytics"
    ]
    for d in dirs:
        run_cmd(f"hdfs dfs -mkdir -p {d}")
    print("[OK] HDFS directories initialized.")

def upload_dataset(local_file="data/weather_data.csv", hdfs_dest="/weather/data/weather_data.csv"):
    if not os.path.exists(local_file):
        print(f"[!] Local file not found: {local_file}")
        return
    print(f"[*] Uploading {local_file} -> {hdfs_dest}...")
    success = run_cmd(f"hdfs dfs -put -f {local_file} {hdfs_dest}")
    if success:
        print(f"[OK] Successfully uploaded dataset to HDFS: {hdfs_dest}")
        run_cmd(f"hdfs dfs -ls {os.path.dirname(hdfs_dest)}")

def list_hdfs(path="/weather"):
    print(f"[*] Listing contents of HDFS path: {path}")
    run_cmd(f"hdfs dfs -ls -R {path}")

if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "check"

    if action == "check":
        host_arg = sys.argv[2] if len(sys.argv) > 2 else "master"
        check_namenode_health(host_arg)
    elif action == "init":
        hdfs_url = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_HDFS
        init_hdfs_dirs(hdfs_url)
    elif action == "upload":
        src = sys.argv[2] if len(sys.argv) > 2 else "data/weather_data.csv"
        dst = sys.argv[3] if len(sys.argv) > 3 else "/weather/data/weather_data.csv"
        upload_dataset(src, dst)
    elif action == "ls":
        target_path = sys.argv[2] if len(sys.argv) > 2 else "/weather"
        list_hdfs(target_path)
    else:
        print(f"Unknown action: {action}")
        print("Available actions: check, init, upload, ls")
