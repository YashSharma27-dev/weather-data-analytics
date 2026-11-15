"""
Dashboard Utility & Data Connector Module
=========================================
Handles robust data ingestion, cluster socket health checking,
and file caching with automatic local/distributed failover.
"""

import os
import json
import socket
import pandas as pd
from datetime import datetime

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
STREAM_BUFFER_FILE = os.path.join(DATA_DIR, "stream_buffer.jsonl")
HISTORICAL_CSV_FILE = os.path.join(DATA_DIR, "weather_data.csv")
ANALYTICS_CACHE_FILE = os.path.join(DATA_DIR, "analytics_cache", "summary_metrics.json")
ML_CACHE_FILE = os.path.join(DATA_DIR, "analytics_cache", "ml_model_metrics.json")

def check_socket(host, port, timeout=1.0):
    """Probes whether a cluster service is reachable on a given host and port."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        return result == 0
    except Exception:
        return False

def load_streaming_data(max_records=150):
    """Loads latest streaming records from sliding buffer file."""
    records = []
    if os.path.exists(STREAM_BUFFER_FILE):
        try:
            with open(STREAM_BUFFER_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in lines[-max_records:]:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        except Exception as e:
            print(f"Error reading stream buffer: {e}")

    if not records and os.path.exists(HISTORICAL_CSV_FILE):
        # Fallback to tail of historical dataset
        df_hist = pd.read_csv(HISTORICAL_CSV_FILE)
        return df_hist.tail(max_records)

    if records:
        df = pd.DataFrame(records)
        return df
    return pd.DataFrame()

def load_historical_data():
    """Loads the complete historical weather dataset."""
    if os.path.exists(HISTORICAL_CSV_FILE):
        return pd.read_csv(HISTORICAL_CSV_FILE)
    return pd.DataFrame()

def load_batch_analytics_metrics():
    """Loads pre-computed PySpark batch analytics metrics."""
    if os.path.exists(ANALYTICS_CACHE_FILE):
        with open(ANALYTICS_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def load_ml_metrics():
    """Loads PySpark MLlib model metrics and weights."""
    if os.path.exists(ML_CACHE_FILE):
        with open(ML_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def predict_temperature(humidity, pressure, wind_speed, hour, ml_metrics=None):
    """Computes real-time inference using trained model coefficients."""
    if ml_metrics and "coefficients" in ml_metrics:
        coef = ml_metrics["coefficients"]
        intercept = ml_metrics.get("intercept", 20.0)
        pred = (
            intercept +
            coef.get("humidity", -0.08) * humidity +
            coef.get("pressure", 0.01) * pressure +
            coef.get("wind_speed", -0.05) * wind_speed +
            coef.get("hour", 0.25) * hour
        )
        return round(float(pred), 2)
    # Physical atmospheric lapse & diurnal approximation
    diurnal = -2.0 + 4.0 * ((hour - 4) % 24) / 14.0 if 4 <= hour <= 18 else -1.5
    base = 25.0 - (humidity * 0.08) + diurnal
    return round(float(base), 2)
