"""
Kafka Weather Producer:
Streams real-time weather observations for multiple cities into the Apache Kafka topic.
Supports:
1. Live Open-Meteo REST API data fetching.
2. Offline / Lab Simulation mode (generates realistic physical weather variations if internet is down).
3. Local buffer fallback (enables testing even without Kafka daemon).
"""

import os
import sys
import json
import time
import random
import requests
from datetime import datetime

# Prevent namespace collision if run inside repo
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

TOPIC_NAME = "weather-stream"
DEFAULT_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
BUFFER_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "stream_buffer.jsonl")

CITIES_METADATA = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090, "base_temp": 26.0},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777, "base_temp": 29.0},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946, "base_temp": 24.0},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639, "base_temp": 28.0},
    "Chennai": {"lat": 13.0827, "lon": 80.2707, "base_temp": 30.0},
    "New York": {"lat": 40.7128, "lon": -74.0060, "base_temp": 14.0},
    "London": {"lat": 51.5074, "lon": -0.1278, "base_temp": 11.0},
    "Tokyo": {"lat": 35.6762, "lon": 139.6503, "base_temp": 16.0},
    "Sydney": {"lat": -33.8688, "lon": 151.2093, "base_temp": 20.0},
    "Paris": {"lat": 48.8566, "lon": 2.3522, "base_temp": 13.0}
}

def calculate_heat_index(temp_c, humidity):
    tf = temp_c * 9/5 + 32
    if tf < 80:
        hi_f = 0.5 * (tf + 61.0 + ((tf - 68.0) * 1.2) + (humidity * 0.094))
    else:
        hi_f = -42.379 + 2.04901523*tf + 10.14333127*humidity - 0.22475541*tf*humidity \
               - 6.83783e-3*tf**2 - 5.481717e-2*humidity**2 + 1.22874e-3*(tf**2)*humidity \
               + 8.5282e-4*tf*(humidity**2) - 1.99e-6*(tf**2)*(humidity**2)
    return round((hi_f - 32) * 5/9, 2)

def fetch_live_weather(city, lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,surface_pressure,precipitation",
        "timezone": "auto"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    curr = response.json()["current"]
    temp = curr.get("temperature_2m", 25.0)
    hum = curr.get("relative_humidity_2m", 60.0)
    wind = curr.get("wind_speed_10m", 10.0)
    wind_dir = curr.get("wind_direction_10m", 180)
    pressure = curr.get("surface_pressure", 1013.0)
    precip = curr.get("precipitation", 0.0)
    
    cond = "Clear"
    if precip > 5.0:
        cond = "Heavy Rain"
    elif precip > 0.0:
        cond = "Light Rain"
    elif hum > 80:
        cond = "Overcast"
    elif hum > 65:
        cond = "Partly Cloudy"

    return {
        "city": city,
        "latitude": lat,
        "longitude": lon,
        "timestamp": curr.get("time", datetime.now().strftime("%Y-%m-%dT%H:%M")),
        "temperature": round(temp, 2),
        "humidity": round(hum, 1),
        "wind_speed": round(wind, 2),
        "wind_direction": int(wind_dir),
        "pressure": round(pressure, 1),
        "precipitation": round(precip, 2),
        "weather_condition": cond,
        "heat_index": calculate_heat_index(temp, hum),
        "aqi": random.randint(45, 180),
        "collected_at": datetime.now().isoformat()
    }

def simulate_weather(city, lat, lon):
    base_t = CITIES_METADATA[city]["base_temp"]
    temp = round(base_t + random.uniform(-4.0, 4.0), 2)
    hum = round(random.uniform(40.0, 85.0), 1)
    wind = round(max(1.0, random.gauss(14.0, 7.0)), 2)
    wind_dir = random.randint(0, 359)
    pressure = round(random.gauss(1012.0, 5.0), 1)
    precip = round(random.uniform(0.0, 8.0), 2) if hum > 75 and random.random() < 0.3 else 0.0
    cond = "Heavy Rain" if precip > 4.0 else ("Light Rain" if precip > 0 else ("Partly Cloudy" if hum > 60 else "Clear"))
    
    return {
        "city": city,
        "latitude": lat,
        "longitude": lon,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": temp,
        "humidity": hum,
        "wind_speed": wind,
        "wind_direction": wind_dir,
        "pressure": pressure,
        "precipitation": precip,
        "weather_condition": cond,
        "heat_index": calculate_heat_index(temp, hum),
        "aqi": random.randint(35, 210),
        "collected_at": datetime.now().isoformat()
    }

def append_to_local_buffer(record):
    """Saves records to a sliding buffer file for local or dashboard consumption."""
    os.makedirs(os.path.dirname(BUFFER_FILE), exist_ok=True)
    with open(BUFFER_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

def run_producer(broker=DEFAULT_BROKER, interval=3, force_simulation=False):
    producer = None
    if KAFKA_AVAILABLE and not force_simulation:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                request_timeout_ms=5000
            )
            print(f"[*] Successfully connected to Kafka Broker at {broker}")
        except Exception as e:
            print(f"[!] Warning: Could not connect to Kafka broker ({e}).")
            print("[*] Running in local buffer streaming mode (records will be saved to stream_buffer.jsonl).")
    else:
        print("[*] Kafka library not detected or simulation requested. Operating in local buffer streaming mode.")

    print(f"[*] Starting Weather Streaming Ingestion on topic '{TOPIC_NAME}' (Interval: {interval}s)...")
    print("[*] Press Ctrl+C to terminate.")

    cities = list(CITIES_METADATA.keys())
    try:
        while True:
            city = random.choice(cities)
            meta = CITIES_METADATA[city]

            try:
                if force_simulation:
                    record = simulate_weather(city, meta["lat"], meta["lon"])
                else:
                    try:
                        record = fetch_live_weather(city, meta["lat"], meta["lon"])
                    except Exception:
                        record = simulate_weather(city, meta["lat"], meta["lon"])
            except Exception as e:
                print(f"[!] Error collecting data: {e}")
                record = simulate_weather(city, meta["lat"], meta["lon"])

            # Send to Kafka if connected
            if producer:
                try:
                    producer.send(TOPIC_NAME, value=record)
                    producer.flush()
                    print(f"-> [KAFKA] Emitted: {record['city']} | Temp: {record['temperature']}C | Hum: {record['humidity']}% | Cond: {record['weather_condition']}")
                except Exception as e:
                    print(f"[!] Failed to push to Kafka: {e}")
            else:
                print(f"-> [BUFFER] Recorded: {record['city']} | Temp: {record['temperature']}C | Hum: {record['humidity']}% | Cond: {record['weather_condition']}")

            # Always maintain local buffer for UI and HDFS batch sync
            append_to_local_buffer(record)
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[*] Stopping Weather Stream Producer.")
        if producer:
            producer.close()

if __name__ == "__main__":
    broker_arg = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BROKER
    sim_mode = "--simulate" in sys.argv
    run_producer(broker=broker_arg, interval=3, force_simulation=sim_mode)
