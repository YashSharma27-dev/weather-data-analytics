"""
Kafka Weather Data Producer
===========================
Streams real-time multi-city weather observations to an Apache Kafka topic.
Designed for 3-Computer Cluster execution (Master: Node 1) with fallback
for standalone/local testing.

Usage:
  python streaming/weather_producer.py [KAFKA_BROKER_IP:9092] [--simulate]
Example on Cluster:
  python streaming/weather_producer.py 192.168.1.100:9092
"""

import os
import sys
import json
import time
import random
import requests
from datetime import datetime

# Graceful Kafka import to avoid fatal errors if running offline
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "weather-stream")
DEFAULT_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
BUFFER_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "stream_buffer.jsonl")

# Monitored cities with geographical and baseline parameters
CITIES = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090, "base_temp": 26.0, "tz": "Asia/Kolkata"},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777, "base_temp": 29.0, "tz": "Asia/Kolkata"},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946, "base_temp": 24.0, "tz": "Asia/Kolkata"},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639, "base_temp": 28.0, "tz": "Asia/Kolkata"},
    "Chennai": {"lat": 13.0827, "lon": 80.2707, "base_temp": 30.0, "tz": "Asia/Kolkata"},
    "New York": {"lat": 40.7128, "lon": -74.0060, "base_temp": 14.0, "tz": "America/New_York"},
    "London": {"lat": 51.5074, "lon": -0.1278, "base_temp": 11.0, "tz": "Europe/London"},
    "Tokyo": {"lat": 35.6762, "lon": 139.6503, "base_temp": 16.0, "tz": "Asia/Tokyo"},
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

def fetch_live_weather(city, geo):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": geo["lat"],
        "longitude": geo["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,surface_pressure,precipitation",
        "timezone": geo["tz"]
    }
    response = requests.get(url, params=params, timeout=8)
    response.raise_for_status()
    curr = response.json().get("current", {})

    temp = float(curr.get("temperature_2m", geo["base_temp"]))
    hum = float(curr.get("relative_humidity_2m", 60.0))
    wind = float(curr.get("wind_speed_10m", 10.0))
    wind_dir = int(curr.get("wind_direction_10m", 180))
    pressure = float(curr.get("surface_pressure", 1013.0))
    precip = float(curr.get("precipitation", 0.0))

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
        "latitude": geo["lat"],
        "longitude": geo["lon"],
        "timestamp": curr.get("time", datetime.now().strftime("%Y-%m-%dT%H:%M")),
        "temperature": round(temp, 2),
        "humidity": round(hum, 1),
        "wind_speed": round(wind, 2),
        "wind_direction": wind_dir,
        "pressure": round(pressure, 1),
        "precipitation": round(precip, 2),
        "weather_condition": cond,
        "heat_index": calculate_heat_index(temp, hum),
        "aqi": random.randint(45, 180),
        "collected_at": datetime.now().isoformat()
    }

def simulate_weather(city, geo):
    base_t = geo["base_temp"]
    temp = round(base_t + random.uniform(-4.0, 4.0), 2)
    hum = round(random.uniform(40.0, 85.0), 1)
    wind = round(max(1.0, random.gauss(14.0, 6.0)), 2)
    wind_dir = random.randint(0, 359)
    pressure = round(random.gauss(1012.0, 5.0), 1)
    precip = round(random.uniform(0.0, 6.0), 2) if hum > 75 and random.random() < 0.25 else 0.0
    cond = "Heavy Rain" if precip > 4.0 else ("Light Rain" if precip > 0 else ("Partly Cloudy" if hum > 60 else "Clear"))

    return {
        "city": city,
        "latitude": geo["lat"],
        "longitude": geo["lon"],
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

def append_to_buffer(payload):
    os.makedirs(os.path.dirname(BUFFER_FILE), exist_ok=True)
    with open(BUFFER_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")

def main():
    broker = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else DEFAULT_BROKER
    simulate = "--simulate" in sys.argv

    producer = None
    if KAFKA_AVAILABLE and not simulate:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                request_timeout_ms=5000
            )
            print(f"[OK] Connected to Kafka Broker at {broker}")
        except Exception as e:
            print(f"[!] Warning: Cannot connect to Kafka at {broker} ({e}).")
            print("[i] Using local buffer streaming mode (data/stream_buffer.jsonl).")
    else:
        print("[i] Running in simulated streaming mode. Writing to local buffer.")

    print(f"[*] Starting Weather Streaming Ingestion on topic '{TOPIC_NAME}'...")
    print(f"[*] Target Broker: {broker}")
    print("[*] Press Ctrl+C to stop.\n")

    cities_list = list(CITIES.keys())
    try:
        while True:
            # Round-robin or random selection across cities
            for city in cities_list:
                geo = CITIES[city]
                if simulate:
                    payload = simulate_weather(city, geo)
                else:
                    try:
                        payload = fetch_live_weather(city, geo)
                    except Exception as err:
                        payload = simulate_weather(city, geo)

                # Send to Kafka Broker
                if producer:
                    try:
                        producer.send(TOPIC_NAME, value=payload)
                        producer.flush()
                        print(f"-> [KAFKA] {payload['city']:<10} | Temp: {payload['temperature']:>5} C | Hum: {payload['humidity']:>4}% | Wind: {payload['wind_speed']:>5} km/h | {payload['weather_condition']}")
                    except Exception as kerr:
                        print(f"[!] Kafka send error: {kerr}")
                else:
                    print(f"-> [BUFFER] {payload['city']:<10} | Temp: {payload['temperature']:>5} C | Hum: {payload['humidity']:>4}% | Wind: {payload['wind_speed']:>5} km/h | {payload['weather_condition']}")

                append_to_buffer(payload)
                time.sleep(2)  # 2 seconds per city

            print("--- Cycle completed. Pausing for 5 seconds ---")
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[*] Stopping Weather Stream Producer.")
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
