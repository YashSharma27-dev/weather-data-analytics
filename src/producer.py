import time
import json
import requests
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'weather-stream'
# Open-Meteo API endpoint for Delhi coordinates (Lat: 28.6139, Lon: 77.2090)
API_URL = "https://api.open-meteo.com/v1/forecast?latitude=28.6139&longitude=77.2090&current=temperature_2m,relative_humidity_2m,wind_speed_10m"

print(f"Starting Weather Producer... Publishing to Kafka topic: {TOPIC_NAME}")

try:
    while True:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            current = data.get("current", {})

            payload = {
                "timestamp": current.get("time"),
                "temperature_c": current.get("temperature_2m"),
                "humidity_pct": current.get("relative_humidity_2m"),
                "wind_speed": current.get("wind_speed_10m"),
                "location": "Delhi"
            }

            producer.send(TOPIC_NAME, value=payload)
            producer.flush()
            print(f"Produced event: {payload}")
        else:
            print(f"Failed to fetch API data: {response.status_code}")

        # Poll every 10 seconds
        time.sleep(10)
except KeyboardInterrupt:
    print("Producer stopped by user.")
