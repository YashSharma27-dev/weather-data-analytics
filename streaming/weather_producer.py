import time
import json
import requests
from kafka import KafkaProducer

# Initialize Kafka Producer pointing to localhost
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'weather-stream'
URL = "https://api.open-meteo.com/v1/forecast?latitude=28.6139&longitude=77.2090&current_weather=true"

print(f"Starting Weather Producer... Publishing to Kafka topic: {TOPIC_NAME}")

try:
    while True:
        response = requests.get(URL)
        if response.status_code == 200:
            data = response.json()
            current = data.get("current_weather", {})
            
            payload = {
                "city": "Delhi",
                "temperature": current.get("temperature"),
                "wind_speed": current.get("windspeed"),
                "timestamp": current.get("time")
            }
            
            producer.send(TOPIC_NAME, value=payload)
            print(f"Published to Kafka: {payload}")
        
        # Wait 10 seconds before fetching the next live update
        time.sleep(10)
except KeyboardInterrupt:
    print("Stopped Weather Producer.")
    producer.close()
