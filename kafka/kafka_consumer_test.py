"""
Kafka Verification Consumer Test
================================
Consumes and prints messages from the 'weather-stream' topic
to verify live message delivery across the 3-computer cluster.

Usage:
  python kafka/kafka_consumer_test.py [BROKER_IP:9092]
Example:
  python kafka/kafka_consumer_test.py 192.168.1.100:9092
"""

import sys
import json
import os

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

TOPIC = "weather-stream"
DEFAULT_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

def run_consumer(broker):
    if not KAFKA_AVAILABLE:
        print("[!] kafka-python is not installed. Run: pip install kafka-python-ng")
        return

    print("=" * 60)
    print(f" Connecting to Kafka Broker at: {broker}")
    print(f" Subscribing to Topic: {TOPIC}")
    print(" Press Ctrl+C to stop listening.")
    print("=" * 60)

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[broker],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='weather-verification-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            data = message.value
            print(f"[Topic: {message.topic} | Partition: {message.partition} | Offset: {message.offset}]")
            print(f"  City:        {data.get('city')}")
            print(f"  Temperature: {data.get('temperature')} C")
            print(f"  Humidity:    {data.get('humidity')} %")
            print(f"  Wind Speed:  {data.get('wind_speed')} km/h")
            print(f"  Condition:   {data.get('weather_condition')}")
            print(f"  Timestamp:   {data.get('timestamp')}")
            print("-" * 50)

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    except Exception as e:
        print(f"[!] Error consuming from Kafka: {e}")

if __name__ == "__main__":
    target_broker = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BROKER
    run_consumer(target_broker)
