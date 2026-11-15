#!/usr/bin/env bash
# Creates the 'weather-stream' topic on the Kafka cluster with 3 partitions and replication factor 2

KAFKA_BIN="${KAFKA_HOME:-/opt/kafka}/bin"
BROKER="${1:-master:9092}"
TOPIC="weather-stream"

echo "=========================================================="
echo " Creating Kafka Topic: $TOPIC on $BROKER"
echo "=========================================================="

"$KAFKA_BIN/kafka-topics.sh" --create \
    --bootstrap-server "$BROKER" \
    --replication-factor 2 \
    --partitions 3 \
    --topic "$TOPIC" \
    --if-not-exists

echo ""
echo "=== Describing Topic ==="
"$KAFKA_BIN/kafka-topics.sh" --describe \
    --bootstrap-server "$BROKER" \
    --topic "$TOPIC"
