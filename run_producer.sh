#!/usr/bin/env bash
BROKER="${1:-localhost:9092}"
echo "=========================================================="
echo " Starting Weather Stream Ingestion targeting $BROKER"
echo "=========================================================="

python3 streaming/weather_producer.py "$BROKER"
