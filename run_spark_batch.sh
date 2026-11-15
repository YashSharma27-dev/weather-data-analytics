#!/usr/bin/env bash
echo "=========================================================="
echo " Executing PySpark Distributed Batch Analytics"
echo "=========================================================="

python3 spark/weather_batch_analytics.py
echo ""
echo "Executing Machine Learning Training Pipeline:"
python3 spark/weather_ml_model.py
