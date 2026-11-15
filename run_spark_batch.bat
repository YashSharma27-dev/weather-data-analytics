@echo off
echo ==========================================================
echo  Executing PySpark Distributed Batch Analytics
echo ==========================================================

python spark/weather_batch_analytics.py
echo.
echo Executing Machine Learning Training Pipeline:
python spark/weather_ml_model.py
pause
