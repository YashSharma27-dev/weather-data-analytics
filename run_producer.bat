@echo off
echo ==========================================================
echo  Starting Weather Stream Ingestion (Kafka Producer)
echo ==========================================================

REM Set broker argument (default: localhost:9092 or pass cluster IP e.g. 192.168.1.100:9092)
set BROKER=%1
if "%BROKER%"=="" set BROKER=localhost:9092

python streaming/weather_producer.py %BROKER%
pause
