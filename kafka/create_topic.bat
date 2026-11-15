@echo off
REM Creates the 'weather-stream' topic on Windows Kafka installation
set BROKER=%1
if "%BROKER%"=="" set BROKER=localhost:9092
set TOPIC=weather-stream

echo ==========================================================
echo  Creating Kafka Topic: %TOPIC% on %BROKER%
echo ==========================================================

call kafka-topics.bat --create --bootstrap-server %BROKER% --replication-factor 1 --partitions 3 --topic %TOPIC% --if-not-exists

echo.
echo === Describing Topic ===
call kafka-topics.bat --describe --bootstrap-server %BROKER% --topic %TOPIC%
pause
