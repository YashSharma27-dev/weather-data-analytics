@echo off
echo ==========================================================
echo  Launching Weather Data Analytics Streamlit Dashboard
echo ==========================================================

REM Check virtual environment
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
)

streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
pause
