#!/usr/bin/env bash
echo "=========================================================="
echo " Launching Weather Data Analytics Streamlit Dashboard"
echo "=========================================================="

if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
