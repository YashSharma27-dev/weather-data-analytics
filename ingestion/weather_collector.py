import os
import requests
import pandas as pd
from datetime import datetime

API_URL = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 28.6139,
    "longitude": 77.2090,
    "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
    "timezone": "Asia/Kolkata"
}

try:
    response = requests.get(API_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    current = data["current"]

    record = {
        "city": "Delhi",
        "timestamp": current["time"],
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "collected_at": datetime.now().isoformat()
    }

    print("Successfully collected weather observation:")
    print(record)

    # Save/Append to CSV file
    os.makedirs("data", exist_ok=True)
    csv_file = "data/weather.csv"
    
    df_new = pd.DataFrame([record])
    
    if os.path.exists(csv_file):
        df_new.to_csv(csv_file, mode='a', header=False, index=False)
    else:
        df_new.to_csv(csv_file, mode='w', header=True, index=False)
        
    print(f"Record saved to {csv_file}")

except Exception as e:
    print(f"Error fetching weather data: {e}")