import os
import requests
import pandas as pd
from datetime import datetime

CITIES = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090, "tz": "Asia/Kolkata"},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777, "tz": "Asia/Kolkata"},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946, "tz": "Asia/Kolkata"},
    "New York": {"lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    "London": {"lat": 51.5074, "lon": -0.1278, "tz": "Europe/London"},
}

API_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather_for_city(city_name="Delhi"):
    geo = CITIES.get(city_name, CITIES["Delhi"])
    params = {
        "latitude": geo["lat"],
        "longitude": geo["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,surface_pressure,precipitation",
        "timezone": geo["tz"]
    }

    try:
        response = requests.get(API_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        current = data["current"]

        temp = current.get("temperature_2m", 25.0)
        hum = current.get("relative_humidity_2m", 60.0)
        precip = current.get("precipitation", 0.0)
        cond = "Rain" if precip > 0 else ("Cloudy" if hum > 70 else "Clear")

        record = {
            "city": city_name,
            "latitude": geo["lat"],
            "longitude": geo["lon"],
            "timestamp": current["time"],
            "temperature": temp,
            "humidity": hum,
            "wind_speed": current.get("wind_speed_10m", 5.0),
            "wind_direction": current.get("wind_direction_10m", 180),
            "pressure": current.get("surface_pressure", 1013.0),
            "precipitation": precip,
            "weather_condition": cond,
            "heat_index": round(temp + (hum / 100.0) * 2.5, 2),
            "aqi": 120,
            "collected_at": datetime.now().isoformat()
        }

        print(f"Successfully collected weather observation for {city_name}:")
        print(record)

        # Save/Append to CSV file
        os.makedirs("data", exist_ok=True)
        csv_file = "data/weather_data.csv"
        df_new = pd.DataFrame([record])

        if os.path.exists(csv_file):
            df_new.to_csv(csv_file, mode='a', header=False, index=False)
        else:
            df_new.to_csv(csv_file, mode='w', header=True, index=False)

        print(f"Record saved to {csv_file}")
        return record

    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return None

if __name__ == "__main__":
    fetch_weather_for_city("Delhi")
