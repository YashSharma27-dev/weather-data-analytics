"""
Dataset Generator for Weather Data Analytics Project.
Generates realistic multi-city historical and streaming weather observations
for PySpark, HDFS, and Kafka pipelines.
"""

import os
import random
import csv
from datetime import datetime, timedelta

CITIES = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090, "base_temp": 26.0, "temp_var": 12.0, "base_hum": 55, "hum_var": 25},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777, "base_temp": 29.0, "temp_var": 5.0, "base_hum": 78, "hum_var": 15},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946, "base_temp": 24.0, "temp_var": 6.0, "base_hum": 65, "hum_var": 20},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639, "base_temp": 28.0, "temp_var": 8.0, "base_hum": 75, "hum_var": 18},
    "Chennai": {"lat": 13.0827, "lon": 80.2707, "base_temp": 30.0, "temp_var": 6.0, "base_hum": 74, "hum_var": 16},
    "New York": {"lat": 40.7128, "lon": -74.0060, "base_temp": 14.0, "temp_var": 14.0, "base_hum": 62, "hum_var": 20},
    "London": {"lat": 51.5074, "lon": -0.1278, "base_temp": 11.0, "temp_var": 9.0, "base_hum": 72, "hum_var": 18},
    "Tokyo": {"lat": 35.6762, "lon": 139.6503, "base_temp": 16.0, "temp_var": 11.0, "base_hum": 64, "hum_var": 22},
    "Sydney": {"lat": -33.8688, "lon": 151.2093, "base_temp": 20.0, "temp_var": 8.0, "base_hum": 68, "hum_var": 18},
    "Paris": {"lat": 48.8566, "lon": 2.3522, "base_temp": 13.0, "temp_var": 10.0, "base_hum": 70, "hum_var": 20}
}

WEATHER_CONDITIONS = ["Clear", "Partly Cloudy", "Overcast", "Light Rain", "Heavy Rain", "Thunderstorm", "Hazy", "Foggy"]

def calculate_heat_index(temp_c, humidity):
    """Calculates apparent temperature / heat index approximation in Celsius."""
    # Rothfusz regression simplified
    tf = temp_c * 9/5 + 32
    if tf < 80:
        hi_f = 0.5 * (tf + 61.0 + ((tf - 68.0) * 1.2) + (humidity * 0.094))
    else:
        hi_f = -42.379 + 2.04901523*tf + 10.14333127*humidity - 0.22475541*tf*humidity \
               - 6.83783e-3*tf**2 - 5.481717e-2*humidity**2 + 1.22874e-3*(tf**2)*humidity \
               + 8.5282e-4*tf*(humidity**2) - 1.99e-6*(tf**2)*(humidity**2)
    return round((hi_f - 32) * 5/9, 2)

def generate_records(num_days=30, readings_per_day=4):
    records = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_days)

    total_steps = num_days * readings_per_day
    step_delta = timedelta(hours=24 / readings_per_day)

    for step in range(total_steps):
        curr_time = start_date + (step * step_delta)
        hour = curr_time.hour
        diurnal_factor = -1.0 + 2.0 * ((hour - 4) % 24) / 14.0 if 4 <= hour <= 18 else -0.8

        for city, profile in CITIES.items():
            temp = profile["base_temp"] + (profile["temp_var"] * 0.4 * diurnal_factor) + random.uniform(-2.5, 2.5)
            temp = round(max(-5.0, min(50.0, temp)), 2)
            
            # Inverse relationship between temperature and humidity
            humidity = profile["base_hum"] - (diurnal_factor * 12.0) + random.uniform(-8.0, 8.0)
            humidity = round(max(15.0, min(99.0, humidity)), 1)
            
            # Wind speed in km/h
            wind_speed = round(max(0.5, random.gauss(12.0, 6.0)), 2)
            wind_direction = random.randint(0, 359)
            
            # Atmospheric pressure in hPa (standard is 1013.25)
            pressure = round(random.gauss(1012.0, 6.0), 1)
            
            # Precipitation probability based on humidity
            if humidity > 80 and random.random() < 0.4:
                precipitation = round(random.uniform(1.0, 25.0), 1)
                condition = "Heavy Rain" if precipitation > 10 else "Light Rain"
            elif humidity > 70 and random.random() < 0.25:
                precipitation = round(random.uniform(0.1, 3.0), 1)
                condition = "Partly Cloudy" if random.random() > 0.5 else "Overcast"
            elif temp > 38:
                precipitation = 0.0
                condition = "Clear" if humidity < 40 else "Hazy"
            elif wind_speed > 35:
                precipitation = round(random.uniform(2.0, 15.0), 1)
                condition = "Thunderstorm"
            else:
                precipitation = 0.0
                condition = random.choice(["Clear", "Clear", "Partly Cloudy"])

            heat_index = calculate_heat_index(temp, humidity)

            # Air Quality Index (proxy calculation for demo)
            aqi = int(max(20, min(500, (50 - temp*0.5 + humidity*0.8 + (1000 - pressure)*2 + random.randint(20, 120)))))
            if city in ["Delhi", "Kolkata"]:
                aqi = int(aqi * 1.6)

            records.append({
                "city": city,
                "latitude": profile["lat"],
                "longitude": profile["lon"],
                "timestamp": curr_time.strftime("%Y-%m-%d %H:%M:%S"),
                "temperature": temp,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "wind_direction": wind_direction,
                "pressure": pressure,
                "precipitation": precipitation,
                "weather_condition": condition,
                "heat_index": heat_index,
                "aqi": aqi,
                "collected_at": curr_time.isoformat()
            })

    return records

def save_to_csv(records, filepath="data/weather_data.csv"):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if not records:
        return
    fieldnames = list(records[0].keys())
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Generated {len(records)} realistic weather records saved to {filepath}")

if __name__ == "__main__":
    records = generate_records(num_days=30, readings_per_day=4)
    save_to_csv(records)
