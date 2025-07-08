import requests
import pyodbc
from datetime import datetime, timedelta
import time

# 1. Define cities and coordinates
CITIES = {
    "London": {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Sydney": {"lat": -33.8688, "lon": 151.2093},
    "Lahore": {"lat": 31.5204, "lon": 74.3587},
    "Dubai": {"lat": 25.276987, "lon": 55.296249}
}

# 2. Connect to SQL Server
sql_connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=WeatherDataWarehouse;'
    'Trusted_Connection=yes;'
)

# 3. Process each city
for city, coords in CITIES.items():
    # 4. Get last recorded date
    cursor = sql_connection.cursor()
    cursor.execute("SELECT MAX(date) FROM fact_weather WHERE city_id = (SELECT city_id FROM dim_city WHERE city_name = ?)", city)
    last_date = cursor.fetchone()[0] or datetime(2000, 1, 1).date()
    
    # 5. Calculate date range
    start_date = last_date + timedelta(days=1)
    end_date = datetime.now().date()
    if start_date > end_date:
        continue
    
    # 6. Fetch weather data from API with retry
    max_retries = 3
    retry_delay = 60  # seconds
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={coords['lat']}&longitude={coords['lon']}&start_date={start_date}&end_date={end_date}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
    daily_data = None

    for attempt in range(max_retries):
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'daily' in data:
                daily_data = data['daily']
                print(f"Data fetched for {city} successfully")
                break  # Exit retry loop on success
        print(f"Attempt {attempt+1} failed for {city}. Retrying in 60 seconds...")
        time.sleep(retry_delay)
    else:  # Executes if loop completes without break
        print(f"ERROR for {city}: Failed after {max_retries} attempts")
        continue  # ⭐ CITY LOOP ENDS HERE FOR FAILED CITIES ⭐
    
    # 7. Insert into staging table ⭐ INSERTION IS ONLY FOR SUCCESSFUL API CALLS ⭐
    for i in range(len(daily_data['time'])):
        cursor.execute("""
            INSERT INTO stg_weather_raw (city_name, date, temp_max, temp_min, precipitation)
            VALUES (?, ?, ?, ?, ?)
        """, city, 
           daily_data['time'][i],
           daily_data['temperature_2m_max'][i],
           daily_data['temperature_2m_min'][i],
           daily_data['precipitation_sum'][i])
    
    sql_connection.commit()
    print(f"Inserted {len(daily_data['time'])} records for {city}")

# 8. Clean up
sql_connection.close()