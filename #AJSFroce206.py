#AJSFroce206

import requests
import sqlite3
import time

#AccuWeather API Key and Base URL
BASE_URL = "http://dataservice.accuweather.com"
API_KEY = "oJGUem8EoAghqOIxrx5QctTjCMnPivOV"

#Database setup
DB_NAME = "AJSChicago_data.db"

def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create cities table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT UNIQUE
        )
    ''')
    # Create weather table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER,
            temperature REAL,
            condition TEXT,
            wind_speed REAL,
            humidity INTEGER,
            date TEXT,
            FOREIGN KEY (city_id) REFERENCES cities (city_id)
        )
    ''')
    conn.commit()
    conn.close()

#Function to fetch weather data
def fetch_weather_data(API_KEY, CITY_NAME, total_days):
     # Fetch city ID
    location_url = f"{BASE_URL}/locations/v1/cities/search"
    params = {"apikey": API_KEY, "q": CITY_NAME}
    response = requests.get(location_url, params=params).json()
    city_id = response[0]['Key']  # Extract city ID

    # Initialize variables
    collected_data = []
    forecast_url = f"{BASE_URL}/forecasts/v1/daily/5day/{city_id}"  # AccuWeather supports 5-day chunks
    forecast_params = {"apikey": API_KEY}
    days_fetched = 0

    # Loop to fetch data in chunks of 5 days until we meet the required number of days
    while days_fetched < total_days:
        # Fetch 5-day forecast data
        forecast_response = requests.get(forecast_url, params=forecast_params).json()
        daily_forecasts = forecast_response['DailyForecasts']

        # Parse the data for required fields
        for forecast in daily_forecasts:
            if days_fetched >= total_days:  # Stop once we reach the required total_days
                break
            weather_entry = {
                "city_name": CITY_NAME,
                "temperature": forecast['Temperature']['Maximum']['Value'],
                "weather_condition": forecast['Day']['IconPhrase'],
                "wind_speed": forecast['Day']['Wind']['Speed']['Value'],
                "humidity": forecast['Day'].get('RainProbability', None),
                "date": forecast['Date']
            }
            collected_data.append(weather_entry)
            days_fetched += 1

    return collected_data  # Return the parsed data

#Function to store weather data in the database
def store_weather_data(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for entry in data:
        cursor.execute("INSERT OR IGNORE INTO cities (city_name) VALUES (?)", (entry['city_name'],))        #Insert the city into 'cities' table, avoiding duplicates
        city_id = cursor.execute("SELECT city_id FROM cities WHERE city_name = ?", (entry['city_name'],)).fetchone()[0]     #Retrieve the city_id for the inserted or existing city

        # Insert the weather data into the `weather` table
        cursor.execute('''
            INSERT INTO weather (city_id, temperature, condition, wind_speed, humidity, date)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            city_id,
            entry['temperature'],         # Maximum temperature (°F)
            entry['weather_condition'],   # Weather condition (e.g., sunny, snowy)
            entry['wind_speed'],          # Wind speed
            entry['humidity'],            # Humidity (or rain probability)
            entry['date']                 # Date of forecast
        ))

    conn.commit()
    conn.close()



if __name__ == "__main__":
    setup_database()
    TOTAL_DAYS = 25  # Fetch forecast for 25 days
    CITY_NAME = "Chicago"

    # Fetch weather data
    try:
        print(f"Fetching weather data for {CITY_NAME}...")
        weather_data = fetch_weather_data(API_KEY, CITY_NAME, TOTAL_DAYS)
        print("Weather data successfully fetched!")
    except Exception as e:
        print("Couldn't fetch data.")
        print(f"Error: {e}")
        weather_data = None

    # Store weather data in the database
    if weather_data:
        try:
            print("Storing weather data in the database...")
            store_weather_data(weather_data)
            print("Weather data successfully stored in the database!")
        except Exception as e:
            print("Couldn't store data.")
            print(f"Error: {e}")





