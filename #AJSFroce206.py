import requests
import sqlite3
import time
import datetime

# WeatherAPI API Key and Base URL
BASE_URL = "http://api.weatherapi.com/v1/forecast.json"
API_KEY = "8a5c7cf8930644dab7414532241412"

# Database setup
DB_NAME = "AJSChicago_data.db"

def setup_database():
    """Set up the SQLite database with required tables."""
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
            FOREIGN KEY (city_id) REFERENCES cities (city_id),
            UNIQUE(city_id, date)  -- Prevent duplicate city/date rows
        )
    ''')

    conn.commit()
    conn.close()

def fetch_weather_data(api_key, city_name, start_day=0, days_per_run=7):
    """Fetch weather data starting from a specific day."""
    params = {
        "key": api_key,
        "q": city_name,
        "days": days_per_run,
    }

    # Fetch data
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)
        data = response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching data from WeatherAPI: {e}")
    
    # Parse the data for required fields
    if "forecast" not in data or "forecastday" not in data["forecast"]:
        raise ValueError(f"Invalid response format: {data}")

    forecast_days = data["forecast"]["forecastday"]
    collected_data = []

    for forecast in forecast_days:
        weather_entry = {
            "city_name": city_name,
            "temperature": forecast["day"]["maxtemp_f"],  # Max temperature in Fahrenheit
            "condition": forecast["day"]["condition"]["text"],  # Weather condition
            "wind_speed": forecast["day"]["maxwind_mph"],  # Max wind speed in MPH
            "humidity": forecast["day"]["avghumidity"],  # Average humidity
            "date": forecast["date"]  # Date of forecast
        }
        collected_data.append(weather_entry)

    return collected_data

def store_weather_data(data, max_rows=25, current_total_rows=0):
    """Store weather data in the SQLite database with row limit enforcement."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    rows_added = 0  # Track rows added for this run
    for entry in data:
        if current_total_rows + rows_added >= max_rows:
            break  # Stop if adding another row exceeds the limit

        # Insert city into 'cities' table, avoiding duplicates
        cursor.execute("INSERT OR IGNORE INTO cities (city_name) VALUES (?)", (entry['city_name'],))
        city_id = cursor.execute("SELECT city_id FROM cities WHERE city_name = ?", (entry['city_name'],)).fetchone()[0]

        # Insert weather data if not already present
        try:
            cursor.execute('''
                INSERT INTO weather (city_id, temperature, condition, wind_speed, humidity, date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                city_id,
                entry['temperature'],
                entry['condition'],
                entry['wind_speed'],
                entry['humidity'],
                entry['date']
            ))
            rows_added += 1
        except sqlite3.IntegrityError:
            print(f"Skipping duplicate entry for city_id={city_id}, date={entry['date']}")

    conn.commit()
    conn.close()

    return rows_added

def verify_database():
    """Verify database rows and check for duplicates."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Total row count
    total_rows = cursor.execute("SELECT COUNT(*) FROM weather").fetchone()[0]
    print(f"Total rows in database: {total_rows}")

    # Check for duplicates
    duplicates = cursor.execute('''
        SELECT city_id, date, COUNT(*)
        FROM weather
        GROUP BY city_id, date
        HAVING COUNT(*) > 1
    ''').fetchall()

    if duplicates:
        print(f"Found duplicates: {duplicates}")
    else:
        print("No duplicates found!")

    conn.close()

def main():
    """Main function to fetch and store weather data."""
    setup_database()
    DAYS_PER_RUN = 7  # Fetch up to 7 days of data per city
    START_DAY = 0  # Adjust this for each run (e.g., 0 for 1st run, 7 for 2nd run, and so on)
    CITIES = ["Chicago", "Ann Arbor", "Upper Peninsula", "New Orleans", "New York", "Boston", "Los Angeles", 
              "San Francisco", "San Diego", "Las Vegas", "Houston", "Miami", "Orlando", "Utah", "Aspen"]  # Add more cities if needed
    MAX_ROWS = 25  # Limit rows to 25 per run

    total_rows = 0  # Track rows added in this run

    for city in CITIES:
        if total_rows >= MAX_ROWS:
            print(f"Reached the maximum of {MAX_ROWS} rows for this run. Stopping...")
            break

        try:
            print(f"Fetching weather data for {city}, starting from day {START_DAY}...")
            weather_data = fetch_weather_data(API_KEY, city, start_day=START_DAY, days_per_run=DAYS_PER_RUN)

            # Limit rows fetched to avoid exceeding the limit
            weather_data = weather_data[:MAX_ROWS - total_rows]  # Trim rows if necessary
            print(f"Weather data for {city} successfully fetched!")

            # Store weather data in the database
            if weather_data:
                rows_added = store_weather_data(weather_data, max_rows=MAX_ROWS, current_total_rows=total_rows)
                total_rows += rows_added
                print(f"Stored {rows_added} rows for {city}. Total rows this run: {total_rows}")
        except Exception as e:
            print(f"Couldn't fetch or store data for {city}. Error: {e}")
            continue

        # Pause to avoid API rate limits
        time.sleep(1)

    # Verify database after execution
    verify_database()

if __name__ == "__main__":
    main()
