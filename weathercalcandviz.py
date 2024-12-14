import requests
import sqlite3
import csv
import time
import matplotlib.pyplot as plt

# WeatherAPI API Key and Base URL
BASE_URL = "http://api.weatherapi.com/v1/forecast.json"
API_KEY = "8a5c7cf8930644dab7414532241412"

# Database setup
DB_NAME = "AJSChicago_data.db"

# Set up the SQLite database with required tables
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
            FOREIGN KEY (city_id) REFERENCES cities (city_id),
            UNIQUE(city_id, date)  -- Prevent duplicate city/date rows
        )
    ''')

    conn.commit()
    conn.close()

# Fetch weather data from WeatherAPI
def fetch_weather_data(api_key, city_name, start_day=0, days_per_run=7):
    params = {
        "key": api_key,
        "q": city_name,
        "days": days_per_run,
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching data from WeatherAPI: {e}")
    
    if "forecast" not in data or "forecastday" not in data["forecast"]:
        raise ValueError(f"Invalid response format: {data}")

    forecast_days = data["forecast"]["forecastday"]
    collected_data = []

    for forecast in forecast_days:
        weather_entry = {
            "city_name": city_name,
            "temperature": forecast["day"]["maxtemp_f"],
            "condition": forecast["day"]["condition"]["text"],
            "wind_speed": forecast["day"]["maxwind_mph"],
            "humidity": forecast["day"]["avghumidity"],
            "date": forecast["date"]
        }
        collected_data.append(weather_entry)

    return collected_data

# Store weather data in SQLite database
def store_weather_data(data, max_rows=25, current_total_rows=0):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    rows_added = 0
    for entry in data:
        if current_total_rows + rows_added >= max_rows:
            break

        cursor.execute("INSERT OR IGNORE INTO cities (city_name) VALUES (?)", (entry['city_name'],))
        city_id = cursor.execute("SELECT city_id FROM cities WHERE city_name = ?", (entry['city_name'],)).fetchone()[0]

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

# Calculate average weather data and save to a CSV
def calculate_average_weather():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        query = '''
            SELECT 
                c.city_name,
                ROUND(AVG(w.temperature), 2) AS avg_temperature,
                ROUND(AVG(w.wind_speed), 2) AS avg_wind_speed
            FROM 
                weather w
            INNER JOIN 
                cities c ON w.city_id = c.city_id
            GROUP BY 
                c.city_name
            ORDER BY 
                c.city_name;
        '''

        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()

        output_file = "city_weather_averages.csv"
        with open(output_file, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["City Name", "Avg Temperature (°F)", "Avg Wind Speed (MPH)"])
            writer.writerows(results)

        print(f"City averages written to {output_file}.")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Visualize data from CSV
def visualize_data_from_csv(csv_file):
    cities = []
    avg_temps = []
    avg_wind_speeds = []

    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                cities.append(row[0])
                avg_temps.append(float(row[1]))
                avg_wind_speeds.append(float(row[2]))

        fig, ax1 = plt.subplots(figsize=(12, 7))

        ax1.bar(cities, avg_temps, color='skyblue', alpha=0.7, label='Avg Temperature (°F)')
        ax1.set_xlabel("City", fontsize=12)
        ax1.set_ylabel("Avg Temperature (°F)", fontsize=12, color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.set_xticks(range(len(cities)))
        ax1.set_xticklabels(cities, rotation=45, ha='right', fontsize=10)

        ax2 = ax1.twinx()
        ax2.plot(cities, avg_wind_speeds, color='limegreen', marker='o', label='Avg Wind Speed (MPH)', linewidth=2)
        ax2.set_ylabel("Avg Wind Speed (MPH)", fontsize=12, color='green')
        ax2.tick_params(axis='y', labelcolor='green')

        ax1.legend(loc="upper left", fontsize=10)
        ax2.legend(loc="upper right", fontsize=10)

        plt.title("Average Temperature and Wind Speed by City", fontsize=16)
        plt.tight_layout()
        plt.savefig("dual_axis_chart.png")
        plt.show()

    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Main function
def main():
    setup_database()

    DAYS_PER_RUN = 7
    START_DAY = 0
    CITIES = ["Chicago", "Ann Arbor", "New Orleans", "New York", "Boston", "Los Angeles", 
              "San Francisco", "San Diego", "Las Vegas", "Houston", "Miami", "Orlando", "Utah", "Aspen", "Hawaii"]
    MAX_ROWS = 25

    total_rows = 0

    for city in CITIES:
        if total_rows >= MAX_ROWS:
            break

        try:
            weather_data = fetch_weather_data(API_KEY, city, start_day=START_DAY, days_per_run=DAYS_PER_RUN)
            weather_data = weather_data[:MAX_ROWS - total_rows]
            rows_added = store_weather_data(weather_data, max_rows=MAX_ROWS, current_total_rows=total_rows)
            total_rows += rows_added
        except Exception as e:
            print(f"Couldn't fetch or store data for {city}. Error: {e}")
        time.sleep(1)

    calculate_average_weather()
    visualize_data_from_csv("city_weather_averages.csv")

if __name__ == "__main__":
    main()
