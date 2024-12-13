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


if __name__ == "__main__":
    setup_database()






