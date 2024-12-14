import requests
import sqlite3
import time

# Yelp API credentials
CLIENT_ID = "9sUMQ5jQaevty0jmN12e7A"
API_KEY = "y3ntefkckj6UDXQ4E03Yb85PvD0QHjvLUVtM8kInsIHCEKQGRhhAChBYRgTQTyvJeN4hQ-wbmjgmdy1mePIchNXBUcrt6i2eDEaSbLBk1OUXqq-TJncU49SB_9dbZ3Yx"
BASE_URL = "https://api.yelp.com/v3/businesses/search"


# Setup Unified Database
def setup_db():
    conn = sqlite3.connect('AJSChicago_data.db')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS RestaurantPrice (
        price INTEGER PRIMARY KEY
        )
    ''')

    cursor.executemany('''
        INSERT OR IGNORE INTO RestaurantPrice (price) VALUES (?)
    ''', [(1,), (2,), (3,), (4,)])
    
    # Yelp Restaurants table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Restaurants (
            business_id TEXT PRIMARY KEY,
            name TEXT,
            location TEXT,
            rating REAL,
            restaurantPrice INTEGER,
            FOREIGN KEY (restaurantPrice) REFERENCES RestaurantPrice(price)
        )
    ''')
    
    conn.commit()
    conn.close()


def get_current_offset():
    try:
        with open("offset.txt", "r") as f:
            return int(f.read())
    except FileNotFoundError:
        return 0  # Start at offset 0 if no file exists
    
def save_offset(offset):
    with open("offset.txt", "w") as f:
        f.write(str(offset))

# Function to get restaurants from Yelp API
def get_yelp_restaurants(location, offset):
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    
    params = {
        "location": location,
        "offset": offset,
        "limit": 25
    }
    
    try:
        response = requests.get(BASE_URL, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json().get('businesses', {})
            return data
        else:
            print(f"Error: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def insert_data(businesses):
    # Convert the price in the api data to an integer value in the price table
    price_map = {"$": 1, "$$": 2, "$$$": 3, "$$$$": 4}
    conn = sqlite3.connect("AJSChicago_data.db")
    c = conn.cursor()
    
    for business in businesses:
        try:
            price_dollar = business.get("price", None)
            if price_dollar:
                price = price_map.get(business.get("price", None), None) 
                c.execute('''
                    INSERT OR IGNORE INTO Restaurants (business_id, name, location, rating, restaurantPrice)
                    VALUES (?, ?, ?, ?, ?)
                ''', (business["id"], business["name"], business["location"]["address1"], business["rating"], price))
            
        except Exception as e:
            print(f"Error inserting data: {e}")
    
    conn.commit()
    conn.close()

def main():
    location = "Chicago"
    
    # Create the database table
    setup_db()
    
    # Get the current offset (progress tracker)
    offset = get_current_offset()
    
    # Fetch data from the Yelp API
    businesses = get_yelp_restaurants(location, offset)
    
    # Insert the fetched data into the database
    if businesses:
        insert_data(businesses)
        print(f"Inserted {len(businesses)} new records into the database.")
        
        # Update the offset to fetch the next batch in the next run
        offset += len(businesses)
        save_offset(offset)
    else:
        print("No new data fetched.")

if __name__ == "__main__":
    main()
