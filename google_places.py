import requests
import sqlite3
import time
import os

# Google Places API key
API_KEY = 'AIzaSyB--RDZw6LYLq0-23rngVX0cUt9idBObv4'

# Database setup function
def setup_db():
    conn = sqlite3.connect('AJSChicago_data.db')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Itinerary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        attraction TEXT UNIQUE,
        category TEXT,
        rating REAL,
        review_count INTEGER,
        location TEXT
    )
    ''')

    conn.commit()
    conn.close()


# Function to get the current pagination token
def get_current_page_token():
    if os.path.exists("page_token.txt"):
        with open("page_token.txt", "r") as f:
            return f.read().strip()
    return None


# Function to save the pagination token
def save_page_token(page_token):
    with open("page_token.txt", "w") as f:
        f.write(page_token)


# Function to get data from Google Places API
def get_google_places_data(api_key, location="Chicago", page_token=None):
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query=activities+in+{location}&key={api_key}"

    if page_token:
        url += f"&pagetoken={page_token}"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses

        data = response.json()
        return data

    except requests.exceptions.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return None


# Function to store data in the database
def store_data_in_db(data):
    conn = sqlite3.connect('AJSChicago_data.db')
    cursor = conn.cursor()
    data_to_store = []

    entries_inserted = 0  # initialize counter
    max_entries = 25

    for place in data.get('results', []):
        if entries_inserted >= max_entries:  # Limit to 25 entries per run
            break

        attraction = place['name']
        category = place['types'][0] if place['types'] else None
        location = place.get('formatted_address', None)
        rating = place.get('rating', None)
        review_count = place.get('user_ratings_total', 0)

        try:
            cursor.execute('''
                INSERT OR IGNORE INTO Itinerary (attraction, category, rating, review_count, location)
                VALUES (?, ?, ?, ?, ?)
            ''', (attraction, category, rating, review_count, location))

            entries_inserted += 1

            data_to_store.append({
                "attraction": attraction,
                "category": category,
                "rating": rating,
                "review_count": review_count,
                "location": location
            })

        except sqlite3.Error as e:
            print(f"Database error: {e}")

    conn.commit()
    conn.close()
    print(f"Inserted {len(data_to_store)} new records into the database.")
    return data_to_store


# Main function to manage the process
def main():
    setup_db()
    queries = [
        "tourist attractions in Chicago",
        "museums in Chicago",
        "parks in Chicago",
        "historical sites in Chicago",
        "art galleries in Chicago",
        "family activities in Chicago",
        "outdoor activities in Chicago"
    ]

    total_entries_inserted = 0
    max_total_entries = 25 
    
    for query in queries:
        print(f"Fetching data for query: '{query}'")
        page_token = None
        
        while True:
            data = get_google_places_data(API_KEY, query, page_token)
            
            if data:
                new_data = store_data_in_db(data)
                
                # Check for the next page token
                page_token = data.get('next_page_token')
                if page_token:
                    print("Next page token found. Continuing to next page...")
                    time.sleep(2)  # Google requires a short delay between paginated requests
                else:
                    break
            else:
                print(f"Error retrieving data for query: '{query}'")
                break



    # location = "Chicago"
    # page_token = get_current_page_token()

    # # Fetch data from Google Places API
    # data = get_google_places_data(API_KEY, location, page_token)

    # if data:
    #     # Store the fetched data in the database
    #     new_data = store_data_in_db(data)

    #     # Check for the next page token
    #     next_page_token = data.get('next_page_token')
    #     if next_page_token:
    #         save_page_token(next_page_token)
    #         print("Next page token saved. Run the script again to fetch the next batch.")
    #     else:
    #         print("No more pages available.")
    #         if os.path.exists("page_token.txt"):
    #             os.remove("page_token.txt")  # Clear the token file when done
    # else:
    #     print("Error retrieving data.")


if __name__ == "__main__":
    main()
