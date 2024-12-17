import requests
import sqlite3
import time

# Google Places API key
API_KEY = 'AIzaSyB--RDZw6LYLq0-23rngVX0cUt9idBObv4'

def setup_db():
    conn = sqlite3.connect('AJSChicago_data.db')
    cursor = conn.cursor()

    # create table called itinerary for possible places to visit in Chicago
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

# get data from Google Places API
def get_google_places_data(api_key, location="Chicago", page_token=None):
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query=activities+in+{location}&key={api_key}"

    if page_token:
        url += f"&pagetoken={page_token}"  # dynamically construct url for different search entries

    try:
        response = requests.get(url)
        response.raise_for_status()  # raises HTTPError for bad responses

        data = response.json()
        return data

    except requests.exceptions.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return None

def store_data_in_db(data, remaining_entries):
    conn = sqlite3.connect('AJSChicago_data.db')
    cursor = conn.cursor()
    entries_inserted = 0  # initialize counter 

    for place in data.get('results', []):
        if entries_inserted >= remaining_entries:
            break  # stop if meet remaining # of entries

        attraction = place['name']
        category = place['types'][0] if place['types'] else None
        location = place.get('formatted_address', None)
        rating = place.get('rating', None)
        review_count = place.get('user_ratings_total', 0)

        try:  # insert or ignore avoids duplicates
            cursor.execute('''
                INSERT OR IGNORE INTO Itinerary (attraction, category, rating, review_count, location)
                VALUES (?, ?, ?, ?, ?)
            ''', (attraction, category, rating, review_count, location))

            # check if insertion ---> successful
            if cursor.rowcount > 0:  # if row was inserted
                entries_inserted += 1

        except sqlite3.Error as e:
            print(f"Database error: {e}")

    conn.commit()
    conn.close()
    print(f"Inserted {entries_inserted} new records into the database.")
    return entries_inserted  # num of new entries inserted

def main():
    setup_db()
    queries = [
        "tourist attractions in Chicago",
        "museums in Chicago",
        "parks in Chicago",
        "historical sites in Chicago",
        "art galleries in Chicago",
        "family activities in Chicago",
        "outdoor activities in Chicago",
        "restaurants in Chicago",  
        "shopping malls in Chicago",  
        "nightlife in Chicago",  
        "beaches in Chicago",  
        "famous landmarks in Chicago", 
        "zoo in Chicago",
        "tourist attractions around Chicago",
        "museums around Chicago",
        "parks around Chicago",
        "historical sites around Chicago",
        "art galleries around Chicago",
        "family activities around Chicago",
        "outdoor activities around Chicago",
        "restaurants around Chicago",  
        "shopping malls around Chicago",  
        "nightlife around Chicago",  
        "beaches around Chicago",  
        "famous landmarks around Chicago", 
        "zoo around Chicago"
    ]

    total_entries_inserted = 0
    max_total_entries = 25  # ensure each run has max of 25 entries

    for query in queries:
        if total_entries_inserted >= max_total_entries:
            print("Max total entries reached. Stopping.")
            break  # stop if the max # of entries  reached

        print(f"Fetching data for query: '{query}'")
        page_token = None

        while total_entries_inserted < max_total_entries:  # continue until max of 25 entries
            remaining_entries = max_total_entries - total_entries_inserted
            data = get_google_places_data(API_KEY , query, page_token)

            if data:
                new_entries = store_data_in_db(data, remaining_entries)
                total_entries_inserted += new_entries  # increment total entries by new entries

                if total_entries_inserted >= max_total_entries:  # stop fetching after reaching the limit
                    print("Max total entries reached. Stopping.")
                    break

                # check for the next page token
                page_token = data.get('next_page_token')
                if page_token:
                    print("Next page token found. Continuing to next page...")
                    time.sleep(2)  # short delay between paginated requests
                else:
                    break  # no more pages --> can't get more entries in db
            else:
                print(f"Error retrieving data for query: '{query}'")
                break  # stop if there are errors

    print(f"Finished. Total entries inserted: {total_entries_inserted}")

if __name__ == "__main__":
    main()
