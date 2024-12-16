import requests
import sqlite3
import time
import os

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


# get the current pagination token
def get_current_page_token():
    if os.path.exists("page_token.txt"): # page_token stores the token from the previous API call
        with open("page_token.txt", "r") as f: # checks for existence of page_token file
            return f.read().strip()
    return None


# save the pagination token
def save_page_token(page_token):
    with open("page_token.txt", "w") as f: # can continue from where last run during next execution
        f.write(page_token)


# get data from Google Places API
def get_google_places_data(api_key, location="Chicago", page_token=None):
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query=activities+in+{location}&key={api_key}"

    if page_token:
        url += f"&pagetoken={page_token}" # dynamically construct url for different search entries

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
    data_to_store = []

    entries_inserted = 0  # initialize counter

    for place in data.get('results', []):
        if entries_inserted >= remaining_entries:  # stop if 25 limit reached
            break

        attraction = place['name']
        category = place['types'][0] if place['types'] else None
        location = place.get('formatted_address', None)
        rating = place.get('rating', None)
        review_count = place.get('user_ratings_total', 0)

        try: # insert or ignore avoids duplicates
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
    max_total_entries = 25 # ensure each run has a max of 25 entries
    
    for query in queries:
        print(f"Fetching data for query: '{query}'")
        page_token = None
        
        while total_entries_inserted < max_total_entries:  # continue until max of 25 entries
            remaining_entries = max_total_entries - total_entries_inserted
            data = get_google_places_data(API_KEY, query, page_token)
            
            if data:
                new_data = store_data_in_db(data, remaining_entries)
                total_entries_inserted += len(new_data) # increment data by # of new entries

                if total_entries_inserted >= max_total_entries: # stop fetching after 25 have been inserted
                    print("Max total entries reached. Stopping.")
                    return
                
                # check for the next page token
                page_token = data.get('next_page_token')
                if page_token:
                    print("Next page token found. Continuing to next page...")
                    time.sleep(2)  # short delay between paginated requests
                else:
                    break # no more pages --> cant get more entries in db 
            else:
                print(f"Error retrieving data for query: '{query}'")
                break # stop if there are errors

    print(f"Finished. Total entries inserted: {total_entries_inserted}")

if __name__ == "__main__":
    main()
