import sqlite3
import csv
import matplotlib.pyplot as plt
import numpy as np

# get iternary data and write to a csv file
def write_processed_data_to_csv():
    conn = sqlite3.connect('AJSChicago_data.db')
    cursor = conn.cursor()

    # SQL query to get average rating and count of attractions by category
    cursor.execute('''
    SELECT category, AVG(rating) AS avg_rating, COUNT(*) AS attraction_count
    FROM Itinerary
    GROUP BY category;
    ''')

    # open the file ---> write  data
    with open('places_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Category", "Average Rating", "Attraction Count"])  # headers

        # write each row
        for row in cursor.fetchall():
            category, avg_rating, attraction_count = row
            
            # if rating is none --> 0
            avg_rating = avg_rating if avg_rating is not None else 0.00
            writer.writerow([category, f"{avg_rating:.2f}", attraction_count])

    conn.close()
    print("Processed data has been written to 'places_data.csv'.")

write_processed_data_to_csv()


