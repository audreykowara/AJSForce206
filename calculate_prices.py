import sqlite3
import csv

def calculate_price_percentages():
    # Connect to the SQLite database
    conn = sqlite3.connect('AJSChicago_data.db')
    cursor = conn.cursor()

    # Query to get the count of restaurants per price range
    cursor.execute('''
    SELECT rp.price, COUNT(r.business_id) 
    FROM Restaurants r
    JOIN RestaurantPrice rp ON r.restaurantPrice = rp.price
    GROUP BY rp.price
    ORDER BY rp.price
    ''')

    # Fetch the results
    price_counts = cursor.fetchall()

    # Get the total number of restaurants
    cursor.execute('SELECT COUNT(*) FROM Restaurants')
    total_restaurants = cursor.fetchone()[0]

    if total_restaurants == 0:
        print("No restaurants found.")
        return

    # Write out the data to a CSV file
    with open('price_percentages.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Price Range", "Count", "Percentage"])  # Writing header row
        
        # Write data for each price range
        for price, count in price_counts:
            percentage = (count / total_restaurants) * 100
            writer.writerow([f"{price}", count, f"{percentage:.2f}%"])

    print(f"Data written to 'price_percentages.csv'.")

    conn.close()

if __name__ == "__main__":
    calculate_price_percentages()
