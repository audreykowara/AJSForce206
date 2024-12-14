import sqlite3
import csv

# Function to calculate the average temperature and average windspeed of each city
def calculate_average_weather():
    """Calculate the average temperature and wind speed for each city and save to a CSV file."""
    try:
        conn = sqlite3.connect("AJSChicago_data.db")
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

        # Write the results to a CSV file
        output_file = "city_weather_averages.csv"
        with open(output_file, "w", newline="") as file:
            writer = csv.writer(file)
            # Write the header
            writer.writerow(["City Name", "Avg Temperature (°F)", "Avg Wind Speed (MPH)"])
            # Write each row
            writer.writerows(results)

        print(f"City averages (temperature and wind speed) have been written to {output_file}.")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    """Main function to handle the program logic."""
    # Example logic; replace with your actual logic
    # This is a placeholder for your main function logic.
    try:
        # You would fetch and store weather data here
        # For now, we'll call the calculate_average_weather function
        calculate_average_weather()
    except Exception as e:
        print(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
