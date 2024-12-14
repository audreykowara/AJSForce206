import sqlite3
import csv
import matplotlib.pyplot as plt

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


#Function to visualize data
def visualize_data_from_csv(csv_file):
    "Create a dual -axis chart for average temperature and wind speed"

    cities = []
    avg_temps = []
    avg_wind_speeds = []

    # Read the CSV file
    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                cities.append(row[0])  # First column: City name
                avg_temps.append(float(row[1]))  # Second column: Avg Temperature
                avg_wind_speeds.append(float(row[2]))  # Third column: Avg Wind Speed

        # Create the visualization
        fig, ax1 = plt.subplots(figsize=(10, 6))

        # Bar graph for average temperature
        ax1.bar(cities, avg_temps, color='lightblue', alpha=0.7, label='Avg Temperature (°F)')
        ax1.set_xlabel("City", fontsize=12)
        ax1.set_ylabel("Avg Temperature (°F)", fontsize=12, color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.set_xticks(range(len(cities)))
        ax1.set_xticklabels(cities, rotation=45, ha='right', fontsize=10)

        # Line plot for average wind speed on a secondary y-axis
        ax2 = ax1.twinx()
        ax2.plot(cities, avg_wind_speeds, color='green', marker='o', label='Avg Wind Speed (MPH)', linewidth=2)
        ax2.set_ylabel("Avg Wind Speed (MPH)", fontsize=12, color='green')
        ax2.tick_params(axis='y', labelcolor='green')

        # Add legends for both axes
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


if __name__ == "__main__":
    # Path to the CSV file containing the data
    csv_file = "city_weather_averages.csv"  # Replace with your CSV file name

    # Visualize the data from the CSV file
    visualize_data_from_csv(csv_file)
