import csv
import matplotlib.pyplot as plt

def create_pie_chart_from_csv(csv_file):
    # Read the CSV file with price range data
    price_ranges = []
    percentages = []

    # Open and read the CSV file
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            price_ranges.append(int(row[0]) * '\$')  # Get the price range (e.g., "1")
            percentages.append(float(row[2].strip('%')))  # Get the percentage and remove the '%' symbol

    # Create a pie chart
    plt.figure(figsize=(7, 7))
    plt.pie(percentages, labels=price_ranges, autopct='%1.2f%%', startangle=140)
    plt.title("Percentage of Restaurants by Price Range")
    plt.show()

if __name__ == "__main__":
    # Path to the CSV file containing the data
    csv_file = 'price_percentages.csv'
    
    # Create and display the pie chart from the CSV data
    create_pie_chart_from_csv(csv_file)
