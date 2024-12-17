import sqlite3
import csv
import matplotlib.pyplot as plt

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



def plot_data():
    conn = sqlite3.connect('AJSChicago_data.db')
    cursor = conn.cursor()

    # get attractions avg rating + count by category
    cursor.execute('''
    SELECT category, AVG(rating) AS avg_rating, COUNT(*) AS attraction_count
    FROM Itinerary
    GROUP BY category;
    ''')

    categories = []
    avg_ratings = []
    attraction_counts = []

    # get results --> organize them into lists
    for row in cursor.fetchall():
        category, avg_rating, attraction_count = row
        categories.append(category)
        avg_ratings.append(avg_rating if avg_rating is not None else 0.00)  # Handle None for ratings
        attraction_counts.append(attraction_count)

    conn.close()

    # figure + axes set up
    fig, ax1 = plt.subplots(figsize=(12, 6))  # width increased --> better spacing

    # bar chart ---> Average Rating ---> left axis
    color = '#A9BCFF'  # periwinkle --> bars
    ax1.set_xlabel('Category')
    ax1.set_ylabel('Average Rating', color=color)
    ax1.bar(categories, avg_ratings, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    # second y-axis ---> Attraction Count
    ax2 = ax1.twinx()
    color = '#7070FF'  # dark vibrant purple --> line
    ax2.set_ylabel('Attraction Count', color=color)
    ax2.plot(categories, attraction_counts, color=color, marker='o', label='Attraction Count', linestyle='--')
    ax2.tick_params(axis='y', labelcolor=color)

    # title
    plt.title('Average Rating and Attraction Count by Category')

    ax1.set_xticks(range(len(categories)))  # set x labels to range of categories
    ax1.set_xticklabels(categories, rotation=45, ha="right")  # rotate x labels for readability

    # adjust layout ---> prevent overlap of x axis labels
    plt.subplots_adjust(bottom=0.2)  # bottom margin

    # display plot
    fig.tight_layout()
    plt.show()

plot_data()