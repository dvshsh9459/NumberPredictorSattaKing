import scrapy
import json
import psycopg2
from psycopg2 import sql
from datetime import datetime

def safe_int(value):
    try:
        return int(value)
    except ValueError:
        # If the value is not a valid integer, set it to -1
        print(f"Invalid integer value: {value}. Setting to -1.")
        return -1


def createDbConnection():
    try:
        # Define connection parameters
        connection = psycopg2.connect(
            user="postgres",
            password="root",
            host="localhost",        # For local connections, use "localhost"
            port="5432",        # The default PostgreSQL port is 5432
            database="satta-king" # The name of your database
        )

        # Create a cursor object using the connection
        cursor = connection.cursor()

        # Execute a sample query
        cursor.execute("SELECT version();")
        # Fetch the result
        db_version = cursor.fetchone()
        print(f"Connected to - {db_version}")
        return connection

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")



# Define the function to insert data
def insert_data(data):
    try:
        connection = createDbConnection()
        cursor = connection.cursor()
        # SQL statement for inserting data into the partitioned table
        insert_query = sql.SQL("""
            INSERT INTO satta_king_metrics (date, dswr, frbd, gzbd, gali, month, year)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """)
        # Loop over each record in the JSON data and insert it
        for record in data:
            # Convert the date string to a PostgreSQL-compatible date format (YYYY-MM-DD)
            formatted_date = record["date"]

            # Insert the data into the table
            cursor.execute(insert_query, (
                formatted_date,  # Date
                safe_int(record["DSWR"]),  # DSWR
                safe_int(record["FRBD"]),  # FRBD
                safe_int(record["GZBD"]),  # GZBD
                safe_int(record["GALI"]),  # GALI
                (record["month"]),  # Month
                (record["year"])  # Year (this determines the partition)
            ))

        # Commit the transaction
        connection.commit()
        print(f"Data inserted successfully into partitioned table.")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while inserting data: {error}")

    finally:
        # Close the connection and cursor
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed.")




class SattaSpider(scrapy.Spider):
    name = "satta_king_crawler"

    # Base URL for the form submission
    start_urls = ['https://satta-king-fast.com/']  # Replace with the actual URL of the form
                                                    #https://satta-king-fast.com/chart.php?month=01&year=2015
    def start_requests(self):
        # Loop through all months and years
        months = [str(month).zfill(2) for month in range(1, 13)]  # ['01', '02', ..., '12']
        years = [str(year) for year in range(2015, 2025)]  # ['2024', '2023', ..., '2015']

        for month in months:
            for year in years:
                yield scrapy.FormRequest(
                    url=self.start_urls[0]+"/chart.php?month="+month+"&year="+year,
                    formdata={'month': month, 'year': year},
                    callback=self.parse,
                    meta={'month': month, 'year': year}  # Pass month and year for logging
                )

    def parse(self, response):
        # Log the month and year being processed
        month = response.meta['month']
        year = response.meta['year']
        # self.log(f'Parsing data for month: {month}, year: {year}')

        # Log the full response text for debugging
        # self.log(
        #     f'Response received for month {month} year {year}: {response.text[:2000]}')  # Log first 2000 characters

        data = []
        table_rows = response.css('div#mix-chart table.chart-table tr.day-number')

        for row in table_rows:
            date = row.css('td.day::attr(title)').get()  # Get the full date and split by comma to get the date part
            dswr = row.css('td.number:nth-child(2)::text').get()
            frbd = row.css('td.number:nth-child(3)::text').get()
            gzbd = row.css('td.number:nth-child(4)::text').get()
            gali = row.css('td.number:nth-child(5)::text').get()
            # Store the data in a dictionary
            date_value = datetime.strptime(date, "%B %d, %Y")
            data.append({
                'date': date,
                'DSWR': dswr,
                'FRBD': frbd,
                'GZBD': gzbd,
                'GALI': gali,
                'month': month,
                'year': year
            })

        insert_data(data)
        # Save the data to a JSON file
        # with open(f'satta_results_{month}_{year}.json', 'w') as f:
        #     json.dump(data, f, indent=4)
        #

        # self.log(f'Data saved to satta_results_{month}_{year}.json')

