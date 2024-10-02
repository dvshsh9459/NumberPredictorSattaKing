import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA

import psycopg2
import pandas as pd
PREDICTOR_FOR_DSWR = 'DSWR'
PREDICTOR_FOR_FRBD = 'FRBD'
PREDICTOR_FOR_GALI = 'GALI'
PREDICTOR_FOR_GZBD = 'GZBD'


def getdataFromMetrics():
    # Step 1: Connect to PostgreSQL database
    conn = psycopg2.connect(
        user="postgres",
        password="root",
        host="localhost",  # For local connections, use "localhost"
        port="5432",  # The default PostgreSQL port is 5432
        database="satta-king"  # The name of your database
    )

    # Step 2: Create a cursor to execute queries
    cursor = conn.cursor()

    # Step 3: Execute SQL query to pull the data
    query = """
        SELECT date, DSWR, FRBD, GZBD, GALI, TO_CHAR(date, 'MM') AS month, TO_CHAR(date, 'YYYY') AS year
        FROM satta_king_metrics;
    """
    cursor.execute(query)

    # Step 4: Fetch all the rows
    rows = cursor.fetchall()

    # Step 5: Convert the fetched data to a list of dictionaries
    data = []
    for row in rows:
        data.append({
            "date": row[0].strftime('%B %d, %Y'),  # Convert date to string in "Month Day, Year" format
            "DSWR": row[1],
            "FRBD": row[2],
            "GZBD": row[3],
            "GALI": row[4],
            "month": row[5],
            "year": row[6]
        })

    # Step 6: Close the connection
    cursor.close()
    conn.close()
    return data

def insertPredictedValues(data):
    # Step 1: Connect to PostgreSQL database
    conn = psycopg2.connect(
        user="postgres",
        password="root",
        host="localhost",  # For local connections, use "localhost"
        port="5432",  # The default PostgreSQL port is 5432
        database="satta-king"  # The name of your database
    )

    # Step 2: Create a cursor to execute queries
    cursor = conn.cursor()

    # Step 1: Create a DataFrame
    df = pd.DataFrame(data)

    # Step 2: Convert the 'date' field to a datetime object
    df['date'] = pd.to_datetime(df['date'], format='%B %d, %Y')

    # Step 3: Set the 'date' column as the index for time series analysis
    df.set_index('date', inplace=True)

    # Option 1: Infer frequency automatically
    df = df.asfreq(pd.infer_freq(df.index))

    # Step 4: Extract the 'DSWR' column as the target for prediction
    dswr_series = df[PREDICTOR_FOR_DSWR]
    # Step 4: Extract the 'FRBD' column as the target for prediction
    frbd_series = df[PREDICTOR_FOR_FRBD]
    # Step 4: Extract the 'GALI' column as the target for prediction
    gali_series = df[PREDICTOR_FOR_GALI]
    # Step 4: Extract the 'GZDB' column as the target for prediction
    gzbd_series = df[PREDICTOR_FOR_GZBD]

    # Step 6: Fit the ARIMA model
    # Note: The order of the ARIMA model (p, d, q) should be tuned
    model = ARIMA(dswr_series, order=(5, 1, 0))  # p=5, d=1, q=0 are initial guesses
    model_fit = model.fit()
    # Step 7: Predict the next 1 period (you can change this to the desired number of periods)
    dswr_forecast = model_fit.forecast(steps=1)

    # Step 6: Fit the ARIMA model
    # Note: The order of the ARIMA model (p, d, q) should be tuned
    model = ARIMA(frbd_series, order=(5, 1, 0))  # p=5, d=1, q=0 are initial guesses
    model_fit = model.fit()
    # Step 7: Predict the next 1 period (you can change this to the desired number of periods)
    frbd_forecast = model_fit.forecast(1)

    # Step 6: Fit the ARIMA model
    # Note: The order of the ARIMA model (p, d, q) should be tuned
    model = ARIMA(gali_series, order=(5, 1, 0))  # p=5, d=1, q=0 are initial guesses
    model_fit = model.fit()
    # Step 7: Predict the next 1 period (you can change this to the desired number of periods)
    gali_forecast = model_fit.forecast(steps=1)

    # Step 6: Fit the ARIMA model
    # Note: The order of the ARIMA model (p, d, q) should be tuned
    model = ARIMA(gzbd_series, order=(5, 1, 0))  # p=5, d=1, q=0 are initial guesses
    model_fit = model.fit()
    # Step 7: Predict the next 1 period (you can change this to the desired number of periods)
    gzbd_forecast = model_fit.forecast(steps=1)

    dswr_count = float(dswr_forecast.iloc[0])
    frbd_count = float(frbd_forecast.iloc[0])
    gali_count = float(gali_forecast.iloc[0])
    gzbd_count = float(gzbd_forecast.iloc[0])

    # Assuming 'data' is a datetime object, extract month and year

    # Get today's date
    today = datetime.now()
    # Get the next date by adding one day
    next_date = today + timedelta(days=1)
    date = next_date.date()  # Example: replace with your actual date
    formatted_date = date.strftime(format="%B %d, %Y")
    month = date.strftime("%m")
    year = date.strftime("%Y")

    # Prepare the SQL query to insert data
    insert_query = """
    INSERT INTO satta_king_prediction (date, DSWR, FRBD, GZBD, GALI, month, year)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    try:
        cursor.execute(insert_query, (formatted_date, dswr_count, frbd_count, gzbd_count, gali_count, month, year))
        conn.commit()  # Commit if successful
        print("Data inserted successfully.")
    except psycopg2.Error as e:
        print("Error executing query:", e)
    # Execute the insert command with the forecasted values

    # Close the cursor and connection
    cursor.close()
    conn.close()

data = getdataFromMetrics()
insertPredictedValues(data)
