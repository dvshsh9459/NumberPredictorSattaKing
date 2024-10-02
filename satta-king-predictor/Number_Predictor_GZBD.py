import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA

import psycopg2
import pandas as pd
PREDICTOR_FOR_GZBD = 'GZBD'
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

# Step 1: Create a DataFrame
df = pd.DataFrame(data)

# Step 2: Convert the 'date' field to a datetime object
df['date'] = pd.to_datetime(df['date'], format='%B %d, %Y')

# Step 3: Set the 'date' column as the index for time series analysis
df.set_index('date', inplace=True)

# Option 1: Infer frequency automatically
df = df.asfreq(pd.infer_freq(df.index))

# Step 4: Extract the 'DSWR' column as the target for prediction
dswr_series = df[PREDICTOR_FOR_GZBD]

# Step 5: Plot the historical data
plt.plot(dswr_series)
plt.title(PREDICTOR_FOR_GZBD + ' Over Time')
plt.show()

# Step 6: Fit the ARIMA model
# Note: The order of the ARIMA model (p, d, q) should be tuned
model = ARIMA(dswr_series, order=(5, 1, 0))  # p=5, d=1, q=0 are initial guesses
model_fit = model.fit()

# Step 7: Predict the next 10 periods (you can change this to the desired number of periods)
forecast = model_fit.forecast(steps=1)

# Step 8: Plot the forecast
plt.plot(dswr_series, label='Historical ' + PREDICTOR_FOR_GZBD)
plt.plot(forecast, label='Forecast' + PREDICTOR_FOR_GZBD, color='red')
plt.legend()
plt.show()

# Print the forecasted values
print('Forecasted', PREDICTOR_FOR_GZBD, 'values for the next periods:')
print(forecast)
# Correct way to access the first value by position
print(f"Forecasted {PREDICTOR_FOR_GZBD} for the next day: {forecast.iloc[0]}")