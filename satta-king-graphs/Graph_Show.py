
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.plotting import scatter_matrix

import psycopg2
import pandas as pd
PREDICTOR_FOR_DSWR = 'DSWR'
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


# Step 1: Plot all variables in one figure
df.plot(y=['DSWR', 'FRBD', 'GZBD', 'GALI'], figsize=(10, 6), title="Historical Data")
plt.ylabel('Value')
plt.xlabel('Date')
plt.legend(title="Metrics")
plt.show()

df.plot(subplots=True, figsize=(10, 12), layout=(4, 1), title="Historical Data for Each Variable")
plt.tight_layout()
plt.show()



# Step 1: Calculate the correlation matrix
corr = df[['DSWR', 'FRBD', 'GZBD', 'GALI']].corr()

# Step 2: Plot the heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f', vmin=-1, vmax=1)
plt.title("Correlation Heatmap")
plt.show()



# Step 1: Create a scatter matrix
scatter_matrix(df[['DSWR', 'FRBD', 'GZBD', 'GALI']], figsize=(10, 10), diagonal='kde')
plt.suptitle("Scatter Matrix of Variables")
plt.show()