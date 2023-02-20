import argparse
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX
import numpy as np

DB_HOST = "data-assignment.cvo9xh8ew9ac.eu-west-1.rds.amazonaws.com"
DB_NAME = "publitas_revolution_production"
USER = "assignment-user"
PASSWORD = "JL34*$AyFzgQ"

# Define command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--start-date", type=str, required=True, help="Start date in YYYY-MM-DD format")
parser.add_argument("--end-date", type=str, required=True, help="End date in YYYY-MM-DD format")
args = parser.parse_args()

DB_HOST = "data-assignment.cvo9xh8ew9ac.eu-west-1.rds.amazonaws.com"
DB_NAME = "publitas_revolution_production"
USER = "assignment-user"
PASSOWRD = "JL34*$AyFzgQ"
START_DATE = args.start_date
END_DATE = args.end_date



def query_function(query):
    try:
        # Try to connect to the database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=USER,
            password=PASSOWRD
        )
        print("Successfully connected to the database.")

        cur = conn.cursor()

        # Execute a SELECT query on the accounts table
        cur.execute(query)

        # Fetch all the rows returned by the query
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
        return df

        # Close the cursor and database connections
        cur.close()
        conn.close()


    except Exception as e:
        # If there's an error, print out the error message
        print("Error: Unable to connect to the database.")
        print(e)

    conn.close()


# Overview Query
def forcast_query(start_date, end_date):

    query = f''' 
    WITH 
        dataset AS (
        SELECT
            *
        FROM
            publications
        WHERE
            first_online_at BETWEEN DATE('{start_date}') AND DATE('{end_date}'))
            
    SELECT 
    
            DATE(dataset.first_online_at) AS Timestamp,
            CASE 
                WHEN tier = 'basic' THEN 1
                WHEN tier = 'bronze' THEN 2
                WHEN tier = 'enterprise' THEN 3
                WHEN tier = 'free' THEN 4
                WHEN tier = 'gold' THEN 5
                WHEN tier = 'gold-2021' THEN 6
                WHEN tier = 'professional' THEN 7
                WHEN tier = 'silver' THEN 8
                WHEN tier IS NULL THEN 9
                ELSE NULL
            END AS tier,
            COUNT(*) AS publication_count
                    
        FROM
        dataset
        INNER JOIN 
        groups AS g
        ON
        dataset.group_id = g.id
        INNER JOIN 
        contracts AS c
        ON
        g.account_id = c.account_id
        INNER JOIN 
        accounts AS a
        ON 
        c.account_id = a.id
        GROUP BY 
        1,2
        ORDER BY 
        1,2'''
    print('query_captured')
    return query



# read the data into a pandas dataframe
df = query_function(forcast_query(START_DATE, END_DATE))

# convert the timestamp column to datetime type and set it as the index
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)

# remove outliers based on the interquartile range
Q1 = df['publication_count'].quantile(0.25)
Q3 = df['publication_count'].quantile(0.75)
IQR = Q3 - Q1
df = df[~((df['publication_count'] < (Q1 - 1.5 * IQR)) | (df['publication_count'] > (Q3 + 1.5 * IQR)))]

# pivot the table to convert the data into a time series
ts = df.pivot(columns='tier', values='publication_count')

# plot the time series data for each tier
fig, axs = plt.subplots(7, 1, figsize=(10, 20))
for i in range(1, 8):
    axs[i - 1].plot(ts.index, ts[i])
    axs[i - 1].set_title('Tier ' + str(i))
plt.tight_layout()
plt.show()

# plot the time series data for all tiers combined
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(ts.index, ts.sum(axis=1))
ax.set_title('All Tiers')
plt.show()

# build a SARIMA model for each tier and make predictions for the next 6 months
forecasts = {}
for i in range(1, 8):
    model = SARIMAX(ts[i], order=(1, 1, 1), seasonal_order=(1, 1, 1, 12))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=30)
    forecasts[i] = np.round(forecast)

    # plot the forecast for each tier
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(ts[i].index[-30:], ts[i][-30:])
    ax.plot(forecast.index, forecast, color='red')
    ax.set_title('Tier ' + str(i) + ' Publication Count Forecast')


    plt.show()
    print('Tier', i, 'Forecast:', forecasts[i])

