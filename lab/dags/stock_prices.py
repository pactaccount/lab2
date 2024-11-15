from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime
import requests

# Function to return Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Task to fetch stock prices
@task
def stockprices(url):
    r = requests.get(url)
    data = r.json()
    print(data)
    results = []
    for d in data['Time Series (Daily)']:
        stock_info = data['Time Series (Daily)'][d]
        stock_info['date'] = d
        results.append(stock_info)
    return results

# Task to transform data
@task
def transform(prices):
    transformed_data = []
    for price in prices:
        price['source'] = 'API'
        transformed_data.append(price)
    return transformed_data

# Task to load data into Snowflake
@task
def load(prices, symbol):
    cur = return_snowflake_conn()
    try:
        # Start a transaction
        cur.execute('BEGIN')
        
        # Ensure table exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS Time_series.raw_data.stockprice(
                date DATE,
                open NUMBER(8,5),
                high NUMBER(8,5),
                low NUMBER(8,5),
                close NUMBER(8,5),
                volume INTEGER,
                symbol VARCHAR,
                source VARCHAR)
        """)
    except Exception as e:
        cur.execute('ROLLBACK')
        print(f"Error during table creation: {e}")
        return

    try:
        for p in prices:
            Open = float(p["1. open"])
            High = float(p["2. high"])
            Low = float(p["3. low"])
            Close = float(p["4. close"])
            Volume = int(p["5. volume"])
            Date = p["date"]
            Source = p["source"]

            # Check if the record already exists
            try:
                check_sql = f"""
                    SELECT 1 FROM Time_series.raw_data.stockprice
                    WHERE date = '{Date}' AND symbol = '{symbol}'
                """
                cur.execute(check_sql)
                if cur.fetchone():  # If a row is returned, skip the insert
                    print(f"Record for {Date} and {symbol} already exists. Skipping insert.")
                    continue

                # Insert new record if it doesn't exist
                insert_sql = f"""
                    INSERT INTO raw_data.stockprice (date, open, high, low, close, volume, symbol, source)
                    VALUES ('{Date}', {Open}, {High}, {Low}, {Close}, {Volume}, '{symbol}', '{Source}')
                """
                cur.execute(insert_sql)
                print(f"Inserted: {insert_sql}")

            except Exception as e:
                print(f"Error during record existence check or insert for date {Date}: {e}")
        
        # Commit the transaction
        cur.execute('COMMIT')

    except Exception as e:
        cur.execute('ROLLBACK')
        print(f"Error during data load: {e}")

# DAG definition
default_args = {
    'owner': 'Abhinav',
    'email': ['123@amail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='stock_prices',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ETL'],
    schedule_interval='*/10 * * * *',
    default_args=default_args,
) as dag:

    api_key = Variable.get("apikey")  # Fetch API key from Airflow Variables
    symbol = 'IBM'  # Stock symbol, can be dynamic if needed
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'

    # Task dependency chain
    prices = stockprices(url)
    transformed_prices = transform(prices)
    load(transformed_prices, symbol)
