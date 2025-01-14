import time
import yfinance as yf
from datetime import datetime


def fetch_realtime_gold_data(gold_ticker=yf.Ticker("GC=F")):
    """
    Fetch and prepare real-time gold price data for insertion into the database.

    Args:
        gold_ticker: The gold futures ticker object (e.g., Yahoo Finance Ticker object).
    
    Returns:
        tuple: A tuple containing the latest data (timestamp, open, high, low, close) or None if no data is available.
    """
    try:
        # Fetch real-time data for gold futures
        gold_realtime_data = gold_ticker.history(period="1d", interval="1m")
        if not gold_realtime_data.empty:
            # Process the latest data point
            latest_data = gold_realtime_data.iloc[-1]
            latest_timestamp = gold_realtime_data.index[-1]  # Get the timestamp

            # Prepare data for insertion
            open_price = float(latest_data["Open"])
            high_price = float(latest_data["High"])
            low_price = float(latest_data["Low"])
            close_price = float(latest_data["Close"])

            # Return the prepared tuple
            return (
                latest_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                open_price,
                high_price,
                low_price,
                close_price
            )
        else:
            print("No data fetched for gold; retrying...")
            return None

    except Exception as e:
        print(f"Error while fetching real-time gold data: {e}")
        return None


import mysql.connector

def insert_gold_data(data, cursor, connection):
    """
    Insert real-time gold price data into the GoldPrice table.

    Args:
        data: A tuple containing the gold data (timestamp, open, high, low, close).
        cursor: Database cursor for executing SQL queries.
        connection: Database connection object.
    """
    try:
        if data:
            # Prepare the insert query
            insert_query = """
                INSERT INTO GoldPrice (Timestamp, Open, High, Low, Close)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            # Extract the data from the tuple
            timestamp, open_price, high_price, low_price, close_price = data
            
            # Validate the data before inserting
            if None in [open_price, high_price, low_price, close_price]:
                print(f"Invalid data for gold at {timestamp}: Missing required fields.")
                return
            
            if open_price == 0 or high_price == 0 or low_price == 0 or close_price == 0:
                print(f"Invalid data for gold at {timestamp}: Prices cannot be zero.")
                return
            
            # Execute the insert query
            cursor.execute(insert_query, (timestamp, open_price, high_price, low_price, close_price))
            
            # Commit the transaction
            connection.commit()
            print(f"Inserted data into GoldPrice table at {timestamp}.")
        
        else:
            print("No data available to insert for gold table.")
            
    except Exception as e:
        print(f"Error while inserting real-time gold data into database: {e}")
