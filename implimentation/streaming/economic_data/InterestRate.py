import time
import yfinance as yf
from datetime import datetime


def fetch_realtime_interest_rate_data(interest_rates=yf.Ticker("^TNX")):
    """
    Fetch and insert real-time interest rate data into the database.

    Args:
        interest_rates: The interest rate ticker object (e.g., Yahoo Finance Ticker object).
    Returns:
        tuple: A tuple containing the latest data (timestamp, open, high, low, close) or None if no new data is available.
    """
    try:
        # Fetch real-time data
        interest_rate_realtime_data = interest_rates.history(period="1d", interval="1m")
        if not interest_rate_realtime_data.empty:
            # Get the most recent row of data
            latest_data = interest_rate_realtime_data.iloc[-1]  # Last row
            latest_timestamp = interest_rate_realtime_data.index[-1]  # Corresponding timestamp

            # Prepare the data for insertion
            open_price = float(latest_data["Open"])
            high_price = float(latest_data["High"])
            low_price = float(latest_data["Low"])
            close_price = float(latest_data["Close"])

            data_tuple = (
                latest_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                open_price,
                high_price,
                low_price,
                close_price
            )

            return data_tuple
        else:
            print("No data fetched for interest rates; retrying...")
            return None

    except Exception as e:
        print(f"Error while fetching real-time interest rate data: {e}")
        return None



import mysql.connector

def insert_interest_rate_data(data, cursor, connection):
    """
    Insert real-time interest rate data into the InterestRate table.

    Args:
        data: A tuple containing the interest rate data (timestamp, open, high, low, close).
        cursor: Database cursor for executing SQL queries.
        connection: Database connection object.
    """
    try:
        if data:
            # Prepare the insert query
            insert_query = """
                INSERT INTO InterestRate (Timestamp, open, high, low, close)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            # Extract the data from the tuple
            timestamp, open_price, high_price, low_price, close_price = data
            
            # Validate the data before inserting
            if None in [open_price, high_price, low_price, close_price]:
                print(f"Invalid data for interest rate at {timestamp}: Missing required fields.")
                return
            
            if open_price == 0 or high_price == 0 or low_price == 0 or close_price == 0:
                print(f"Invalid data for interest rate at {timestamp}: Prices cannot be zero.")
                return
            
            # Execute the insert query
            cursor.execute(insert_query, (timestamp, open_price, high_price, low_price, close_price))
            
            # Commit the transaction
            connection.commit()
            print(f"Inserted data into InterestRate table at {timestamp}.")
        
        else:
            print("No data available to insert.")
            
    except Exception as e:
        print(f"Error while inserting real-time interest rate data into database: {e}")
