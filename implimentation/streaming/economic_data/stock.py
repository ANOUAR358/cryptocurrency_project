import time
from datetime import datetime
import yfinance as yf

def fetch_realtime_stock_data(sp500 = yf.Ticker("^GSPC")):
    """
    Fetch and insert real-time stock data into the database.

    Args:
        sp500: The stock data object (e.g., Yahoo Finance Ticker object).
        cursor: Database cursor for executing SQL queries.
        connection: Database connection object.
    """
    try:
        # Fetch real-time data
        realtime_data = sp500.history(period="1d", interval="1m")
        if not realtime_data.empty:
            # Process the latest data point
            latest_data = realtime_data.iloc[-1]
            latest_timestamp = realtime_data.index[-1]  # Get the timestamp

            # Prepare data for insertion
            open_price = float(latest_data["Open"])
            high_price = float(latest_data["High"])
            low_price = float(latest_data["Low"])
            close_price = float(latest_data["Close"])
            volume = float(latest_data["Volume"])

            data_tuple = (
                latest_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            )

            return data_tuple
        else:
            print("no data fitched for the stock ,we will try again ")
            return None 

    except Exception as e:
        print(f"Error while inserting real-time stock data: {e}")




def insert_stock_data(data, cursor, connection):
    """
    Insert real-time stock data into the stocksPrices table.

    Args:
        data: A tuple containing stock data (timestamp, open, high, low, close, volume).
        cursor: Database cursor for executing SQL queries.
        connection: Database connection object.
    """
    try:
        if data:
            # Prepare the insert query
            insert_query = """
                INSERT INTO stocksPrices (Timestamp, Open, High, Low, Close, Volume)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            # Extract the data from the tuple
            timestamp, open_price, high_price, low_price, close_price, volume = data
            
            # Validate the data before inserting
            if None in [open_price, high_price, low_price, close_price, volume]:
                print(f"Invalid data for stock at {timestamp}: Missing required fields.")
                return
            
            if open_price == 0 or high_price == 0 or low_price == 0 or close_price == 0:
                print(f"Invalid data for stock at {timestamp}: Prices cannot be zero.")
                return
            
            if volume < 0:
                print(f"Invalid data for stock at {timestamp}: Volume cannot be zero or negative.")
                return
            
            # Execute the insert query
            cursor.execute(insert_query, (timestamp, open_price, high_price, low_price, close_price, volume))
            
            # Commit the transaction
            connection.commit()
            print(f"Inserted data into stocksPrices table at {timestamp}.")
        
        else:
            print("No data available to insert.")
            
    except Exception as e:
        print(f"Error while inserting real-time stock data into database: {e}")
