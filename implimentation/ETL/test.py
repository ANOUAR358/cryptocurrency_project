import mysql.connector
from pyhive import hive
import pandas as pd
import pandas_ta as ta

def get_technical_indicators(days=30, host="localhost", user="root", password="358WNMGF", database="crypto"):
    """
    Function to retrieve technical indicators (RSI, EMA, SMA) for data collected every 4 minutes.
    """
    try:
        # Establish connection to the MySQL database
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if not connection.is_connected():
            print("Connection to MySQL database failed.")
            return pd.DataFrame()
        else:
            print("Connected to MySQL database.")
        cursor = connection.cursor()

        # Query to fetch data
        query = """
        SELECT created_at, Coin, Close, Open 
        FROM crypto_data
        WHERE created_at >= NOW() - INTERVAL %s DAY;
        """
        cursor.execute(query, (days,))
        results = cursor.fetchall()

        print(f"Fetched {len(results)} rows from the database.")  # Debugging line
        if not results:
            print("No data available for the specified interval.")
            return pd.DataFrame()

        # Convert results to DataFrame
        df = pd.DataFrame(results, columns=["Timestamp", "Coin", "Close", "Open"])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

        # Ensure data is sorted and remove duplicates
        df.sort_values(by="Timestamp", inplace=True)
        df.drop_duplicates(subset='Timestamp', inplace=True)

        # Resample data to 4-minute intervals
        df.set_index("Timestamp", inplace=True)
        df = df.resample("4min").agg({
            "Close": "last",
            "Coin": "first",  # Choose the first value in the interval
        })
        df['Close'] = df['Close'].ffill().bfill()  # Fill missing Close values

        # Ensure there are no None values in 'Close'
        if df['Close'].isnull().any():
            print("Error: 'Close' column contains None values after resampling.")
            return pd.DataFrame()

        # Ensure enough data points for indicator calculation
        if len(df) < 4:  # Adjust to match the look-back period (e.g., 4 for RSI)
            print("Not enough data points to calculate technical indicators.")
            return pd.DataFrame()

        # Calculate technical indicators with adjusted periods
        df['RSI'] = ta.rsi(df['Close'], length=4)  # Adjusted to 4-period RSI
        df['EMA'] = ta.ema(df['Close'], length=4)  # Adjusted to 4-period EMA
        df['SMA'] = ta.sma(df['Close'], length=4)  # Adjusted to 4-period SMA

        # Drop rows with NaN values (occurs at the start due to look-back period)
        df.dropna(inplace=True)

        # Reset index for export
        result = df.reset_index()[['Timestamp', 'Coin', 'RSI', 'EMA', 'SMA']]
        print(f"Technical indicators calculated for {len(result)} records.")

        # Close the connection
        cursor.close()
        connection.close()

        return result

    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


def insert_technical_indicators_to_hive(dataframe, conn):
    if dataframe is None or dataframe.empty:
        print("Error: No data to insert into Hive.")
        return
    if conn is None:
        print("Error: No connection to Hive.")
        return
    else:    
        print("Connected to Hive.")
    try:
        # Create a cursor object using the connection
        cursor = conn.cursor()
        
        # Prepare the insert query
        query = """
        INSERT INTO technical_indicators (ts, coin, rsi, sma, ema)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # Convert dataframe to list of tuples
        data_to_insert = [
            (row['Timestamp'], row['Coin'], row['RSI'], row['EMA'], row['SMA'])
            for _, row in dataframe.iterrows()
        ]
        
        # Execute the batch insert
        for data in data_to_insert:
            cursor.execute(query, data)
        
        # Commit the transaction
        conn.commit()
        
        # Print summary of the insert process
        print(f"Successfully inserted {len(data_to_insert)} rows into the technical_indicators table.")
        
    except Exception as e:
        print("Error while inserting to Hive")
        print(f"Error details: {e}")

# Run the code
host = '192.168.11.119'
port = 10000
database = 'crypto3'
conn = hive.connect(host=host, port=port, database=database)

# Get technical indicators data
data = get_technical_indicators()

# Insert data into Hive
insert_technical_indicators_to_hive(data, conn)
