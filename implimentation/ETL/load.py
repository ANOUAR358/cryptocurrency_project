from pyhive import hive
import pandas as pd
from datetime import datetime
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta


#===========================================load sentiment================================================================================
def insert_sentiment_data_to_hive(sentiment_results,conn):
    """
    Inserts sentiment analysis results into Hive.
    :param data: List of tuples containing (timestamp, content).
    """
    # Connect to Hive
    if conn is None:
        print("Error: No connection to Hive.")
        return
    else:
        print("Connected to Hive.")
    cursor = conn.cursor()
    try:
        # Insert sentiment results into the Hive table
        for record in sentiment_results:
            timestamp = record['timestamp']
            score = record['score']
            sentiment = record['sentiment']
            
            query = f"""
            INSERT INTO sentiment_dim
            VALUES ('{timestamp}', {score}, '{sentiment}')
            """
            cursor.execute(query)
        print("Sentiment data successfully inserted into Hive.")
    except Exception as e:
        print(f"Error while inserting sentiment data into Hive")

#===========================================load correlations================================================================================


def insert_correlation_data_to_hive(data,conn):
        
        if conn is None:
            print("Error: No connection to Hive.")
            return
        else:
            print("Connected to Hive.")
        cursor = conn.cursor()
        try:
            for record in data:
                query = f"""
                INSERT INTO correlations_dim (ts, gold_price, interest_rate, stocks_price)
                VALUES ('{record['time_stamp']}', {record['goldprice']}, {record['intersrate']}, {record['stocke']})
                """
                cursor.execute(query)
            
            print(f"Inserted {len(data)} records into Hive.")
        except Exception as e:
            print("Error inserting correlation data into Hive")

#===========================================load coins data to cryoto dim================================================================================
def insert_coins_data_to_hive(data,conn):
    """
    Inserts coin data into Hive.
    :param data: List of dictionaries with keys: time_stamp, coin, open, close, high, low, volume, market_cap, etc.
    """
    if conn is None:
        print("Error: No connection to Hive.")
        return
    else:
        print("Connected to Hive.")
    try:
        cursor = conn.cursor()
        for record in data:

            query = f"""
            INSERT INTO crypto_dim (ts, coin, open, high, low, close, volume, 
                                    market_cap, price_change, price_change_perc, 
                                    market_cap_change, market_cap_change_perc)
            VALUES (
                '{record['time_stamp']}', '{record['coin']}', {record['open']}, {record['high']}, {record['low']},
                {record['close']}, {record['volume']}, {record['market_cap']}, {record['price_change']},
                {record['price_change_percentage']}, {record['market_cap_change']}, {record['market_cap_change_percentage']}
            )
            """
            cursor.execute(query)
        print("Data insertion into Hive completed.")

    except Exception as e:
        print(f"An unexpected error occurred while inserting to Hive")

#===========================================load technicals indicator ================================================================================

def insert_technical_indicators_to_hive(dataframe,conn):

    if conn is None:
        print("Error: No connection to Hive.")
        return
    else:    
        print("Connected to Hive.")
    try:
        # Create a cursor object using the connection
        cursor = conn.cursor()
        # Counter for successful inserts
        successful_inserts = 0
        # Iterate over the dataframe and insert data into the table
        for _, row in dataframe.iterrows():
            query = f"""
            INSERT INTO technical_indicators (ts, coin, rsi, sma, ema)
            VALUES ('{row['Timestamp']}', '{row['Coin']}', {row['RSI']}, {row['SMA']}, {row['EMA']})
            """
            cursor.execute(query)
            # Increment successful insert counter, but do not print each success
            successful_inserts += 1
        # Commit the transaction and close the connection
        
        # Print summary of the insert process
        print(f"Successfully inserted {successful_inserts} rows into the technical_indicators table.")
        
    except Exception as e:
        print("Error while inserting to Hive")




#===========================================load fact table data ================================================================================

def insert_results_into_fact_table(results,conn):

    if conn is None:
        print("Error: No connection to Hive.")
        return  
    else:
        print("Connected to Hive.")
    try:
        cursor = conn.cursor()
        # Table name
        table_name = "fact_table"
        # Insert each row into the Hive table
        for row in results:
            # Ensure the keys in the dictionary match the table's schema
            keys = ["ts", "coin", "price", "dominance", "exchangerate"]
            
            # Format the timestamp correctly for Hive
            timestamp = row.get("timestamp").strftime('%Y-%m-%d %H:%M:%S') if row.get("timestamp") else None
            values = [timestamp, row.get("coin"), row.get("close_price"), row.get("dominance"), row.get("exchange_rate")]
            
            # Format values for SQL query
            formatted_values = ", ".join(
                f"'{v}'" if isinstance(v, str) else "NULL" if v is None else str(v)
                for v in values
            )
            query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({formatted_values})"

            cursor.execute(query)
        print(f"Inserted {len(results)} records into the fact table.")
        # Close the connection

    except Exception as e:
        print(f"Error while inserting  to Hive")




#===========================================load metadata ================================================================================

def insert_metadata_into_hive(crypto_metadata,conn):

    if conn is None:
        print("Error: No connection to Hive.")
        return  
    else:    
        print("Connected to Hive.") 
    try:
        cursor = conn.cursor()
        # Prepare the insert statement
        insert_statement = """
            INSERT INTO crypto_info (symbol, launch_date, ath_price, ath_date, total_supply)
            VALUES (%s, %s, %s, %s, %s)
        """

        # Loop through the data and insert each record into the table
        for record in crypto_metadata:
            # Convert string date to date type
            launch_date = datetime.strptime(record['launch_date'], '%Y-%m-%d').date()
            ath_date = datetime.strptime(record['ath_date'], '%Y-%m-%d').date()

            # Execute the insert statement
            cursor.execute(insert_statement, (
                record['symbol'], 
                launch_date, 
                record['ath_price'], 
                ath_date, 
                record['total_supply']
            ))
        print(f"{len(crypto_metadata)} records inserted into crypto_info table in Hive.")

    except Exception as e:
        print("Error inserting metadata into Hive")

#===========================================load block chain data ================================================================================

def insert_blockchain_info_into_hive(blockchain_info_data,conn):

    # Connect to the Hive server
    if conn is None:
        print("Error: No connection to Hive.")
        return  
    else:
        print("Connected to Hive.")
    cursor = conn.cursor()
    try:
        for row in blockchain_info_data:
        # Prepare the insert statement
            insert_statement = """
                INSERT INTO blockchain_info (ts, symbol, hashrate, pts, total_trans)
                VALUES (%s, %s, %s, %s, %s)
            """

            # Execute the insert statement using the data from blockchain_info_data
            cursor.execute(insert_statement, (
                row['timestamp'], 
                row['symbol'], 
                row['hashrate'], 
                row['pts'], 
                row['total_trans']
            ))
        print(f"{len(blockchain_info_data)} lines inserted into blockchain_info table in hive data warehouse")
    except:
        print("Error inserting blockchain data into Hive")

#===========================================load datetime data ================================================================================

def insert_date_dimensions_to_hive(date_dimensions,conn):
    """
    Insert transformed date dimensions into the Hive `date_dim` table.

    Parameters:
        date_dimensions (dict): A dictionary containing the date dimensions.
    """
    if conn is None:
        print("Error: No connection to Hive.")
        return  
    else:
        print("Connected to Hive.")
    try:
        # Establish a connection to Hive
        cursor = conn.cursor()

        # Prepare the INSERT INTO statement
        insert_query = """
            INSERT INTO TABLE date_dim
            (ts, day, month, year, hour, day_of_week, week)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        # Extract values from the date_dimensions dictionary
        values = (
            date_dimensions['timestamp'],
            date_dimensions['day'],
            date_dimensions['month'],
            date_dimensions['year'],
            date_dimensions['hour'],
            date_dimensions['day_of_week'],
            date_dimensions['week']
        )

        # Execute the insert query
        cursor.execute(insert_query, values)
        # Commit and close the connection
        print("Date dimensions successfully inserted into Hive.")

    except Exception as e:
        print(f"Error inserting date dimensions into Hive")
