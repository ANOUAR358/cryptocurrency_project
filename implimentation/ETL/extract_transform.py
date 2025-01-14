
import mysql.connector
import pandas as pd
from datetime import datetime
import yfinance as yf
import pandas_ta as ta
from datetime import datetime, timedelta
import mysql.connector
import pandas as pd
import pandas_ta as ta



#=======================================get news and tweets========================================================

# Function to get sentiment data from the last 'interval' minutes
def get_sentiment_data(interval=4, host="localhost", user="root", password="358WNMGF", database="crypto"):

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
            return []
        else:
            print("Connected to MySQL database.")
        cursor = connection.cursor()

        # SQL query to select timestamp and content from the sentiment table for the last 'interval' minutes
        query = f"""
            SELECT created_at, content 
            FROM sentiment 
            WHERE created_at >= NOW() - INTERVAL {interval} MINUTE
        """
        cursor.execute(query)

        # Fetch all rows from the result
        rows = cursor.fetchall()
        if not rows:
            print("No data found for the specified time range.")
            return []
        else:
            print(f"Fetched {len(rows)} sentiment records from the database.")
         
        # Close the cursor and connection
        cursor.close()
        connection.close()

        print(f"Fetched {len(rows)}  sentiments records from the database")
        return rows

    except mysql.connector.Error as err:
        print(f"Error fetching sentiment data from MySQL ")
        return []  # Return empty list if an error occurs


#=======================================get economic data ========================================================

def get_correlation_data(days=4, host="localhost", user="root", password="358WNMGF", database="crypto"):
    try:
        # Establish connection to the MySQL database
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = connection.cursor()
        if not connection.is_connected():
            print("Connection to MySQL database failed.")
            return []
        else:
            print("Connected to MySQL database.")
        
        # SQL query to select data for the last N days and calculate the average of close and open values
        query = """
        SELECT 
            g.Timestamp AS time_stamp,
            (g.Close + g.Open) / 2 AS goldprice,
            (i.Close + i.Open) / 2 AS intersrate,
            (s.Close + s.Open) / 2 AS stocke
        FROM
            GoldPrice g
        LEFT JOIN
            InterestRate i ON DATE(g.Timestamp) = DATE(i.Timestamp)
        LEFT JOIN
            stocksPrices s ON DATE(g.Timestamp) = DATE(s.Timestamp)
        WHERE
            DATE(g.Timestamp) BETWEEN CURDATE() - INTERVAL %s DAY AND CURDATE()
        ORDER BY 
            g.Timestamp DESC
        LIMIT 10;
        """

        # Execute the query with a single parameter for 'days'
        cursor.execute(query, (days,))

        # Fetch the results
        results = cursor.fetchall()

        if not results:
            print("No data found for the specified time range.")
            return []

        # Prepare the final output
        data = []
        for row in results:
            record = {
                'time_stamp': row[0],
                'goldprice': row[1],
                'intersrate': row[2],
                'stocke': row[3]
            }
            data.append(record)

        print(f"Fetched {len(data)} records from MySQL database.")

        # Close the connection
        cursor.close()
        connection.close()

        return data

    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred when fetching correlation data: {e}")


#=======================================get coins data ========================================================

def get_coins_data(interval=4, host="localhost", user="root", password="358WNMGF", database="crypto"):
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
            return []
        else:
            print("Connected to MySQL database.")
        cursor = connection.cursor()

        # Calculate the time for the last minute
        current_time = datetime.now()
        past_time = current_time - timedelta(minutes=interval)
        formatted_past_time = past_time.strftime('%Y-%m-%d %H:%M:%S')

        # SQL query to select data for the last minute based on 'created_at'
        query = """
        SELECT
            created_at AS time_stamp,
            Coin,
            Open,
            High,
            Low,
            Close,
            Volume,
            Market_Cap
        FROM
            crypto_data
        WHERE
            created_at >= %s;
        """

        # Execute the query with the calculated past time as the parameter
        cursor.execute(query, (formatted_past_time,))

        # Fetch the results
        results = cursor.fetchall()

        if not results:
            print("No data found for the specified time range.")
            return []

        # Prepare the final output with price, market cap, and volume changes
        data = []
        for row in results:
            # Extract current record data
            time_stamp = row[0]
            coin = row[1]
            open_price = row[2]
            high_price = row[3]
            low_price = row[4]
            close_price = row[5]
            volume = row[6]
            market_cap = row[7]

            # Calculate the 24-hour window (look 24 hours back from this record's timestamp)
            twenty_four_hours_ago = time_stamp - timedelta(hours=24)
            formatted_24h_ago = twenty_four_hours_ago.strftime('%Y-%m-%d %H:%M:%S')

            # Query to get the data for the last 24 hours (before the current record)
            query_24h = """
            SELECT
                Close,
                Market_Cap
            FROM
                crypto_data
            WHERE
                Coin = %s AND created_at >= %s AND created_at <= %s
            ORDER BY created_at ASC;
            """

            cursor.execute(query_24h, (coin, formatted_24h_ago, time_stamp))

            # Fetch 24-hour data
            data_24h = cursor.fetchall()
            if not data_24h:    # If no data found for the last 24 hours, skip this record
                continue

            if len(data_24h) > 0:
                # Use the first record from 24 hours ago (if available)
                first_record = data_24h[0]
                first_close_price = first_record[0]
                first_market_cap = first_record[1]

                # Calculate the price change and percentage price change
                price_change = close_price - first_close_price
                price_change_percentage = (price_change / first_close_price) * 100 if first_close_price != 0 else 0

                # Calculate the market cap change and percentage change
                market_cap_change = market_cap - first_market_cap
                market_cap_change_percentage = (market_cap_change / first_market_cap) * 100 if first_market_cap != 0 else 0
            else:
                # If no data found for 24 hours ago, set changes to zero
                price_change = 0
                price_change_percentage = 0
                market_cap_change = 0
                market_cap_change_percentage = 0
            # Append the record with changes to the final data
            record = {
                'time_stamp': time_stamp,
                'coin': coin,
                'open': open_price,
                'close': close_price,
                'high': high_price,
                'low': low_price,
                'volume': volume,
                'price_change': price_change,
                'price_change_percentage': price_change_percentage,
                'market_cap': market_cap,
                'market_cap_change': market_cap_change,
                'market_cap_change_percentage': market_cap_change_percentage
            }
            data.append(record)

        print(f"Fetched {len(data)} records from MySQL database.")

        # Close the connection
        cursor.close()
        connection.close()

        return data

    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")




#=======================================get coins data and calculate indicators========================================================


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


#=======================================get effect table data ========================================================
def get_crypto_info(interval_minutes=4, host="localhost", user="root", password="358WNMGF", database="crypto"):
    try:

        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if not connection.is_connected():
            print("Connection to MySQL database failed.")
            return []
        else:
            print("Connected to MySQL database.")
        cursor = connection.cursor()

        # Calculer le timestamp pour filtrer les dernières minutes
        last_time = datetime.now() - timedelta(minutes=interval_minutes)
        formatted_last_time = last_time.strftime('%Y-%m-%d %H:%M:%S')

        # Requête pour extraire les données de crypto_data
        query_crypto = """
        SELECT created_at, Coin, Close 
        FROM crypto_data 
        WHERE created_at >= %s
        """
        cursor.execute(query_crypto, (formatted_last_time,))
        crypto_data = cursor.fetchall()  # Fetch all results to avoid "Unread result found"

        # Requête pour extraire les données de dominance
        query_dominance = """
        SELECT BTC_Dominance, ETH_Dominance, Altcoin_Dominance, created_at 
        FROM dominance 
        WHERE created_at >= %s
        """
        cursor.execute(query_dominance, (formatted_last_time,))
        dominance_data = cursor.fetchone()  # Fetch one result

        # Ensure all results from previous queries are consumed
        cursor.fetchall()  # If any results are still pending, fetch them

        # Requête pour extraire les données d'ExchangeRate
        query_exchange = """
        SELECT Timestamp, unit, value 
        FROM ExchangeRate 
        WHERE Timestamp >= %s
        """
        cursor.execute(query_exchange, (formatted_last_time,))
        exchange_data = cursor.fetchall()  # Fetch all results for this query

        # Préparer les résultats
        results = []
        for timestamp, coin, close in crypto_data:
            # Identifier la dominance associée à la crypto
            if coin.lower() == "btc":
                dominance = dominance_data[0]  # BTC_Dominance
            elif coin.lower() == "eth":
                dominance = dominance_data[1]  # ETH_Dominance
            else:
                dominance = dominance_data[2]  # Altcoin_Dominance

            # Trouver le taux d'échange correspondant
            exchange_rate = next((rate for ts, name, rate in exchange_data if name.lower() == coin.lower()), None)

            # Ajouter les données au résultat
            results.append({
                "timestamp": timestamp,
                "coin": coin,
                "dominance": dominance,
                "exchange_rate": exchange_rate,
                "close_price": close
            })
            if results:
                print(f"fetched {len(results)} records for the effect table.")
            else:
                print("No data found for the specified time range.")

        # Fermer la connexion
        cursor.close()
        connection.close()
        return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []  # Return empty list in case of error

#==========================================crypto metadata ==================================================
def cryptoinfo():
    try:
        crypto_metadata = [
            {
                "symbol": "BTC",
                "launch_date": "2009-01-03",
                "ath_price": 69000,  # Adjusted all-time high in USD
                "ath_date": "2021-11-10",
                "total_supply": 21000000
            },
            {
                "symbol": "ETH",
                "launch_date": "2015-07-30",
                "ath_price": 4878,
                "ath_date": "2021-11-10",
                "total_supply": 12044000000  # ETH has no fixed supply cap
            },
            {
                "symbol": "BNB",
                "launch_date": "2017-07-25",
                "ath_price": 690,
                "ath_date": "2021-05-10",
                "total_supply": 200000000
            },
            {
                "symbol": "ADA",
                "launch_date": "2017-09-29",
                "ath_price": 3.10,
                "ath_date": "2021-09-02",
                "total_supply": 45000000000
            },
            {
                "symbol": "SOL",
                "launch_date": "2020-03-24",
                "ath_price": 260,
                "ath_date": "2021-11-06",
                "total_supply": 539312705
            },
            {
                "symbol": "XRP",
                "launch_date": "2012-06-02",
                "ath_price": 3.84,
                "ath_date": "2018-01-04",
                "total_supply": 100000000000
            }
        ]
        if crypto_metadata:
            print("Cryptocurrency metadata successfully created.")
        return crypto_metadata
    except Exception as e:
        print(f"Error while creating cryptocurrency metadata:")
        return []



#====================================== generate block chaine data =======================================================

import random
def blockchaininf():
    print("no connection to the database required for this data.")
    timestamp = datetime.now()
    ranges = {
        "BTC": {"hashrate": (100, 200), "tps": (3, 7), "total_trans": (5e9, 6e9)},
        "ETH": {"hashrate": (200, 400), "tps": (10, 30), "total_trans": (1e9, 1.5e9)},
        "BNB": {"hashrate": (50, 100), "tps": (5, 15), "total_trans": (2e8, 3e8)},
        "ADA": {"hashrate": (30, 80), "tps": (5, 12), "total_trans": (1e8, 2e8)},
        "SOL": {"hashrate": (50, 150), "tps": (10, 25), "total_trans": (1e8, 2e8)},
        "XRP": {"hashrate": (5, 20), "tps": (5, 10), "total_trans": (3e8, 5e8)},
    }

    # Generate synthetic blockchain info
    blockchain_info_data = []
    for symbol in ['BTC', 'ETH', 'BNB', 'ADA', 'SOL', 'XRP']:
        data_range = ranges.get(symbol, {"hashrate": (1, 10), "tps": (1, 2), "total_trans": (1e6, 2e6)})
        blockchain_info = {
            "hashrate": round(random.uniform(*ranges[symbol]["hashrate"]), 2),
            "tps": round(random.uniform(*ranges[symbol]["tps"]), 2),
            "total_trans": int(random.uniform(*ranges[symbol]["total_trans"])),
        }

        # Prepare data for Snowflake
        blockchain_info_data.append({
            "timestamp": timestamp,
            "symbol": symbol,
            "hashrate": blockchain_info["hashrate"],
            "pts": blockchain_info["tps"],
            "total_trans": blockchain_info["total_trans"],
        })
    
    if blockchain_info_data:
        print("Blockchain info data generated successfully.")
    else:
        print("Error generating blockchain  info data.")

    return blockchain_info_data



#====================================== timestamp data =======================================================

def get_last_timestamp(host="localhost", user="root", password="358WNMGF", database="crypto"):
    """
    Fetch the latest timestamp from the crypto_data table.
    
    :return: The latest timestamp (created_at) from the table.
    """
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if not conn.is_connected():
            print("Connection to MySQL database failed.")
            return None
        else:
            print("Connected to MySQL database.")
        cursor = conn.cursor()

        # Prepare the SQL query to fetch the latest timestamp
        query = """
            SELECT max(created_at) AS last_timestamp
            FROM crypto_data
        """
        
        # Execute the query
        cursor.execute(query)

        # Fetch the result (last timestamp)
        result = cursor.fetchone()
        if not result:
            print("No data found in the table.")
            return None
        else:
            print("Fetched last timestamp from the database.")
        # Close the connection
        cursor.close()
        conn.close()

        # Return the last timestamp
        return result[0] if result[0] else None

    except mysql.connector.Error as e:
        print(f"Error fetching last timestamp from MySQL: {e}")
        return None

