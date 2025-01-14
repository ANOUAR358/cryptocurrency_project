import time
import requests
from datetime import datetime

# Binance API endpoint for real-time price data
BINANCE_API_URL = "https://api.binance.com/api/v1/ticker/24hr"

def fetch_realtime_data(symbol):
    """
    Fetch real-time price data for a specific symbol from Binance API.

    Args:
        symbol (str): The symbol for the cryptocurrency (e.g., 'BTCUSDT').

    Returns:
        tuple: A tuple containing price and volume for the specified symbol.
    """
    url = BINANCE_API_URL
    params = {'symbol': symbol}

    try:
        response = requests.get(url, params=params)
        data = response.json()

        if response.status_code == 200:
            # Extract relevant data from the API response
            price = float(data['lastPrice'])
            volume = float(data['volume'])
            return price, volume
        else:
            print(f"Error fetching data for {symbol}: {response.status_code}")
            return None, None
    except Exception as e:
        print(f"Error: {e}")
        return None, None


def fetch_realtime_dominance_data():
    """
    Fetch and prepare real-time dominance data for specified coins for insertion into the database.

    Args:
        coins: List of coin symbols to fetch dominance data for.
    
    Returns:
        tuple: A tuple containing the timestamp and dominance data (BTC Dominance, ETH Dominance, Altcoin Dominance) or None if no data is available.
    """
    try:
        # Fetch real-time data for specified coins (BTC and ETH in this case)
        btc_price, btc_volume = fetch_realtime_data('BTCUSDT')
        eth_price, eth_volume = fetch_realtime_data('ETHUSDT')

        if btc_price is not None and eth_price is not None:
            # Calculate dominance values
            btc_market_cap = btc_price * btc_volume
            eth_market_cap = eth_price * eth_volume
            total_market_cap = btc_market_cap + eth_market_cap

            btc_dominance = (btc_market_cap / total_market_cap) * 100
            eth_dominance = (eth_market_cap / total_market_cap) * 100
            altcoin_dominance = 100 - (btc_dominance + eth_dominance)

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Return the dominance data as a tuple
            return (
                timestamp,
                btc_dominance,
                eth_dominance,
                altcoin_dominance
            )
        else:
            print(f"{datetime.now()} - No data available for the specified period and interval.")
            return None

    except Exception as e:
        print(f"Error while fetching real-time dominance data: {e}")
        return None





def insert_dominance_data(data, cursor, connection):
    """
    Insert real-time dominance data into the database.

    Args:
        data: A tuple containing the data (timestamp, btc_dominance, eth_dominance, altcoin_dominance)
        cursor: The database cursor to execute queries.
        connection: The database connection for committing changes.
    
    Returns:
        None: Data is inserted into the database.
    """
    try:
        # Prepare the insert query
        insert_query = """
            INSERT INTO dominance (BTC_Dominance, ETH_Dominance, Altcoin_Dominance, created_at)
            VALUES (%s, %s, %s, %s)
        """
        
        # Ensure data is in the correct format
        if data:
            timestamp, btc_dominance, eth_dominance, altcoin_dominance = data

            # Validate the data before inserting
            if None in [btc_dominance, eth_dominance, altcoin_dominance]:
                print(f"Invalid data for dominance at {timestamp}: Missing required fields.")
                return
            
            # Insert the data into the table
            cursor.execute(insert_query, (btc_dominance, eth_dominance, altcoin_dominance, timestamp))
            connection.commit()
            print(f"Inserted dominance data: BTC: {btc_dominance}%, ETH: {eth_dominance}%, Altcoin: {altcoin_dominance}% at {timestamp}")

        else:
            print("No valid data to insert into the database.")

    except Exception as e:
        print(f"Error while inserting dominance data into the database: {e}")
