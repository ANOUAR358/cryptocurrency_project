import time
import requests
from datetime import datetime

API_KEY = 'ed52685eec3f13719d7483816fae2d41a7f04179998fba9fd6aeca857d684412'
url = f'https://min-api.cryptocompare.com/data/v2/histoday'

# List of coins you want to track
coins = ['BTC', 'ETH', 'BNB', 'ADA', 'SOL', 'XRP']

def fetch_realtime_crypto_data(coins=coins):
    """
    Fetch and prepare real-time cryptocurrency data for insertion into the database.

    Args:
        coins: List of coins to track (default is the list provided).
    
    Returns:
        tuple: A tuple containing the latest data (timestamp, coin, open, high, low, close, volume, market_cap)
    """
    try:
        all_coin_data = []

        for coin in coins:
            # Get the current Unix timestamp
            current_time = int(time.time())

            # Define parameters for the request
            params = {
                'fsym': coin,  # Coin symbol
                'tsym': 'USD',  # Fiat currency (USD)
                'limit': 1,  # Only get the latest data (1 data point)
                'toTs': current_time,  # Request the data until the current time
                'apiKey': API_KEY
            }

            # Make the request
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()

                if data['Response'] == 'Success':
                    coin_data = data['Data']['Data'][0]

                    # Extract relevant data
                    open_price = coin_data.get("open", None)
                    high_price = coin_data.get("high", None)
                    low_price = coin_data.get("low", None)
                    close_price = coin_data.get("close", None)
                    volume = coin_data.get("volumefrom", None)
                    market_cap = coin_data.get("volumeto", None)  # Assuming volumeto can be considered as market cap

                    # Add the data to the list
                    all_coin_data.append((
                        coin,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        market_cap
                    ))

                else:
                    print(f"Error fetching data for {coin}: {data.get('Message', 'Unknown error')}")
            else:
                print(f"Failed to fetch data for {coin}. Status code: {response.status_code}")

        # Return all the processed data
        return all_coin_data

    except Exception as e:
        print(f"Error while fetching real-time cryptocurrency data: {e}")
        return None


# Function to insert data into the database
def insert_crypto_data(data,cursor,connection):
    """
    Insert real-time cryptocurrency data into the database.

    Args:
        data: A list of tuples containing (timestamp, coin, open, high, low, close, volume, market_cap)
    """
    try:
        # Prepare insert query
        insert_query = """
            INSERT INTO crypto_data ( Coin, Open, High, Low, Close, Volume, Market_Cap)
            VALUES ( %s, %s, %s, %s, %s, %s, %s)
        """
        
        for record in data:
            coin, open_price, high_price, low_price, close_price, volume, market_cap = record
            
            # Validate the data before inserting
            if None in [open_price, high_price, low_price, close_price, volume, market_cap]:
                print(f"Invalid data for {coin} : Missing required fields.")
                continue  # Skip this record
            
            if open_price == 0 or high_price == 0 or low_price == 0 or close_price == 0:
                print(f"Invalid data for {coin} : Prices cannot be zero.")
                continue  # Skip this record
            
            if volume <= 0 or market_cap <= 0:
                print(f"Invalid data for {coin} : Volume or market cap cannot be zero or negative.")
                continue  # Skip this record
            
            # Execute the insert
            cursor.execute(insert_query, ( coin, open_price, high_price, low_price, close_price, volume, market_cap))
        
        # Commit the transaction
        connection.commit()
        print(f"Inserted data into crypto_data table.")

    except Exception as e:
        print(f"Error while inserting data into database: {e}")



