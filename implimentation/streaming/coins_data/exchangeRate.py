import time
import requests
from datetime import datetime

def fetch_exchange_rate_data(coins=['BTC', 'ETH', 'BNB', 'XRP']):
    """
    Fetch real-time exchange rate data for the specified coins from CoinGecko API.

    Args:
        coins (list): List of coin symbols to fetch data for.
    
    Returns:
        list: A list of tuples containing exchange rate data.
    """
    url = "https://api.coingecko.com/api/v3/exchange_rates"
    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": "CG-pDcojhJhRT1iAEtMAgXVCgif"  # Replace with your API key
    }
    try:
        # Fetch data from API
        response = requests.get(url, headers=headers)
        data = response.json()

        # Prepare the data for the specified coins
        if "rates" in data:
            timestamp = datetime.now()
            exchange_data = []

            for coin in coins:
                coin_key = coin.lower()  # API uses lowercase keys
                if coin_key in data["rates"]:
                    details = data["rates"][coin_key]
                    name = details.get("name", "Unknown")
                    unit = details.get("unit", "Unknown")
                    value = details.get("value", 0.0)
                    rate_type = details.get("type", "Unknown")

                    # Add to the data list
                    exchange_data.append((timestamp, name, unit, value, rate_type))
                else:
                    print(f"Coin {coin} not found in API response.")

            return exchange_data
        else:
            print("No rates found in the API response.")
            return None

    except Exception as e:
        print(f"Error while fetching data from API: {e}")
        return None





def insert_exchange_rate_data(data, cursor, connection):
    """
    Insert real-time exchange rate data into the database.

    Args:
        data: A list of tuples containing (timestamp, name, unit, value, rate_type)
        cursor: The database cursor to execute queries
        connection: The database connection for committing changes
    """
    try:
        # Prepare insert query
        insert_query = """
            INSERT INTO ExchangeRate (Timestamp, name, unit, value, type)
            VALUES (%s, %s, %s, %s, %s)
        """

        for record in data:
            timestamp, name, unit, value, rate_type = record

            # Validate the data before inserting
            if None in [name, unit, value, rate_type]:
                print(f"Invalid data at {timestamp}: Missing required fields.")
                continue  # Skip this record
            
            if value == 0:
                print(f"Invalid data at {timestamp}: Value cannot be zero.")
                continue  # Skip this record

            # Execute the insert query
            cursor.execute(insert_query, (timestamp, name, unit, value, rate_type))
        
        # Commit the transaction to save changes
        connection.commit()
        print(f"Inserted data into ExchangeRate table.")

    except Exception as e:
        print(f"Error while inserting data into database: {e}")
