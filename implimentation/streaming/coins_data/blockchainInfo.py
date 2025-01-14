import time
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from datetime import datetime

def fetch_blockchain_statistics(coins=['BTC', 'ETH', 'BNB', 'ADA', 'SOL', 'XRP']):
    """
    Fetch real-time blockchain statistics for the specified coins from CoinMarketCap API.

    Args:
        coins (list): List of coin symbols to fetch data for.

    Returns:
        list: A list of tuples containing blockchain statistics.
    """
    url = 'https://sandbox-api.coinmarketcap.com/v1/blockchain/statistics/latest'
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': '0640fdfc-abe8-4107-95b8-de306df3173d',  # Replace with your API key
    }
    session = Session()
    session.headers.update(headers)

    try:
        # Fetch data from the API
        response = session.get(url, params={'symbol': ','.join(coins)})
        data = response.json()

        # Prepare the data for the specified coins
        if 'data' in data:
            blockchain_data = []
            timestamp = datetime.now()

            for coin in coins:
                if coin in data['data']:
                    details = data['data'][coin]

                    # Extract data fields
                    block_reward_static = details.get('block_reward_static', 0)
                    consensus_mechanism = details.get('consensus_mechanism', 'Unknown')
                    difficulty = details.get('difficulty', 'Unknown')
                    hashrate_24h = details.get('hashrate_24h', 'Unknown')
                    pending_transactions = details.get('pending_transactions', 0)
                    reduction_rate = details.get('reduction_rate', 'Unknown')
                    total_blocks = details.get('total_blocks', 0)
                    total_transactions = details.get('total_transactions', 'Unknown')
                    tps_24h = details.get('tps_24h', 0.0)
                    first_block_timestamp = details.get('first_block_timestamp', None)

                    # Convert first_block_timestamp to DATETIME
                    if first_block_timestamp:
                        first_block_timestamp = datetime.fromisoformat(first_block_timestamp.replace('Z', '+00:00'))

                    # Add to the data list
                    blockchain_data.append((
                        timestamp,
                        coin,
                        block_reward_static,
                        consensus_mechanism,
                        difficulty,
                        hashrate_24h,
                        pending_transactions,
                        reduction_rate,
                        total_blocks,
                        total_transactions,
                        tps_24h,
                        first_block_timestamp
                    ))
                else:
                    print(f"Coin {coin} not found in API response.")

            return blockchain_data
        else:
            print("No data found in the API response.")
            return None

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"Error while fetching data from API: {e}")
        return None





def insert_blockchain_statistics_data(data, cursor, connection):
    """
    Insert real-time blockchain statistics data into the database.

    Args:
        data: A list of tuples containing the blockchain statistics data.
        cursor: The database cursor to execute queries.
        connection: The database connection to commit the transaction.
    
    Returns:
        None: Data is inserted into the database.
    """
    try:
        # Prepare the insert query
        insert_query = """
            INSERT INTO blockchain_statistics (
                Timestamp, Coin, Block_Reward_Static, Consensus_Mechanism, Difficulty,
                Hashrate_24h, Pending_Transactions, Reduction_Rate, Total_Blocks, 
                Total_Transactions, Tps_24h, First_Block_Timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        """

        # Ensure data is in the correct format
        if data:
            for record in data:
                timestamp, coin, block_reward_static, consensus_mechanism, difficulty, \
                hashrate_24h, pending_transactions, reduction_rate, total_blocks, \
                total_transactions, tps_24h, first_block_timestamp = record

                # Validate the data before inserting
                if None in [block_reward_static, consensus_mechanism, difficulty, hashrate_24h, 
                            pending_transactions, reduction_rate, total_blocks, total_transactions, 
                            tps_24h]:
                    print(f"Invalid data for {coin} at {timestamp}: Missing required fields.")
                    continue  # Skip this record
                
                if block_reward_static == 0 or tps_24h == 0:
                    print(f"Invalid data for {coin} at {timestamp}: Values for block_reward_static or tps_24h cannot be zero.")
                    continue  # Skip this record
                
                if pending_transactions < 0 or total_blocks < 0:
                    print(f"Invalid data for {coin} at {timestamp}: Negative values for transactions or blocks are not allowed.")
                    continue  # Skip this record
                
                # Execute the insert query
                cursor.execute(insert_query, (
                    timestamp, coin, block_reward_static, consensus_mechanism, difficulty,
                    hashrate_24h, pending_transactions, reduction_rate, total_blocks,
                    total_transactions, tps_24h, first_block_timestamp
                ))

            # Commit the transaction
            connection.commit()
            print(f"Inserted blockchain statistics data into database.")

        else:
            print("No valid data to insert into the database.")

    except Exception as e:
        print(f"Error while inserting blockchain statistics data into the database: {e}")
