from pyhive import hive

# Hive Connection and Schema Creation
def create_hive_schema():
    host = "192.168.11.119"  # Hive server's IP address
    port = 10000  # Port used by HiveServer2
    username = "hive"
    database = "crypto3"  # Database name in Hive

    try:
        # Establish a connection to Hive
        conn = hive.Connection(host=host, port=port, username=username, database=database)
        cursor = conn.cursor()

        # Define Schema Creation Queries
        schema_queries = [
            """
            CREATE TABLE IF NOT EXISTS date_dim (
                ts TIMESTAMP,
                day INT,
                month INT,
                year INT,
                hour INT,
                day_of_week STRING,
                week INT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS crypto_info (
                symbol STRING,
                launch_date DATE,
                ath_price FLOAT,
                ath_date DATE,
                total_supply BIGINT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS blockchain_info (
                ts TIMESTAMP,
                symbol STRING,
                hashrate FLOAT,
                pts INT,
                total_trans BIGINT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS technical_indicators (
                ts TIMESTAMP,
                coin STRING,
                rsi FLOAT,
                sma FLOAT,
                ema FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS correlations_dim (
                tS TIMESTAMP,
                gold_price FLOAT,
                interest_rate FLOAT,
                stocks_price FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sentiment_dim (
                ts TIMESTAMP,
                sentiment_score FLOAT,
                sentiment STRING
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_table (
                ts TIMESTAMP,
                coin STRING,
                price FLOAT,
                dominance FLOAT,
                exchangerate FLOAT
            )
            """,
            """ 
            CREATE TABLE IF NOT EXISTS crypto_dim (
                ts TIMESTAMP,
                coin STRING,
                open FLOAT,
                close FLOAT,
                high FLOAT,
                low FLOAT,
                volume FLOAT,
                market_cap FLOAT,
                price_change FLOAT,
                price_change_perc FLOAT,
                market_cap_change FLOAT,
                market_cap_change_perc FLOAT
            )
            """
        ]

        # Execute each query
        for query in schema_queries:
            try:
                cursor.execute(query)
                print(f"Executed: {query.strip()[:50]}...")  # Print first 50 characters of the query
            except Exception as query_error:
                print(f"Error executing query: {query.strip()[:50]}...\n{query_error}")

        cursor.close()
        conn.close()
        print("Data warehouse schema created successfully in Hive!")

    except Exception as e:
        print(f"Failed to connect or execute queries on Hive: {e}")

if __name__ == "__main__":
    create_hive_schema()
