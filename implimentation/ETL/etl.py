import time
from datetime import datetime, timedelta
import traceback
from pyhive import hive
import pandas as pd
from datetime import datetime
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta
# Import functions from the provided modules
from extract_transform import (
    get_sentiment_data, 
    get_correlation_data, 
    get_coins_data,
    get_last_timestamp,
    get_technical_indicators, 
    get_crypto_info, 
    cryptoinfo,
    blockchaininf
)

from transform import (
    apply_sentiment_analysis, 
    transforme_date_dimensions
)

from load import (
    insert_sentiment_data_to_hive,
    insert_correlation_data_to_hive,
    insert_coins_data_to_hive,
    insert_technical_indicators_to_hive,
    insert_results_into_fact_table,
    insert_metadata_into_hive,
    insert_blockchain_info_into_hive,
    insert_date_dimensions_to_hive
)


def perform_etl_cycle(conn):
    """
    Perform a complete ETL cycle for different data sources
    """
    try:
        # 1. Sentiment Analysis
        print("loading sentiment dim".center(160, '='))
        sentiment_data = get_sentiment_data()
        if sentiment_data:
            processed_sentiment = apply_sentiment_analysis(sentiment_data)
            insert_sentiment_data_to_hive(processed_sentiment,conn)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle sentiment dim")
    try:
        # 2. Correlation Data
        print("loading correlation dim".center(160, '='))
        correlation_data = get_correlation_data()
        if correlation_data:
            insert_correlation_data_to_hive(correlation_data,conn)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle correlation dim")
    try: 
        # 3. Coins Data
        print("loading coins dim".center(160, '='))
        coins_data = get_coins_data()
        if coins_data:
            insert_coins_data_to_hive(coins_data,conn)
    except Exception as e:
        
        print(f"Error in ETL Cycle:there is a probleme when trying to handle coins dim")
    try:
        # 4. Technical Indicators
        print("loading indicators dim".center(160, '='))
        indicators_data = get_technical_indicators()
        if not indicators_data.empty:
            insert_technical_indicators_to_hive(indicators_data,conn)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle indicators dim")
    try:
        # 5. Last Timestamp and Date Dimensions
        print("loading date dim".center(160, '='))
        last_timestamp = get_last_timestamp()
        if last_timestamp:
            transformed_timestamp = transforme_date_dimensions(last_timestamp)
            insert_date_dimensions_to_hive(transformed_timestamp,conn)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle date dim")
    try:
        # 6. Blockchain Information
        # Note: You might need to modify blockchaininf function to match your exact requirements
        print("loading blockchain dim".center(160, '='))
        blockchain_data = blockchaininf()
        if blockchain_data:
            insert_blockchain_info_into_hive(blockchain_data,conn)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle blockchain dim")
    try:
        # 7. Crypto Info Fact Table
        print("loading fact table".center(160, '='))
        fact_data = get_crypto_info()
        if fact_data:
            insert_results_into_fact_table(fact_data,conn)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle fact table")
    try:
        # 8. Metadata
        print("loading metadata dim".center(160, '='))
        metadata = cryptoinfo()
        if metadata:
            insert_metadata_into_hive(metadata,conn)
    
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle metadata dim")

def main():
    """
    Main function to run continuous ETL process
    """
    # Configuration for ETL cycle
    cycle_interval = 4 * 60  # 4 minutes 
    max_runtime_hours = 24  # Run for 24 hours maximum

    print("Starting Continuous ETL Process")
    start_time = datetime.now()
    counter = 1
    try:
        while True:
            host='192.168.11.119'
            port=10000
            database='crypto3'
            conn = hive.connect(host=host, port=port, database=database)
            if bool(conn):
                print("Connected to Hive Server")
            # Perform ETL cycle
            print(f"Starting ETL cycle {counter}".center(160, '|'))
            counter += 1
            perform_etl_cycle(conn)

            # Check runtime
            current_runtime = datetime.now() - start_time
            if current_runtime.total_seconds() >= max_runtime_hours * 3600:
                print(f"Maximum runtime of {max_runtime_hours} hours reached. Stopping ETL process.")
                break

            # Wait before next cycle
            print(f"Waiting {cycle_interval} seconds before next ETL cycle")
            time.sleep(cycle_interval)
            conn.commit()

    except KeyboardInterrupt:
        print("ETL Process manually stopped by user")
    except Exception as e:
        print("Unexpected error in main ETL loop")
    finally:
        print("ETL Process Terminated")

if __name__ == "__main__":
    main()