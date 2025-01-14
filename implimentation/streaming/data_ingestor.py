
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from datetime import datetime

import time
import requests

import time
import yfinance as yf
from datetime import datetime, timedelta

import tweepy

import mysql.connector



from coins_data.blockchainInfo import fetch_blockchain_statistics , insert_blockchain_statistics_data
from coins_data.dominance import fetch_realtime_dominance_data ,insert_dominance_data
from coins_data.exchangeRate import fetch_exchange_rate_data,insert_exchange_rate_data
from coins_data.prices import fetch_realtime_crypto_data,insert_crypto_data

from economic_data.Gold import fetch_realtime_gold_data ,insert_gold_data
from economic_data.InterestRate import fetch_realtime_interest_rate_data, insert_interest_rate_data
from economic_data.stock import fetch_realtime_stock_data , insert_stock_data


from tweets_news.newsapi import fetch_realtime_crypto_news , insert_news_data
from tweets_news.x import fetch_realtime_crypto_tweets , insert_tweet_data





connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="358WNMGF",
    database="crypto"
)
cursor = connection.cursor()



# Initialize the last insertion times
last_news_tweets_insertion_time = None
last_coins_data_insertion_time = None
last_economic_data_insertion_time = None

# Main loop
while True:
    current_time = datetime.now()

    # Insert coins data every 3 minutes
    if last_coins_data_insertion_time is None or (current_time - last_coins_data_insertion_time >= timedelta(minutes=3)):
        print("=============================== coins data ===================================")
        data_blockchain = fetch_blockchain_statistics()
        insert_blockchain_statistics_data(data=data_blockchain, cursor=cursor, connection=connection)

        data_dominance = fetch_realtime_dominance_data()
        insert_dominance_data(data=data_dominance, cursor=cursor, connection=connection)

        data_exchange = fetch_exchange_rate_data()
        insert_exchange_rate_data(data=data_exchange, cursor=cursor, connection=connection)

        data_crypto = fetch_realtime_crypto_data()
        insert_crypto_data(data=data_crypto, cursor=cursor, connection=connection)

        # Update the last coins data insertion time
        last_coins_data_insertion_time = current_time

    # Insert economic data every 3 minutes
    if last_economic_data_insertion_time is None or (current_time - last_economic_data_insertion_time >= timedelta(minutes=3)):
        print("=============================== economic data ===================================")
        data_gold = fetch_realtime_gold_data()
        insert_gold_data(data=data_gold, cursor=cursor, connection=connection)

        data_interest_rate = fetch_realtime_interest_rate_data()
        insert_interest_rate_data(data=data_interest_rate, cursor=cursor, connection=connection)

        data_stock = fetch_realtime_stock_data()
        insert_stock_data(data=data_stock, cursor=cursor, connection=connection)

        # Update the last economic data insertion time
        last_economic_data_insertion_time = current_time

    # Insert news and tweets every 30 minutes
    if last_news_tweets_insertion_time is None or (current_time - last_news_tweets_insertion_time >= timedelta(minutes=16)):
        print("=============================== news and tweets ===================================")
        data_news = fetch_realtime_crypto_news()
        insert_news_data(data=data_news, cursor=cursor, connection=connection)

        data_tweets = fetch_realtime_crypto_tweets()
        insert_tweet_data(data=data_tweets, cursor=cursor, connection=connection)

        # Update the last news and tweets insertion time
        last_news_tweets_insertion_time = current_time

    # Sleep for 3 minutes before the next iteration
    time.sleep(180)

    
