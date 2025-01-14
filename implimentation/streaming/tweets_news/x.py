import time
import tweepy
from datetime import datetime

def fetch_realtime_crypto_tweets(bearer_token="AAAAAAAAAAAAAAAAAAAAAKoiuAEAAAAAjnmBFmX3VrVkSY274zaH6QRnUVg%3DZggvLOYiyPtuUww9lqoL9YoTSmQ3yFPJX7LZeIGvW02C2qZpSa"):
    """
    Fetch and prepare real-time cryptocurrency tweets for insertion.

    Args:
        bearer_token: The Twitter API Bearer Token.
    
    Returns:
        list: A list of tuples containing source (username), content (tweet text), and URL.
    """
    try:
        # Initialize the Tweepy client
        client = tweepy.Client(bearer_token=bearer_token)

        # Define the query and search parameters
        query = "crypto OR cryptocurrency OR bitcoin"

        # Fetch recent tweets
        response = client.search_recent_tweets(
            query=query,
            max_results=10,
            expansions="author_id",  # Request author details
            user_fields=["username"]  # Include username in user details
        )

        results = []

        # Check if the response contains data
        if response.data:
            # Create a mapping of user IDs to usernames
            users = {user["id"]: user["username"] for user in response.includes["users"]}

            # Process the tweets
            for tweet in response.data:
                username = users.get(tweet.author_id, "Unknown")  # Get username by author_id
                source = username
                content = tweet.text

                # Add to the results list
                results.append((source, content))

            return results
        else:
            print("No tweets found for the query.")
            return None

    except Exception as e:
        print(f"Error while fetching real-time tweets: {e}")
        return None



def insert_tweet_data(data, cursor, connection):
    """
    Insert real-time cryptocurrency tweet data into the sentiment table.

    Args:
        tweet_data: A list of tuples containing (source, content) for each tweet.
        cursor: Database cursor for executing SQL queries.
        connection: Database connection object.
    """
    try:
        if data:
            # Prepare the insert query
            insert_query = """
                INSERT INTO sentiment (source, content)
                VALUES (%s, %s)
            """
            counter = 0
            for record in data:
                source, content = record
                
                # Validate the data before inserting
                if not content:
                    print(f"Skipping record with missing content: {record}")
                    continue  # Skip this record if content or URL is missing
                
                # Execute the insert query
                cursor.execute(insert_query, (source, content))
                counter += 1
                if counter ==20:
                    break
            # Commit the transaction
            connection.commit()
            print(f"Inserted {len(data)} tweets into sentiment table.")
        
        else:
            print("No tweet data available to insert.")
            
    except Exception as e:
        print(f"Error while inserting tweet data into database: {e}")
