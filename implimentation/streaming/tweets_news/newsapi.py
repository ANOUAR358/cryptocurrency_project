import time
import requests
from datetime import datetime

def fetch_realtime_crypto_news(keywords=["cryptocurrency", "bitcoin", "ethereum", "blockchain"]):
    """
    Fetch and prepare real-time cryptocurrency news data for insertion.

    Args:
        keywords: A list of keywords to search for in the news articles.
    
    Returns:
        list: A list of tuples containing source, title, and URL for each article.
    """
    try:
        API_KEY = '8c5f4316d7be4cd68af1e68c64afc3eb'  
        query = " OR ".join(keywords)  
        url = f"https://newsapi.org/v2/everything?q={query}&language=en&sortBy=publishedAt&apiKey={API_KEY}"

        # Make the API request
        response = requests.get(url)
        data = response.json()

        # Prepare the news data
        news_data = []
        if "articles" in data and data["articles"]:
            counter = 0
            for article in data["articles"]:
                source = article.get('source', {}).get('name', 'Unknown Source')
                title = article.get('title', 'No Title')

                # Add to the news data list
                news_data.append((source, title))
                counter += 1
                if counter >= 30:
                    break

            return news_data
        else:
            print("No articles found in the API response.")
            return None

    except Exception as e:
        print(f"Error while fetching real-time news: {e}")
        return None

def insert_news_data(data, cursor, connection):
    """
    Insert real-time news data into the sentiment table.

    Args:
        news_data: A list of tuples containing (source, title) for each article.
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
            
            for record in data:
                source, title = record
                
                # Validate the data before inserting
                if not title:
                    print(f"Skipping record with missing title: {record}")
                    continue  # Skip this record if title or URL is missing
                
                # Execute the insert query
                cursor.execute(insert_query, (source, title))
            
            # Commit the transaction
            connection.commit()
            print(f"Inserted {len(data)} news records into sentiment table.")
        
        else:
            print("No news data available to insert.")
            
    except Exception as e:
        print(f"Error while inserting news data into database: {e}")


