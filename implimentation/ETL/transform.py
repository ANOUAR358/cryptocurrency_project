import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Fonction pour appliquer l'analyse de sentiment sur les données extraites
def apply_sentiment_analysis(data):
    try:
        # Initialiser le modèle d'analyse de sentiment
        nltk.download('vader_lexicon')  # Télécharger le lexique si ce n'est pas déjà fait
        sia = SentimentIntensityAnalyzer()

        result = []
        for row in data:
            timestamp = row[0]
            content = row[1]

            # Appliquer l'analyse de sentiment
            sentiment_score = sia.polarity_scores(content)
            compound_score = sentiment_score['compound']

            # Déterminer le sentiment basé sur le score 'compound'
            if compound_score >= 0.5:
                sentiment = 'positive'
            elif compound_score <= -0.5:
                sentiment = 'negative'
            else:
                sentiment = 'neutral'

            # Créer un dictionnaire pour chaque enregistrement
            record = {
                'timestamp': timestamp,
                'score': compound_score,  # Inclure seulement le score 'compound'
                'sentiment': sentiment
            }
            result.append(record)

        print(f"Applied sentiment analysis to {len(data)} records")
        return result

    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        return []  # Return empty list if an error occurs

from datetime import datetime

def transforme_date_dimensions(timestamp):
    """
    Extract date dimensions from a given timestamp.

    Parameters:
        timestamp (str): A string representing a timestamp in the format 'YYYY-MM-DD HH:MM:SS'.

    Returns:
        dict: A dictionary with extracted date dimensions.
    """
    try:
        # Parse the timestamp
        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        # Extract dimensions
        date_dimensions = {
            "timestamp": timestamp,
            "day": dt.day,
            "month": dt.month,
            "year": dt.year,
            "hour": dt.hour,
            "day_of_week": dt.strftime("%A"),  # Full day name
            "week": dt.isocalendar()[1]         # Week number of the year
        }

        return date_dimensions
    except ValueError as e:
        raise ValueError(f"Invalid timestamp format: {e}")



def transforme_date_dimensions(timestamp):
    """
    Extract date dimensions from a given timestamp.

    Parameters:
        timestamp (datetime.datetime or str): A datetime object or a string representing a timestamp.

    Returns:
        dict: A dictionary with extracted date dimensions.
    """
    try:
        # Ensure the timestamp is a datetime object
        if isinstance(timestamp, str):
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        elif isinstance(timestamp, datetime):
            dt = timestamp
        else:
            raise TypeError(f"Invalid timestamp type: {type(timestamp)}")

        # Extract dimensions
        date_dimensions = {
            "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),  # Ensure timestamp is in string format
            "day": dt.day,
            "month": dt.month,
            "year": dt.year,
            "hour": dt.hour,
            "day_of_week": dt.strftime("%A"),  # Full day name
            "week": dt.isocalendar()[1]         # Week number of the year
        }

        return date_dimensions
    except Exception as e:
        print(f"Error transforming date dimensions: {e}")
        raise

