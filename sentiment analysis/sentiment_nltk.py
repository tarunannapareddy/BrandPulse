import os

nltk_path = 'nltk-3.8.1.zip'
os.system(f"pip install --no-index --find-links=./ {nltk_path}")

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Download the VADER sentiment analysis model
nltk.download('vader_lexicon')

# Create a SentimentIntensityAnalyzer object
sid = SentimentIntensityAnalyzer()

# Example sentences for sentiment analysis
sentences = [
    "I love this product, it's amazing!",
    "The weather is terrible today.",
    "Neutral sentence without strong sentiment.",
    "I'm not sure about this movie.",
    "The concert was a disappointment.",
    "The movie was okayish. Neither good nor bad.|"
]

# Analyze the sentiment of each sentence
for sentence in sentences:
    sentiment_score = sid.polarity_scores(sentence)
    print(f"Sentence: {sentence}")
    print(f"Sentiment Score: {sentiment_score}")

    # Determine the sentiment based on the highest score
    max_sentiment = max(sentiment_score, key=sentiment_score.get)

    if max_sentiment == 'pos':
        print("Positive sentiment")
    elif max_sentiment == 'neg':
        print("Negative sentiment")
    else:
        print("Neutral sentiment")
    
    print("\n")