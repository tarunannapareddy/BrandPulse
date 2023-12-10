import os

nltk_path = 'nltk-3.8.1.zip'
os.system(f"pip install --no-index --find-links=./ {nltk_path}")

import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from nltk.sentiment import SentimentIntensityAnalyzer

spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# Download the VADER sentiment analysis model (you only need to run this once)
nltk.download('vader_lexicon')

# Create a SentimentIntensityAnalyzer object
sid = SentimentIntensityAnalyzer()

# Example sentences for sentiment analysis
sentences = [
    "I love this product, it's amazing!",
    "Terrible weather today",
    "Neutral sentence without strong sentiment.",
    "I'm not sure about this movie.",
    "The concert was a disappointment.",
    "The movie was okayish. Neither good nor bad."
]

schema = StructType([StructField("sentence", StringType(), True)])
df = spark.createDataFrame([(sentence,) for sentence in sentences], schema)

def analyze_sentiment(sentence):
    sentiment_score = sid.polarity_scores(sentence)
    max_sentiment = max(sentiment_score, key=lambda k: sentiment_score[k] if k != 'compound' else float('-inf'))

    if max_sentiment == 'pos':
        return "Positive sentiment"
    elif max_sentiment == 'neg':
        return "Negative sentiment"
    else:
        return "Neutral sentiment"

sentiment_udf = udf(analyze_sentiment, StringType())
result_df = df.withColumn("sentiment", sentiment_udf("sentence"))
result_df.show(truncate=False)