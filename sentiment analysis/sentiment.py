#! /usr/bin/python

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType, StringType
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

# Create a Spark session
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# Sample data with a "text" column
data = [("I love PySpark", 1),
        ("I hate big data", 0),
        ("PySpark is amazing", 1),
        ("Big data is complicated", 0)]

schema = ["text", "label"]
df = spark.createDataFrame(data, schema=schema)

# BERT Tokenizer and Model
tokenizer = AutoTokenizer.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')
model = AutoModelForSequenceClassification.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')

# Define functions for sentiment scoring and categorization
def sentiment_score(review):
    tokens = tokenizer.encode(review, return_tensors='pt')
    result = model(tokens)
    return int(torch.argmax(result.logits))+1

def categorize_sentiment(score):
    if score <= 2:
        return 'Negative'
    elif score == 3:
        return 'Neutral'
    elif score >= 4:
        return 'Positive'

# Define a UDF to apply the sentiment scoring function to Spark DataFrame
sentiment_score_udf = udf(sentiment_score)

# Apply sentiment scoring to the DataFrame
df = df.withColumn("sentiment_score", sentiment_score_udf(df["text"]))

# Define a UDF to apply the sentiment categorization function to Spark DataFrame
sentiment_category_udf = udf(categorize_sentiment, StringType())

# Apply sentiment categorization to the DataFrame
df = df.withColumn("sentiment_category", sentiment_category_udf(df["sentiment_score"]))

# Show the results
df.select("text", "sentiment_score", "sentiment_category").show(truncate=False)

# Stop the Spark session
spark.stop()
