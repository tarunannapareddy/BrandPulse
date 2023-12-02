import os

# Cython
cython_path = 'Cython-3.0.6.tar.gz'
os.system(f"pip install --no-index --find-links=./ {cython_path}")

wheel_path = 'wheel-0.42.0.tar.gz'
os.system(f"pip install --no-index --find-links=./ {wheel_path}")

# Pyproject-metadata
pyproject_metadata_path = 'pyproject-metadata-0.7.1.tar.gz'
os.system(f"pip install --no-index --find-links=./ {pyproject_metadata_path}")

# Setuptools
setuptools_path = 'setuptools-69.0.2.tar.gz'
os.system(f"pip install --no-index --find-links=./ {setuptools_path}")

# Transformers
transformers_path = 'transformers-4.35.2.tar.gz'
os.system(f"pip install --no-index --find-links=./ {transformers_path}")

# Torch
torch_path = 'torch-2.1.1-cp311-cp311-manylinux2014_aarch64.whl'
os.system(f"pip install --no-index --find-links=./ {torch_path}")

# Numpy
numpy_path = 'numpy-1.26.2.tar.gz'
os.system(f"pip install --no-index --find-links=./ {numpy_path}")

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType, StringType
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import numpy as np

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

sentiment_score_udf = udf(sentiment_score)

# Apply sentiment scoring to the DataFrame
df = df.withColumn("sentiment_score", sentiment_score_udf(df["text"]))
sentiment_category_udf = udf(categorize_sentiment, StringType())
df = df.withColumn("sentiment_category", sentiment_category_udf(df["sentiment_score"]))
df.select("text", "sentiment_score", "sentiment_category").show(truncate=False)

spark.stop()