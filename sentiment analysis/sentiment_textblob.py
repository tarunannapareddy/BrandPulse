from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from textblob import TextBlob

# Create a Spark session
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# Sample data with a "text" column
data = [("I love PySpark",),
        ("I hate big data",),
        ("PySpark is amazing",),
        ("Big data is complicated",)]

schema = ["text"]
df = spark.createDataFrame(data, schema=schema)

# Define a UDF to apply sentiment analysis to text
def analyze_sentiment(text):
    blob = TextBlob(text)
    sentiment = "positive" if blob.sentiment.polarity > 0 else "negative" if blob.sentiment.polarity < 0 else "neutral"
    return sentiment

# Create a Spark UDF
sentiment_udf = udf(analyze_sentiment, StringType())

# Apply sentiment analysis to the DataFrame
df = df.withColumn("sentiment", sentiment_udf(df["text"]))

# Show the results
df.select("text", "sentiment").show(truncate=False)

# Stop the Spark session
spark.stop()