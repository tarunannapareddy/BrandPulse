from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

textblob_path = 'textblob-0.17.1.tar.gz'
os.system(f"pip install --no-index --find-links=./ {textblob_path}")


from textblob import TextBlob


def get_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity
# Register the UDF
sentiment_udf = udf(get_sentiment, DoubleType())

spark = (SparkSession.builder.appName("KafkaApp")
    .master("local[*]")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

KAFKA_TOPIC_NAME = "events2"
KAFKA_BOOTSTRAP_SERVER = "10.142.0.2:9092"
sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

sampleDataframe.printSchema()

base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
schema = (StructType()
        .add("tweet", StringType())
        .add("company", StringType()))
data = base_df.select(
    from_json(col("value"), schema).getField("tweet").alias("tweet"),
    from_json(col("value"), schema).getField("company").alias("company"),
    "timestamp"
)

# Apply sentiment analysis to the 'tweet' column and add a new column 'sentiment'
result_df = data.withColumn("sentiment", sentiment_udf(col("tweet")))

query = result_df.writeStream \
    .format("console") \
    .outputMode("append")\
    .start()

query.awaitTermination()