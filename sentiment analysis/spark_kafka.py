from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import bigtable
import os

textblob_path = 'textblob-0.17.1.tar.gz'
os.system(f"pip install --no-index --find-links=./ {textblob_path}")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gs://spark_distinct_job_bucket/google_bigtable.json"
bigtable_path = 'google-cloud-bigtable-2.17.0.tar.gz'
os.system(f"pip install --no-index --find-links=./ {bigtable_path}")
from textblob import TextBlob
from google.cloud import bigtable

client = bigtable.Client(project='mystical-studio-406018')
instance = client.instance('bigtable')
table_id = 'sentiment'
table = instance.table(table_id)

def get_sentiment_label(sentiment):
    if sentiment < 0:
        return "NEGATIVE"
    elif sentiment == 0:
        return "NEUTRAL"
    else:
        return "POSITIVE"

def write_to_bigtable(row):
    time= row["timestamp"]
    company= row["company"]
    sentiment = row["sentiment"]
    tweet = row["tweet"]

    row_key = 'row-{}'.format(time.isoformat())
    bigtable_row = table.row(row_key)
    
    # Store data in Bigtable
    bigtable_row.set_cell(b'companyInfo', b'company', str(company).encode(), timestamp=time)
    bigtable_row.set_cell(b'companyInfo', b'sentiment', str(sentiment).encode(), timestamp=time)
    bigtable_row.set_cell(b'companyInfo', b'tweet', str(tweet).encode(), timestamp=time)
    bigtable_row.set_cell(b'companyInfo', b'time', str(time).encode(), timestamp=time)
    
    # Comment and print
    resp = bigtable_row.commit()
    print(f"Loaded records - {row_key} and {resp}")


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
result_df = data.withColumn("sentiment", get_sentiment_label(sentiment_udf(col("tweet"))))
result_df.foreach(write_to_bigtable)

query = result_df.writeStream \
    .format("console") \
    .outputMode("append")\
    .start()

query.awaitTermination()