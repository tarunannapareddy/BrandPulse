from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json
from datetime import datetime

textblob_path = 'textblob-0.17.1.tar.gz'
os.system(f"pip install --no-index --find-links=./ {textblob_path}")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_bigtable.json"
bigtable_path = 'google-cloud-bigtable-2.17.0.tar.gz'
os.system(f"pip install --no-index --find-links=./ {bigtable_path}")
redis_path = 'redis-5.0.1.tar.gz'
os.system(f"pip install --no-index --find-links=./ {redis_path}")
kafka_path = 'kafka-python-2.0.2.tar.gz'
os.system(f"pip install --no-index --find-links=./ {kafka_path}")
from kafka import KafkaProducer
from textblob import TextBlob
from google.cloud import bigtable
import redis

redis_host = '10.183.205.163'
redis_port = 6379
KAFKA_TOPIC_NAME = "events2"
KAFKA_TOPIC_NOTIFICATION = "notification"
KAFKA_BOOTSTRAP_SERVER = "10.142.0.2:9092"

def get_sentiment_label(sentiment):
    return when(sentiment < 0, "NEGATIVE").when(sentiment == 0, "NEUTRAL").otherwise("POSITIVE")

def serializer(message):
    return json.dumps(message).encode('utf-8')

def write_to_kafka(companies):
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    value_serializer=serializer)

    for row in companies:
        company = row["company"]
        if r.get(company) is None:
            r.set(company, str(datetime.now()), px= 9000)
            kafka_message = {'company': company}
            producer.send(KAFKA_TOPIC_NOTIFICATION, kafka_message)
            print(f"published kafka message for company {company}")
        else:
            print(f"company already notified at time {r.get(company)} and time now is {datetime.now()}")

def write_to_bigtable(batch_df, epoch_id):
    # This function will be called for each batch of data in the streaming DataFrame
    batch_df.foreach(lambda row: write_row_to_bigtable(row))
    batch_df.show(truncate=False)
    distinct_companies = batch_df.select("company").distinct().collect()
    write_to_kafka(distinct_companies)

def write_row_to_bigtable(row):
    client = bigtable.Client(project='mystical-studio-406018')
    instance = client.instance('bigtable')
    table_id = 'sentiment'
    table = instance.table(table_id)

    time= row["timestamp"]
    company= row["company"]
    sentiment = row["sentiment_label"]
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


sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
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
result_df = result_df.withColumn("sentiment_label", get_sentiment_label(col("sentiment")))

query = result_df.writeStream \
    .foreachBatch(write_to_bigtable) \
    .outputMode("append")\
    .start()

query.awaitTermination()

