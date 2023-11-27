from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("KafkaApp")
    .master("local[*]")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

KAFKA_TOPIC_NAME = "events"
KAFKA_BOOTSTRAP_SERVER = "10.142.0.2:9092"
sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )
sampleDataframe.printSchema()

query = sampleDataframe \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()