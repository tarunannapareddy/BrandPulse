import json 
from kafka import KafkaConsumer

BROKER = '10.142.0.6:9092'
CONSUMER_TOPIC = 'quickstart-events'

if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest'
    )
    for message in consumer:
        print(json.loads(message.value))