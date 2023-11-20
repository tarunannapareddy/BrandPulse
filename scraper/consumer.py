import json 
from kafka import KafkaConsumer

BROKER = '10.142.0.6:9092'
CONSUMER_TOPIC = 'events'

if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest'
    )
    for message in consumer:
        if message.value:  # Check if message is not empty
            try:
                payload = json.loads(message.value)
                print(payload)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Received an empty message")