import json 
from kafka import KafkaConsumer
import os
from datetime import datetime, timedelta
from google.cloud import bigtable
from datetime import datetime, timedelta
from google.cloud.bigtable.row_filters import (
    TimestampRangeFilter,
    TimestampRange
)


BROKER = '10.142.0.2:9092'
CONSUMER_TOPIC = 'notification'
columnfamily = 'companyInfo'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigdata/google_bigtable.json"
client = bigtable.Client(project='mystical-studio-406018')
instance = client.instance('bigtable')
table_id = 'sentiment'
table = instance.table(table_id)

def read_data(company_name):
    rows = table.read_rows( filter_=TimestampRangeFilter(TimestampRange(start=datetime.now() - timedelta(minutes=300), end=datetime.now())))

    for row in rows:
        cells = row.cells[columnfamily]
        company = cells['company'.encode('utf-8')][0].value.decode('utf-8')
        sentiment = cells['sentiment'.encode('utf-8')][0].value.decode('utf-8')
        time = cells['time'.encode('utf-8')][0].value.decode('utf-8')
        print(f"loaded data {company} and {sentiment} and {time}")


if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset='latest'
    )
    for message in consumer:
        if message.value:  # Check if message is not empty
            try:
                payload = json.loads(message.value)
                print(payload)
                company_name = payload['company']
                read_data(company_name)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Received an empty message")