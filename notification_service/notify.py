import json 
from kafka import KafkaConsumer
import os
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigtable
from datetime import datetime, timedelta
from google.cloud.bigtable.row_filters import (
    TimestampRangeFilter,
    TimestampRange
)


BROKER = '10.142.0.2:9092'
CONSUMER_TOPIC = 'notification'
columnfamily = 'companyInfo'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_bigtable.json"
client = bigtable.Client(project='mystical-studio-406018')
instance = client.instance('bigtable')
table_id = 'sentiment'
table = instance.table(table_id)

def read_data(company_name):
    rows = table.read_rows( filter_=TimestampRangeFilter(TimestampRange(start=datetime.now() - timedelta(minutes=360), end=datetime.now())))
    data = []
    for row in rows:
        cells = row.cells[columnfamily]
        company = cells['company'.encode('utf-8')][0].value.decode('utf-8')
        sentiment = cells['sentiment'.encode('utf-8')][0].value.decode('utf-8')
        time = cells['time'.encode('utf-8')][0].value.decode('utf-8')
        print(f"loaded data {company} and {sentiment} and {time}")
        data.append({'company': company, 'sentiment': sentiment, 'time': time})
    df = pd.DataFrame(data)
    df['hour'] = df['time'].dt.hour
    df['sentiment_count'] = 1  # For counting occurrences
    
    # Group by hour and sentiment
    grouped_data = df.groupby(['hour', 'sentiment']).count().reset_index()

    # Pivot the table for better visualization
    pivot_table = grouped_data.pivot(index='hour', columns='sentiment', values='sentiment_count').fillna(0)

    # Plotting
    pivot_table.plot(kind='bar', stacked=True)
    plt.title('Sentiment Counts by Hour')
    plt.xlabel('Hour')
    plt.ylabel('Count')
    plt.savefig('sentiment_counts_plot.png', bbox_inches='tight')
    plt.show()


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