import json 
from kafka import KafkaConsumer
import os
from google.cloud import bigtable
from datetime import datetime, timedelta

BROKER = '10.142.0.2:9092'
CONSUMER_TOPIC = 'notification'
columnfamily = 'companyInfo'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigdata/google_bigtable.json"
client = bigtable.Client(project='mystical-studio-406018')
instance = client.instance('bigtable')
table_id = 'sentiment'
table = instance.table(table_id)

def read_data(company_name):
    # Get the current time and calculate the timestamp for the last 1 hour
    current_time = datetime.utcnow()
    one_hour_ago = current_time - timedelta(hours=1)
    one_hour_ago_timestamp = int(one_hour_ago.timestamp())

    # Define a filter to get rows where 'company' column matches and 'time' is within the last 1 hour
    filter_str = f'qualifier=company,value={company_name}'
    
    # Apply the filter to the row keys
    row_filter = bigtable.row_filters.RowFilterChain(
        filters=[
            bigtable.row_filters.FamilyNameRegexFilter(columnfamily),
            bigtable.row_filters.RowFilter(
                row_filter=bigtable.row_filters.RowFilterChain(
                    filters=[
                        bigtable.row_filters.ColumnRangeFilter(columnfamily, start=b'company', end=b'company\uffff'),
                        bigtable.row_filters.ValueRegexFilter(f'^{company_name}$')
                    ]
                )
            ),
            bigtable.row_filters.TimestampRangeFilter(
                start_timestamp_micros=(one_hour_ago_timestamp * 1_000_000),  # Convert to microseconds
                end_timestamp_micros=bigtable.ServerTimestamp,
                inclusive_start=True,
                inclusive_end=True
            )
        ]
    )

    # Read rows with the applied filter
    rows = table.read_rows(filter_=row_filter)

    for row in rows:
        cells = row.cells[columnfamily]
        company = cells.get('company'.encode('utf-8'), [None])[0]
        sentiment = cells.get('sentiment'.encode('utf-8'), [None])[0]
        time = cells.get('time'.encode('utf-8'), [None])[0]
        print(f"loaded data {company} and {sentiment} and {time}")


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
                company_name = payload['company']
                read_data(company_name)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Received an empty message")