from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google.oauth2 import service_account
from kafka import KafkaProducer
import json
import time

BROKER = '10.142.0.2:9092'
PRODUCER_TOPIC = 'events'

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '16IQ9G3f3YRUTa1MCeZBnq2km81qwR-GOkgO8yIYgcTU'
SAMPLE_RANGE_NAME = 'Sheet1!A1:B101'
SERVICE_ACCOUNT_FILE = 'scraper/datacenter_keys.json'
credentails = None
credentials = service_account.Credentials.from_service_account_file( SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=serializer
)

def main():
    try:
        service = build('sheets', 'v4', credentials=credentials)

        # Call the Sheets API
        while True:
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range=SAMPLE_RANGE_NAME).execute()
            values = result.get('values', [])

            for row in values:
                message = {'tweet':row[0], 'company':row[1]}
                json_message = json.dumps(message)
                print(json_message)
                producer.send(PRODUCER_TOPIC, json_message)
                time.sleep(2)
            time.sleep(60)

    except HttpError as err:
        print(err)


if __name__ == '__main__':
    main()