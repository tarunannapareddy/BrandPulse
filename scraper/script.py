from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google.oauth2 import service_account
from kafka import KafkaProducer
import json
import time

BROKER = '10.142.0.2:9092'
PRODUCER_TOPIC = 'events2'

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '16IQ9G3f3YRUTa1MCeZBnq2km81qwR-GOkgO8yIYgcTU'
sheet_range_list = ['Sheet1!A1:B10', 'Sheet2!A1:B10', 'Sheet3!A1:B10', 'Sheet4!A1:B10', 'Sheet5!A1:B10']
SERVICE_ACCOUNT_FILE = 'datacenter_keys.json'
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
            for SAMPLE_RANGE_NAME in sheet_range_list:
                sheet = service.spreadsheets()
                result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                            range=SAMPLE_RANGE_NAME).execute()
                values = result.get('values', [])

                for row in values:
                    message = {'tweet':row[0], 'company':row[1]}
                    print(message)
                    producer.send(PRODUCER_TOPIC, message)
                    time.sleep(1)
                time.sleep(10)
            time.sleep(60)

    except HttpError as err:
        print(err)


if __name__ == '__main__':
    main()