from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google.oauth2 import service_account

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '16IQ9G3f3YRUTa1MCeZBnq2km81qwR-GOkgO8yIYgcTU'
SAMPLE_RANGE_NAME = 'Sheet1!A1:B101'
SERVICE_ACCOUNT_FILE = 'keys.json'
credentails = None
credentials = service_account.Credentials.from_service_account_file( SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Output file to write the data
OUTPUT_FILE = 'output.txt'

def main():
    try:
        service = build('sheets', 'v4', credentials=credentials)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        with open(OUTPUT_FILE, 'w') as output_file:
            for row in values:
                print(row)
                output_file.write(', '.join(row) + '\n')

        print(f'Data written to {OUTPUT_FILE}')
    except HttpError as err:
        print(err)


if __name__ == '__main__':
    main()
