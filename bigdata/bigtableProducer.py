import os
from datetime import datetime, timedelta
from google.cloud import bigtable
import time

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_bigtable.json"

client = bigtable.Client(project='mystical-studio-406018')
instance = client.instance('bigtable')
table_id = 'sentiment'
table = instance.table(table_id)
sentimentValues = ["POSITIVE", "NEGATIVE", "NEUTRAL"]
columnfamily = 'companyInfo'

for i in range(10):
    timestamp = datetime.now()
    row_key = 'row-{}'.format(timestamp.isoformat())
    row = table.row(row_key)

    company = "meta"
    sentiment = sentimentValues[i%3]

    row.set_cell(b'companyInfo', b'company', str(company).encode(), timestamp=timestamp)
    row.set_cell(b'companyInfo', b'sentiment', str(sentiment).encode(), timestamp=timestamp)
    resp = row.commit()
    print(f"Loaded records - {row_key} and {resp}")
    time.sleep(30)
