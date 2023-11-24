import os
from datetime import datetime, timedelta
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import (
    TimestampRangeFilter,
    TimestampRange
)
columnfamily = 'companyInfo'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigdata/google_bigtable.json"

client = bigtable.Client(project='mystical-studio-406018')
instance = client.instance('bigtable')
table_id = 'sentiment'
table = instance.table(table_id)

rows = table.read_rows(
    filter_=TimestampRangeFilter(TimestampRange(start=datetime.now() - timedelta(minutes=30), end=datetime.now()))
)

for row in rows:
    company_cell = row.cells[columnfamily]['company'.encode('utf-8')][0]
    sentiment_cell = row.cells[columnfamily]['sentiment'.encode('utf-8')][0]
    company = company_cell.value.decode('utf-8')
    sentiment = sentiment_cell.value.decode('utf-8')
    time = (company_cell.timestamp)
    print(f"loaded data {company} and {sentiment} and {time}")
