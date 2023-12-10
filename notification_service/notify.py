import json 
from kafka import KafkaConsumer
import psycopg2
import os
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from google.cloud import bigtable
from datetime import datetime, timedelta
from google.cloud.bigtable.row_filters import (
    TimestampRangeFilter,
    TimestampRange
)

DB_HOST = '34.74.206.172'
DB_PORT = '5432'
DB_NAME = 'brandpulse'
DB_USER = 'postgres'
DB_PASSWORD = 'test1234'

BROKER = '10.142.0.2:9092'
CONSUMER_TOPIC = 'notification'
columnfamily = 'companyInfo'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_bigtable.json"
client = bigtable.Client(project='mystical-studio-406018')
instance = client.instance('bigtable')
table_id = 'sentiment'
table = instance.table(table_id)
image = 'sentiment_plot.png'

def read_data(company_name):
    rows = table.read_rows( filter_=TimestampRangeFilter(TimestampRange(start=datetime.now() - timedelta(minutes=360), end=datetime.now())))
    data = []
    for row in rows:
        cells = row.cells[columnfamily]
        company = cells['company'.encode('utf-8')][0].value.decode('utf-8')
        sentiment = cells['sentiment'.encode('utf-8')][0].value.decode('utf-8')
        time = pd.to_datetime(cells['time'.encode('utf-8')][0].value.decode('utf-8'))
        data.append({'company': company, 'sentiment': sentiment, 'time': time})
    df = pd.DataFrame(data)
    df['hour'] = df['time'].dt.hour
    df['sentiment_count'] = 1  # For counting occurrences
    
    # Group by hour and sentiment
    grouped_data = df.groupby(['hour', 'sentiment']).size().unstack(fill_value=0)

    print(grouped_data)
    # Plotting
    grouped_data.plot(kind='line', marker='o')
    plt.title('Sentiment Counts by Hour')
    plt.xlabel('Hour')
    plt.ylabel('Count')
    plt.savefig(image, bbox_inches='tight')

def get_data_from_db(company_name):
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        # Create a cursor object
        cursor = conn.cursor()

        # Define your SQL query to fetch data from the table (replace 'your_table_name' with your actual table name)
        sql_query = f"SELECT * FROM customer WHERE name = %s"

        # Execute the SQL query with the provided company_name parameter
        cursor.execute(sql_query, (company_name,))

        # Fetch all rows from the result
        rows = cursor.fetchall()

        # Process the data as needed
        for row in rows:
            # Access the columns of each row as row[0], row[1], etc.
            print(row)
            email = row[3]

        # Close the cursor and connection
        cursor.close()
        conn.close()

        print(email)
        
        return email

    except (Exception, psycopg2.Error) as e:
        print(f"Error while fetching data from PostgreSQL: {e}")

def send_mail(company):
    # Email configuration
    sender_email = "dcsc4403@gmail.com"
    # receiver_email = "tarunannapareddy1997@gmail.com"
    receiver_email = get_data_from_db(company)
    subject = f"BrandPulse: Public pulse for your company {company}"

    # Create message container
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # Attach image to the email
    with open(image, "rb") as img_file:
        attachment = MIMEApplication(img_file.read(), _subtype="png")
        attachment.add_header('Content-Disposition', 'attachment', filename=f"{image}")
        msg.attach(attachment)

    # Connect to the SMTP server
    smtp_server = smtplib.SMTP("smtp.gmail.com", 587)
    smtp_server.starttls()
    smtp_server.login(sender_email, "DCSC@4403123")  # Replace with your email and password

    # Send email
    smtp_server.sendmail(sender_email, receiver_email, msg.as_string())

    # Quit SMTP server
    smtp_server.quit()


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
                send_mail(company_name)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Received an empty message")
        break
