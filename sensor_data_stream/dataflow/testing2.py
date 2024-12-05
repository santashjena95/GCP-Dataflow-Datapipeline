import os
import io
import time
import csv
from xml.etree import ElementTree as ET
from google.cloud import pubsub_v1
from google.cloud import storage

# Project and subscription details
project_id = "turnkey-cove-443706-t1"
subscription_id = "storage-notification-sub"

# Create a subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Get the full path of the subscription
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def convert_xml_csv(file_name):
    xml_bucket = "sensor_data_input_demo"
    # Initialize GCS client
    storage_client = storage.Client()

    # Get the bucket and blob
    bucket = storage_client.bucket(xml_bucket)
    xml_blob = bucket.blob(file_name)

    # Download the XML content
    xml_content = xml_blob.download_as_text()

    # Parse the XML content
    root = ET.fromstring(xml_content)

    # Prepare CSV content
    csv_buffer = io.StringIO()
    csvwriter = csv.writer(csv_buffer)

    # Write the header row
    csvwriter.writerow(['timestamp', 'temperature', 'humidity', 'luminosity'])

    # Iterate through each record in the XML
    for record in root.findall('record'):
        timestamp = record.find('timestamp').text
        temperature = record.find('temperature').text
        humidity = record.find('humidity').text
        luminosity = record.find('luminosity').text

        # Write the data to the CSV buffer
        csvwriter.writerow([timestamp, temperature, humidity, luminosity])

    # Upload the CSV content to GCS
    csv_blob_name = file_name.replace("xml", "csv")
    bucket_name = "sensor_data_input_csv_demo"
    csv_bucket = storage_client.bucket(bucket_name)
    csv_blob = csv_bucket.blob(csv_blob_name)
    csv_blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"CSV file '{csv_blob_name}' has been created successfully in the bucket '{bucket_name}'")


def callback(message):
    file_name = message.attributes.get('objectId')
    message.ack()
    convert_xml_csv(file_name)

def pull_messages():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        # Keep the main thread alive
        while True:
            time.sleep(60)
            print(streaming_pull_future)
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Streaming pull future canceled.")

if __name__ == "__main__":
    pull_messages()
