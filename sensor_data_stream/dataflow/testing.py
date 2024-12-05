import os
import time
import csv
from google.cloud import pubsub_v1
from google.cloud import storage
import xmltodict
from threading import Event

def extract_data(record):
    ingestion_date = record['timestamp'][:10]
    extracted = {
        'timestamp': record['timestamp'],
        'temperature': record['temperature']['#text'],
        'humidity': record['humidity']['#text'],
        'luminosity': record['luminosity']['#text'],
        'date': ingestion_date
    }
    return extracted

def parse_into_dict(file_name):
    # Storage bucket name
    xml_bucket = "sensor_data_input_demo"
    
    # Initialize GCS client
    storage_client = storage.Client()

    # Get the bucket and blob
    bucket = storage_client.bucket(xml_bucket)
    xml_blob = bucket.blob(file_name)

    with xml_blob.open("r") as f:
        file = xmltodict.parse(f.read())
        get_records(file)

def get_records(file):
    accumulated_data = []
    for record in file['sensor_data']['record']:
        accumulated_data.append(extract_data(record))
    print(accumulated_data)

def callback(message, received_files, processing_event):
    file_name = message.attributes.get('objectId')
    received_files.append(file_name)  # Store the file name
    message.ack()
    processing_event.set()  # Signal that a file name has been received

def pull_messages():
    project_id = "turnkey-cove-443706-t1"
    subscription_id = "storage-notification-sub"
    received_files = []  # Shared list to store received file names
    processing_event = Event()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    subscription_future = subscriber.subscribe(
        subscription_path, 
        lambda message: callback(message, received_files, processing_event)
    )

    print("Listening for messages...")
    
    # Wait for a file name to be received
    processing_event.wait()  
    
    #Returning the first received file name and stopping subscriber after the first message
    subscription_future.cancel()
    return received_files[0] if received_files else None

if __name__ == "__main__":
    file_name = pull_messages()
    print(file_name)
