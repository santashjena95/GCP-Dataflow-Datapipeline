import apache_beam as beam
from threading import Event
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import bigquery
import xmltodict

def delete_file(filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket("sensor_data_input_demo")
    blob = bucket.blob(filename)
    blob.delete()
    print(f"Deleted file: {filename}")

def load_bigquery(data_and_filename):
    main_data, filename = data_and_filename
    client = bigquery.Client()

    dataset_id = 'main_dataset'
    table_id = 'sensor_data_table'

    table_ref = client.dataset(dataset_id).table(table_id)
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("temperature", "FLOAT"),
        bigquery.SchemaField("humidity", "FLOAT"),
        bigquery.SchemaField("luminosity", "FLOAT"),
        bigquery.SchemaField("date", "DATE")
    ]
    errors = client.insert_rows_json(table=table_ref, json_rows=main_data)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Errors occurred:", errors)
    return filename


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
    xml_bucket = "sensor_data_input_demo"
    
    storage_client = storage.Client()

    bucket = storage_client.bucket(xml_bucket)
    xml_blob = bucket.blob(file_name)

    with xml_blob.open("r") as f:
        file = xmltodict.parse(f.read())
        list_json = get_records(file)
        return list_json, file_name

def get_records(file):
    accumulated_data = []
    for record in file['sensor_data']['record']:
        accumulated_data.append(extract_data(record))
    return accumulated_data

def callback(message, received_files, processing_event):
    file_name = message.attributes.get('objectId')
    received_files.append(file_name)
    message.ack()
    processing_event.set()

def pull_messages():
    project_id = "turnkey-cove-443706-t1"
    subscription_id = "storage-notification-sub"
    received_files = []
    processing_event = Event()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    subscription_future = subscriber.subscribe(
        subscription_path, 
        lambda message: callback(message, received_files, processing_event)
    )

    print("Listening for messages...")
    
    processing_event.wait()  
    
    subscription_future.cancel()
    return received_files[0] if received_files else None

def run():
    argv = [
        "--project=turnkey-cove-443706-t1",
        "--job_name=sensordatatobq4",
        "--no_use_public_ips",
        "--save_main_session",
        "--staging_location=gs://dataflow-pipeline-poc-bucket/staging",
        "--temp_location=gs://dataflow-pipeline-poc-bucket/tmp",
        "--template_location=gs://dataflow-pipeline-poc-bucket/templates/SensorDataToBigquery",
        "--runner=DataflowRunner",
        "--setup_file=./setup.py",
        "--region=us-central1",
        "--worker_region=us-central1",
        "--subnetwork=https://www.googleapis.com/compute/v1/projects/turnkey-cove-443706-t1/regions/us-central1/subnetworks/custom-subnet",
        "--service_account_email=225425778127-compute@developer.gserviceaccount.com"
    ]
    p = beam.Pipeline(argv=argv)
    (p
      |'KickStartThePipeline' >> beam.Create(['Start'])
      |'PullPubSubMessages' >> beam.Map(lambda line: pull_messages())
      |'GetReleventData' >> beam.Map(lambda filename: parse_into_dict(filename))
      |'LoadDataInBigQuery' >> beam.Map(lambda data_and_filename: load_bigquery(data_and_filename))
      |'DeleteTheProcessedFile' >> beam.Map(lambda delete: delete_file(delete))
    )
    p.run()

if __name__ == "__main__":
    run()
