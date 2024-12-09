import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub
from google.cloud import storage
from google.cloud import bigquery
import xmltodict
import json
import logging

class ParseXMLDoFn(beam.DoFn):
    def process(self, element):
        file_name = element
        storage_client = storage.Client()
        bucket = storage_client.bucket("sensor_data_input_demo")
        blob = bucket.blob(file_name)
        
        with blob.open("r") as f:
            file = xmltodict.parse(f.read())
            records = self.get_records(file, file_name)
            for record in records:
                yield record

    def get_records(self, file, file_name):
        accumulated_data = []
        storage_client = storage.Client()
        bucket = storage_client.bucket("sensor_data_input_demo")
        blob = bucket.blob(file_name)
        blob.delete()
        logging.info(f"Deleted file: {file_name}")
        for record in file['sensor_data']['record']:
            accumulated_data.append(self.extract_data(record))
        return accumulated_data

    def extract_data(self, record):
        ingestion_date = record['timestamp'][:10]
        return {
            'timestamp': record['timestamp'],
            'temperature': float(record['temperature']['#text']),
            'humidity': float(record['humidity']['#text']),
            'luminosity': float(record['luminosity']['#text']),
            'date': ingestion_date
        }


def run():
    project_id = "turnkey-cove-443706-t1"
    subscription_id = "storage-notification-sub"

    options = PipelineOptions([
        "--project=turnkey-cove-443706-t1",
        "--job_name=sensordatatobq001",
        "--no_use_public_ips",
        "--save_main_session",
        "--staging_location=gs://dataflow-pipeline-poc-bucket/staging",
        "--temp_location=gs://dataflow-pipeline-poc-bucket/tmp",
        "--runner=DataflowRunner",
        "--setup_file=./setup.py",
        "--region=us-central1",
        "--worker_region=us-central1",
        "--subnetwork=https://www.googleapis.com/compute/v1/projects/turnkey-cove-443706-t1/regions/us-central1/subnetworks/custom-subnet",
        "--service_account_email=225425778127-compute@developer.gserviceaccount.com"
    ])
    options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)

    pubsub_data = (p
     | 'ReadFromPubSub' >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_id}',
                                          with_attributes=True)
     | 'ExtractFileName' >> beam.Map(lambda msg: msg.attributes.get('objectId'))
    )

    parsed_data = (pubsub_data
     | 'ParseXML' >> beam.ParDo(ParseXMLDoFn())
    )

    _ = (parsed_data
     | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
         'turnkey-cove-443706-t1:main_dataset.sensor_data_table',
         schema='timestamp:TIMESTAMP,temperature:FLOAT,humidity:FLOAT,luminosity:FLOAT,date:DATE',
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )


    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()
