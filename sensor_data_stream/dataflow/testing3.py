from google.cloud import bigquery
import os
import json


# Initialize a client for BigQuery
client = bigquery.Client()

# Your dataset and table you want to write to
dataset_id = 'main_dataset'
table_id = 'demo_table'

# Set the dataset and table reference
table_ref = client.dataset(dataset_id).table(table_id)

# If table is not yet created in the dataset, we can create it with the schema we describe
schema = [
    bigquery.SchemaField("timestamp", "STRING"),
    bigquery.SchemaField("temperature", "STRING"),
    bigquery.SchemaField("humidity", "STRING"),
    bigquery.SchemaField("luminosity", "STRING"),
    bigquery.SchemaField("date", "STRING")
]

table = bigquery.Table(table_ref, schema=schema)

# Data to insert (make sure to convert your data to fit the schema)
rows_to_insert = [{"timestamp": "2024-12-05 13:45:55", "temperature": "29.68", "humidity": "51.87", "luminosity": "406.67", "date": "2024-12-05"}, {"timestamp": "2024-12-05 13:46:55", "temperature": "30.0", "humidity": "68.98", "luminosity": "112.0", "date": "2024-12-05"}, {"timestamp": "2024-12-05 13:47:55", "temperature": "28.57", "humidity": "67.28", "luminosity": "211.55", "date": "2024-12-05"}, {"timestamp": "2024-12-05 13:48:55", "temperature": "29.96", "humidity": "58.47", "luminosity": "965.57", "date": "2024-12-05"}, {"timestamp": "2024-12-05 13:49:55", "temperature": "25.32", "humidity": "50.47", "luminosity": "420.27", "date": "2024-12-05"}, {"timestamp": "2024-12-05 13:50:55", "temperature": "20.91", "humidity": "36.23", "luminosity": "674.2", "date": "2024-12-05"}, {"timestamp": "2024-12-05 13:51:55", "temperature": "25.22", "humidity": "65.66", "luminosity": "583.37", "date": "2024-12-05"}, {"timestamp": "2024-12-05 13:52:55", "temperature": "27.75", "humidity": "34.93", "luminosity": "648.78", "date": "2024-12-05"}]

errors = client.insert_rows_json(table=table_ref, json_rows=rows_to_insert)

if errors == []:
    print("New rows have been added.")
else:
    print("Errors occurred:", errors)

