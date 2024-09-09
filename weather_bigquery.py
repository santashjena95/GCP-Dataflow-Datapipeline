# https://api.openweathermap.org/data/2.5/weather?lat=18.496668&lon=73.941666&appid={API_KEY}
# https://openweathermap.org/current
# https://home.openweathermap.org/api_keys
# https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

import requests
import json
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import secretmanager
import apache_beam as beam
import pandas

def get_weather_data(city):
    # OpenWeatherMap API key
    project_id = "data-engineering-poc-435112"
    client = secretmanager.SecretManagerServiceClient()
    secret = f"projects/{project_id}/secrets/weather_api_key/versions/latest"
    key = client.access_secret_version(name=secret)
    API_KEY = key.payload.data.decode("UTF-8")

    base_url = f"http://api.openweathermap.org/data/2.5/weather?q={city},IN&appid={API_KEY}&units=metric"
    response = requests.get(base_url)
    
    if response.status_code == 200:
        data = response.json()
        sunrise_utc_time = datetime.utcfromtimestamp(data["sys"]["sunrise"])
        sunrise_ist_time = sunrise_utc_time + timedelta(hours=5, minutes=30)
        sunset_utc_time = datetime.utcfromtimestamp(data["sys"]["sunset"])
        sunset_ist_time = sunset_utc_time + timedelta(hours=5, minutes=30)
        utc_time = datetime.utcfromtimestamp(data["dt"])
        ist_time = utc_time + timedelta(hours=5, minutes=30)
        return {
            "city": city,
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather_update": data["weather"][0]["description"],
            "temperature_feels_like": data["main"]["feels_like"],
            "sunrise_time": sunrise_ist_time.strftime("%Y-%m-%d %H:%M:%S IST"),
            "sunset_time": sunset_ist_time.strftime("%Y-%m-%d %H:%M:%S IST"),
            "wind_speed": data["wind"]["speed"],
            "record_date_time": ist_time.strftime("%Y-%m-%d %H:%M:%S IST")
        }
    else:
        return None

def weather_data_report():
    weather_data = []

    # List of districts in Maharashtra
    maharashtra_districts = [
        "Mumbai", "Pune", "Nagpur", "Thane", "Nashik", "Aurangabad", "Solapur", "Kolhapur",
        "Amravati", "Jalgaon", "Akola", "Latur", "Dhule", "Ahmednagar", "Chandrapur",
        "Parbhani", "Jalna", "Buldhana", "Satara", "Sangli", "Beed", "Yavatmal", "Wardha",
        "Osmanabad", "Nandurbar", "Hingoli", "Washim", "Gondia", "Ratnagiri", "Bhandara"
    ]
    for district in maharashtra_districts:
        data = get_weather_data(district)
        if data:
            weather_data.append(data)
            print(f"Retrieved data for {district}")
        else:
            print(f"Failed to retrieve data for {district}")
    dataframe = pandas.DataFrame(
        weather_data,
        columns=[
            "city",
            "temperature",
            "humidity",
            "weather_update",
            "temperature_feels_like",
            "sunrise_time",
            "sunset_time",
            "wind_speed",
            "record_date_time",
        ],
    )
    dataframe = dataframe.astype({
    'city': 'string',
    'temperature': 'string',
    'humidity': 'string',
    'weather_update': 'string',
    'temperature_feels_like': 'string',
    'sunrise_time': 'string',
    'sunset_time': 'string',
    'wind_speed': 'string',
    'record_date_time': 'string'
})
    client = bigquery.Client()
    table_id = "data-engineering-poc-435112.weather_data.maharashtra_weather_report"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("temperature", "STRING"),
            bigquery.SchemaField("humidity", "STRING"),
            bigquery.SchemaField("weather_update", "STRING"),
            bigquery.SchemaField("temperature_feels_like", "STRING"),
            bigquery.SchemaField("sunrise_time", "STRING"),
            bigquery.SchemaField("sunset_time", "STRING"),
            bigquery.SchemaField("wind_speed", "STRING"),
            bigquery.SchemaField("record_date_time", "STRING"),
        ],
    )
    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )
    job.result()
    print(f"\nWeather data saved to {table_id} BigQuery table")

def run():
    argv = [
        "--project=data-engineering-poc-435112",
        "--job_name=weatherdataetl",
        "--no_use_public_ips",
        "--save_main_session",
        "--staging_location=gs://dataflow-pipeline-poc-bucket/staging",
        "--temp_location=gs://dataflow-pipeline-poc-bucket/tmp",
        "--template_location=gs://dataflow-pipeline-poc-bucket/templates/WeatherReportToBigquery",
        "--runner=DataflowRunner",
        "--setup_file=./setup.py",
        "--region=us-east4",
        "--worker_region=us-east4",
        "--subnetwork=https://www.googleapis.com/compute/v1/projects/data-engineering-poc-435112/regions/us-east4/subnetworks/dataflow-subnet",
        "--service_account_email=917426886994-compute@developer.gserviceaccount.com"
    ]
    p = beam.Pipeline(argv=argv)
    (p
      |'KickStartThePipeline' >> beam.Create(['Start'])
      |'ImportingWeatherDatatoGoogleBigQuery' >> beam.Map(lambda line: weather_data_report())
    )
    p.run()

if __name__ == "__main__":
    run()