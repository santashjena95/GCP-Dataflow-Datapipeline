# Steps To Create The Dataflow Pipeline Template

## 1. First create virtual env and activate it

### i. virtualenv myvir

### ii. source vir/bin/activate

## 2. pip3 install -r requirements.txt

## 3. python3 weather_bigquery.py

## 4. Have to add "Secret Manager Secret Accessor" permissions even if we are using default compute service account with Editor access

## NOTE: when using --save_main_session don't keep any global variable or we ca get error.

## NOTE: If we have multiple functions we have to use "--save_main_session" flag or we will get not found custom function error.