import requests
from datetime import datetime, timedelta
import logging
import json

ts = datetime.now()
new_execution_date = datetime.strftime(ts, '%Y-%m-%dT%H:%M:%SZ')
logging.info(new_execution_date)

def ext_trigger_airflow(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    url = "http://e67b-158-140-180-20.ngrok.io/api/v1/dags/example_bigquery_operations/dagRuns"

    data = {
      "conf": {},
      "logical_date": "2022-01-01T13:55:02.580Z"
    }

    data['logical_date'] = new_execution_date
    
    payload = json.dumps(data)

    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Basic YWlyZmxvdzphaXJmbG93'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))