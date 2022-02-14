from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow import DAG
from alert import slack_alerts


DAG_ID = 'api_to_postgres_ingestion_dag'

configs = Variable.get(DAG_ID, deserialize_json=True)

default_args = {
    'owner': 'Angga Pradikta',
    'depends_on_past': False,
    'start_date': configs['start_date'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': slack_alerts.task_send_failure_slack_alert
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Ingest API to Postgres',
    tags=['api', 'postgres'],
    schedule_interval=None)


def load_api_data_to_postgres():
    # 1. Get API data
    # 2. Transfrom pake Pandas
    # 3. Load to Postgres
    #Kalau pakai 3 function berdasarkan pembagian diatas. Xcoms
    #atau bikin custom airflow operator
    url = "https://jsonmock.hackerrank.com/api/food_outlets?city=Seattle"

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    df = pd.json_normalize(response.json()['data'])
    df = df.rename(columns={'user_rating.average_rating': 'average_rating', 'user_rating.votes': 'votes'})

    logging.info('df is \n{}'.format(df.head()))

    with open('/opt/airflow/dags/example_api_to_postgre/api_to_postgres.json', 'r') as j:
        contents = json.loads(j.read())

    list_schema = []
    for c in contents:
        col_name = c['column_name']
        data_type = c['column_type']
        constraint = c['is_null_able']
        x = [col_name, data_type, constraint]
        list_schema.append(x)

    t = []
    for w in list_schema:
        s = ' '.join(w)
        t.append(s)

    create_schema = """ CREATE TABLE IF NOT EXISTS api_to_postgres {}; """.format(tuple(t)).replace("'", "")

    #sebenernya bagusnya pake PostgreHook
    conn = psycopg2.connect(database="shipping_orders",
                            user='postgres', password='postgres', 
                            host='host.docker.internal', port='5432'
    )

    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(create_schema)
    #1. Dari Pandas df ke postgres -> insert_query
    #2. Dari pandas df save dulu ke csv -> bulk copy ke postgres -> best practice
    engine = create_engine('postgresql://postgres:postgres@host.docker.internal:5432/shipping_orders')
    df.to_sql('api_to_postgres', engine, if_exists='append', index=False)

    logging.info("Total inserted rows: {}".format(len(df)))


start = DummyOperator(
    task_id='start',
    dag=dag)

completed = DummyOperator(
    task_id='completed',
    dag=dag)

load_api_data_to_postgres_task = PythonOperator(
    task_id='load_api_data_to_postgres',
    python_callable=load_api_data_to_postgres,
    # provide_context=True,
    dag=dag
)

start >> load_api_data_to_postgres_task >> completed