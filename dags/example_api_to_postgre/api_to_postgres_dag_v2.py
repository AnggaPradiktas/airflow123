from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow import DAG
from alert import slack_alerts
from api_plugins.operators.api_to_postgres_operator import ApiToPostgresOperator


DAG_ID = 'api_to_postgres_ingestion_dag_v2'

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


start = DummyOperator(
    task_id='start',
    dag=dag)

completed = DummyOperator(
    task_id='completed',
    dag=dag)

load_api_data_to_postgres_v2_task = ApiToPostgresOperator(
    task_id='load_api_data_to_postgres_v2',
    url=configs['url'],
    page=configs['page'],
    json_schema=configs['json_schema'],
    rename_cols=configs['rename_cols'],
    postgres_conn_id=configs['postgres_conn_id'],
    dag=dag
)

start >> load_api_data_to_postgres_v2_task >> completed