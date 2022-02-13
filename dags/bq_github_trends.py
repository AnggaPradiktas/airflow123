import json
from datetime import timedelta, datetime
from alert import slack_alerts
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

DAG_ID = 'bigquery_github_trends_dag'

# Config variables
configs = Variable.get(DAG_ID, deserialize_json=True)


default_args = {
    'owner': 'Angga',
    'depends_on_past': True,    
    'start_date': configs['start_date'],
    'end_date': configs['end_date'],
    'email': ['anggapradiktas@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': slack_alerts.task_send_failure_slack_alert
}

# Set Schedule: Run pipeline once a day. 
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
# schedule_interval = "00 21 * * *"

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    DAG_ID, 
    default_args=default_args,
    description='Run Query for Github Sample data in BQ',
    tags=['bigquery'],
    schedule_interval=configs['interval']
    )

## Task 1: check that the github archive data has a dated table created for that date
# To test this task, run this command:
# docker-compose -f docker-compose-gcloud.yml run --rm webserver airflow test bigquery_github_trends bq_check_githubarchive_day 2018-12-01
t1 = BigQueryCheckOperator(
        task_id='bq_check_githubarchive_day',
        sql='''
        #standardSQL
        SELECT
          table_id
        FROM
          `githubarchive.day.__TABLES_SUMMARY__`
        WHERE
          table_id = "{{ yesterday_ds_nodash }}"
        ''',
        use_legacy_sql=False,
        bigquery_conn_id=configs['gcp_conn_id'],
        dag=dag
    )

## Task 2: check that the hacker news table contains data for that date.
t2 = BigQueryCheckOperator(
        task_id='bq_check_hackernews_full',
        sql='''
        #standardSQL
        SELECT
          FORMAT_TIMESTAMP("%Y%m%d", timestamp ) AS date
        FROM
          `bigquery-public-data.hacker_news.full`
        WHERE
          type = 'story'
          AND FORMAT_TIMESTAMP("%Y%m%d", timestamp ) = "{{ yesterday_ds_nodash }}"
        LIMIT
          1
        ''',
        use_legacy_sql=False,
        bigquery_conn_id=configs['gcp_conn_id'],
        dag=dag
    )

# Setting up Dependencies
t1 >> t2
