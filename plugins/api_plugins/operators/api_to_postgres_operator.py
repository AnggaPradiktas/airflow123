import pandas as pd
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
import requests
import logging
import tempfile
import time


class ApiToPostgresOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                url,
                page,
                json_schema,
                rename_cols,
                postgres_conn_id,
                *args, 
                **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.page = page
        self.json_schema = json_schema  
        self.rename_cols = rename_cols
        self.postgres_conn_id = postgres_conn_id


    def execute(self, context):
        schema= BaseHook.get_connection(self.postgres_conn_id).schema
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                         schema=schema)
        logging.info("Connecting to Postgres...")
        hook.run(self.create_schema())
        for i in range(self.page+1):
            self.load_data(hook, self.url.format(i))
            time.sleep(2)
        logging.info("Load data DONE...")

    def get_api_data(self, url):
        payload={}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        logging.info('url is \n{}'.format(self.url))

        df = pd.json_normalize(response.json()['data'])
        df = df.rename(columns=self.rename_cols)
        logging.info('df is \n{}'.format(df.head()))

        return df

    def generate_file(self, url):
        logging.info("Generating CSV file...")
        tmp_file = tempfile.NamedTemporaryFile()
        csv_file = tmp_file.name + '.csv'
        df = self.get_api_data(url)
        df.to_csv(csv_file, index=False, sep='\t')
        logging.info("Generating CSV file DONE...")
        return csv_file
        
    def create_schema(self):
        with open(self.json_schema, 'r') as j:
            contents = json.loads(j.read())

        list_schema = [[c['column_name'], c['column_type'], c['is_null_able']] for c in contents]
        t = [' '.join(w) for w in list_schema]

        create_schema = """ CREATE TABLE IF NOT EXISTS api_to_postgres {}; """.format(tuple(t)).replace("'", "")
        logging.info("Creating schema...")
        return create_schema


    def load_data(self, hook, url):
        logging.info("Loading data...")
        csv_file = self.generate_file(url)
        hook.bulk_load(self.table_name, csv_file)
        logging.info("Loading data DONE...")