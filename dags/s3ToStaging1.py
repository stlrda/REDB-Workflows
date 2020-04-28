# Standard Library
import os
import sys
import json
import datetime as dt

# Third Party
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

# Custom
sys.path.append(".")
from scripts.mdb_to_postgres import main

# Credentials for S3 Bucket
BUCKET_CONN = BaseHook.get_connection('redb-test')
BUCKET_NAME = BUCKET_CONN.conn_id
AWS_ACCESS_KEY_ID = json.loads(BUCKET_CONN.extra)['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = json.loads(BUCKET_CONN.extra)['aws_secret_access_key']

# Credentials for Database
DATABASE_CONN = BaseHook.get_connection('redb')
DATABASE_NAME = DATABASE_CONN.conn_id
DATABASE_HOST = DATABASE_CONN.host
DATABASE_USER = DATABASE_CONN.login
DATABASE_PORT = DATABASE_CONN.port
DATABASE_PASSWORD = DATABASE_CONN.password


default_args = {
    'owner': 'redb',
    'start_date': dt.datetime.now(),
    'concurrency': 1,
    'retries': 0,
    'catchup': False
}

with DAG('s3ToStaging1',
         default_args=default_args,
         schedule_interval='@once',
         ) as dag:
    transfer_mdb = PythonOperator(task_id='TransferMDB',
                               python_callable=main,
                               op_kwargs={'bucket': BUCKET_NAME,
                                        'aws_access_key_id': AWS_ACCESS_KEY_ID,
                                        'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
                                        'pg_database': DATABASE_NAME,
                                        'pg_host': DATABASE_HOST,
                                        'pg_user': DATABASE_USER,
                                        'pg_port': DATABASE_PORT,
                                        'pg_password': DATABASE_PASSWORD})
transfer_mdb