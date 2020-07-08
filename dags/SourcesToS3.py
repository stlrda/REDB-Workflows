# Standard Library
import os
import sys
import datetime as dt

# Third Party
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

# Custom
sys.path.append("/usr/local/airflow/dags/efs/redb")
from scripts.transfer_to_s3 import main

CONN = BaseHook.get_connection('redb-workbucket')
BUCKET = CONN.conn_id
AWS_ACCESS_KEY_ID = CONN.login
AWS_SECRET_ACCESS_KEY = CONN.password

default_args = {
    'owner': 'redb',
    'start_date': dt.datetime(2020, 4, 15, 3, 00, 00),
    'concurrency': 1,
    'retries': 0,
    'catchup': False
}

with DAG('SourcesToS3',
        default_args=default_args,
        schedule_interval='@once',
        ) as dag:
    transfer = PythonOperator(task_id='Transfer', python_callable=main, op_kwargs={'bucket': BUCKET,'aws_access_key_id': AWS_ACCESS_KEY_ID,'aws_secret_access_key': AWS_SECRET_ACCESS_KEY})
transfer
