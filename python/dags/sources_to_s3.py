# Standard Library
import os
import sys
import datetime as dt

# Third Party
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Custom
sys.path.append(".")
from Scripts.transfer_to_s3 import tempfile_to_s3
from Scripts.unzip_s3_objects import unzip
from credentials import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 4, 14, 15, 25, 00),
    'concurrency': 1,
    'retries': 0
}


with DAG('SourcesToS3',
         default_args=default_args,
         schedule_interval='*/35 * * * *',
         ) as dag:
    transfer = PythonOperator(task_id='Transfer',
                               python_callable=tempfile_to_s3)
transfer
