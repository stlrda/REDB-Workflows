import os
import sys
from datetime import datetime, timedelta, date

# Third Party
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

# Make python folder a module
sys.path.append(".")
from python.staging_2_functions import poulate_dead_parcels_table, copy_data

# Connect to Amazon Aurora Postgres database using Airflow
CONN = BaseHook.get_connection("redb")
BUCKET = CONN.conn_id
HOST = CONN.host
LOGIN = CONN.login
PASSWORD = CONN.password
PORT = CONN.port

default_args = {
    "owner": "airflow",
    "start_date": date.today(),
    "concurrency": 1,
    "retries": 3
}

dag = DAG(
    "update_staging_2",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

populate_dead_parcels_table = PythonOperator(
    task_id="populate_dead_parcels_table",
    python_callable=poulate_dead_parcels_table,
    op_kwargs={
        "database": BUCKET,
        "host": HOST,
        "username": LOGIN,
        "password": PASSWORD,
        "port": PORT
    },
    dag=dag
)

copy_data = PythonOperator(
    task_id="copy_data",
    python_callable=copy_data,
    op_kwargs={
        "database": BUCKET,
        "host": HOST,
        "username": LOGIN,
        "password": PASSWORD,
        "port": PORT
    },
    dag=dag
)