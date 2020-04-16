# Standard Library
import os
import sys
from datetime import datetime, timedelta

# Third Party
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

# Make python folder a module
sys.path.append(".")
from python.create_staging2_schema import create_schema, create_tables


# Connect to Amazon Aurora Postgres database using Airflow
CONN = BaseHook.get_connection("redb")
BUCKET = CONN.conn_id
HOST = CONN.host
LOGIN = CONN.login
PASSWORD = CONN.password
PORT = CONN.port

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 4, 15, 3, 00, 00),
    "concurrency": 1,
    "retries": 3
}


dag = DAG(
    "staging_2",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

create_schema = PythonOperator(
    task_id="create_schema",
    python_callable=create_schema,
    op_kwargs={
        "database": BUCKET,
        "host": HOST,
        "username": LOGIN,
        "password": PASSWORD,
        "port": PORT
    },
    dag=dag
)

create_tables = PythonOperator(
    task_id="create_tables",
    python_callable=create_tables,
    op_kwargs={
        "database": BUCKET,
        "host": HOST,
        "username": LOGIN,
        "password": PASSWORD,
        "port": PORT
    },
    dag=dag
)

create_schema >> create_tables