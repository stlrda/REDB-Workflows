# Standard Library
import os
import sys
from datetime import datetime

# Third Party
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

# Make python folder a module
sys.path.append("/usr/local/airflow/dags/efs/redb")
from scripts.staging_2_functions import create_schema, create_tables, create_dead_parcels_table, create_dead_parcels_function


# Connect to Amazon Aurora Postgres database using Airflow
CONN = BaseHook.get_connection("redb_postgres")
BUCKET = CONN.conn_id
HOST = CONN.host
LOGIN = CONN.login
PASSWORD = CONN.password
PORT = CONN.port

default_args = {
    "owner": "redb",
    "start_date": datetime(2020, 4, 15, 3, 00, 00),
    "concurrency": 1,
    "retries": 3
}

op_kwargs = {
    "database": BUCKET,
    "host": HOST,
    "username": LOGIN,
    "password": PASSWORD,
    "port": PORT
}

dag = DAG(
    "CreateStaging2",
    default_args=default_args,
    schedule_interval='@once'
)

create_schema = PythonOperator(
    task_id="create_schema",
    python_callable=create_schema,
    op_kwargs=op_kwargs,
    dag=dag
)

create_tables = PythonOperator(
    task_id="create_tables",
    python_callable=create_tables,
    op_kwargs=op_kwargs,
    dag=dag
)

create_dead_parcels_table = PythonOperator(
    task_id="create_dead_parcels_table",
    python_callable=create_dead_parcels_table,
    op_kwargs=op_kwargs,
    dag=dag
)

create_dead_parcels_function = PythonOperator(
    task_id="create_dead_parcels_function",
    python_callable=create_dead_parcels_function,
    op_kwargs=op_kwargs,
    dag=dag
)

create_schema >> create_tables >> create_dead_parcels_table >> create_dead_parcels_function
