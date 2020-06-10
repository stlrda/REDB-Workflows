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

sys.path.append("/usr/local/airflow")

# Credentials for Database
DATABASE_CONN = BaseHook.get_connection('redb_postgres')
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

with DAG('CreateCore',
        default_args=default_args,
        schedule_interval='@once',
        ) as dag:
    create_tables = PythonOperator(task_id='CreateCore',
                                python_callable=main,
                                op_kwargs={
                                        'pg_database': DATABASE_NAME,
                                        'pg_host': DATABASE_HOST,
                                        'pg_user': DATABASE_USER,
                                        'pg_port': DATABASE_PORT,
                                        'pg_password': DATABASE_PASSWORD})
create_tables