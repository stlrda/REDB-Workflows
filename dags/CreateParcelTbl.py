# Standard Library
import os
import sys
import json
import datetime as dt

# Third Party
from airflow import DAG
from airflow.utils.helpers import chain
from airflow.hooks.base_hook import BaseHook
from airflow.operators.postgres_operator import PostgresOperator

# Custom
#sys.path.append("/usr/local/airflow")
# Unused?

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

# This DAG will execute each of the .sql scripts from the path specified in "template_searchpath"
with DAG('InsertParcelTbl',
        default_args=default_args,
        template_searchpath="/usr/local/airflow/dags/efs/redb/sql/inserts/",
        schedule_interval='@once',
        ) as dag:
    parcel = PostgresOperator(task_id="InsertParcelTbl", postgres_conn_id="redb_postgres", sql="parcel.sql", database=DATABASE_NAME)

# run the task
parcel
