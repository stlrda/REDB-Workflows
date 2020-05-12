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
sys.path.append("/usr/local/airflow")
from redb.scripts.insert_into_core import main

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
with DAG('InsertCoreData',
         default_args=default_args,
         template_searchpath="/usr/local/airflow/dags/redb/sql/inserts/",
         schedule_interval='@once',
         ) as dag:

    parcel = PostgresOperator(task_id="parcel", postgres_conn_id="redb_postgres", sql="parcel.sql", database=DATABASE_NAME)
    building = PostgresOperator(task_id="building", postgres_conn_id="redb_postgres", sql="building.sql", database=DATABASE_NAME)
    unit = PostgresOperator(task_id="unit", postgres_conn_id="redb_postgres", sql="unit.sql", database=DATABASE_NAME)
    legal_entity = PostgresOperator(task_id="legal_entity", postgres_conn_id="redb_postgres", sql="legal_entity.sql", database=DATABASE_NAME)
    address = PostgresOperator(task_id="address", postgres_conn_id="redb_postgres", sql="address.sql", database=DATABASE_NAME)
    county_id_mapping_table = PostgresOperator(task_id="county_id_mapping_table", postgres_conn_id="redb_postgres", sql="county_id_mapping_table.sql", database=DATABASE_NAME)
    county = PostgresOperator(task_id="county", postgres_conn_id="redb_postgres", sql="county.sql", database=DATABASE_NAME)
    neighborhood = PostgresOperator(task_id="neighborhood", postgres_conn_id="redb_postgres", sql="neighborhood.sql", database=DATABASE_NAME)
    sub_parcel_type = PostgresOperator(task_id="sub_parcel_type", postgres_conn_id="redb_postgres", sql="sub_parcel_type.sql", database=DATABASE_NAME)
    special_parcel_type = PostgresOperator(task_id="special_parcel_type", postgres_conn_id="redb_postgres", sql="special_parcel_type.sql", database=DATABASE_NAME)

# The "chain" method executes the tasks like normal, but provides a cleaner structure for legibility.
chain(
    county,
    neighborhood,
    address,
    legal_entity,
    building,
    unit,
    county_id_mapping_table,
    special_parcel_type,
    sub_parcel_type,
    parcel
)