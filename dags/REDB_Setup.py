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

# Credentials for Database
DATABASE_CONN = BaseHook.get_connection('redb_postgres')
DATABASE_NAME = DATABASE_CONN.schema
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

with DAG('REDB_Setup',
        default_args=default_args,
        template_searchpath="/usr/local/airflow/dags/efs/redb/sql/",
        schedule_interval='@once',
        ) as dag:

    # Create schemas (once) (sql/functions)
    create_schemas = PostgresOperator(task_id="create_schemas", postgres_conn_id="redb_postgres", sql="functions/create_core_tables.sql", database=DATABASE_NAME)

    # Create tables (once) (sql/functions)
    create_tables = PostgresOperator(task_id="create_tables", postgres_conn_id="redb_postgres", sql="functions/create_REDB_schemas.sql", database=DATABASE_NAME)

    # Define functions (once) (sql/functions)
    define_add_county = PostgresOperator(task_id="define_add_county", postgres_conn_id="redb_postgres", sql="functions/add_county.sql", database=DATABASE_NAME)
    define_format_parcel_address = PostgresOperator(task_id="define_format_parcel_address", postgres_conn_id="redb_postgres", sql="functions/format_parcel_address.sql", database=DATABASE_NAME)
    define_format_parcelId = PostgresOperator(task_id="define_format_parcelId", postgres_conn_id="redb_postgres", sql="functions/format_parcelId.sql", database=DATABASE_NAME)
    define_insert_week1_to_staging1 = PostgresOperator(task_id="define_insert_week1_to_staging1", postgres_conn_id="redb_postgres", sql="functions/insert_week1_to_staging1.sql", database=DATABASE_NAME)
    define_insert_week2_to_staging1 = PostgresOperator(task_id="define_insert_week2_to_staging1", postgres_conn_id="redb_postgres", sql="functions/insert_week2_to_staging1.sql", database=DATABASE_NAME)
    

    
chain(
    create_schemas
    , create_tables
    , define_add_county
    , define_format_parcel_address
    , define_format_parcelId
    , define_insert_week1_to_staging1
    , define_insert_week2_to_staging1
)