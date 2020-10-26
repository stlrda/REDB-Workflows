# Standard Library
import sys
import datetime as dt

# Third Party
from airflow import DAG
from airflow.utils.helpers import chain
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

# Custom
sys.path.append("/usr/local/airflow/dags/efs")
import redb.scripts.parcels_to_postgres as parcelsToREDB

# Credentials for S3 Bucket
BUCKET_CONN = BaseHook.get_connection('redb-workbucket')
BUCKET_NAME = BUCKET_CONN.conn_id
AWS_ACCESS_KEY_ID = BUCKET_CONN.login
AWS_SECRET_ACCESS_KEY = BUCKET_CONN.password

# Credentials for Database
DATABASE_CONN = BaseHook.get_connection('redb_postgres')
DATABASE_NAME = DATABASE_CONN.schema
DATABASE_HOST = DATABASE_CONN.host
DATABASE_USER = DATABASE_CONN.login
DATABASE_PORT = DATABASE_CONN.port
DATABASE_PASSWORD = DATABASE_CONN.password

default_args = {
    'owner': 'redb',
    'start_date': dt.datetime(2020,7,23),
    'concurrency': 1,
    'retries': 0,
    'catchup': False
}

with DAG('REDB_ELT',
         default_args=default_args,
         template_searchpath="/usr/local/airflow/dags/efs/redb/sql/",
         schedule_interval='@monthly',
         catchup=False
         ) as dag:

    # Scrapes the Parcel API and stores values in the RedB database
    ParcelsToREDB = PythonOperator(task_id='MDBtoREDB', python_callable=parcelsToREDB.main)

# The "chain" method executes the tasks like normal, but provides a cleaner structure for legibility.
chain(
    ParcelsToREDB
)
