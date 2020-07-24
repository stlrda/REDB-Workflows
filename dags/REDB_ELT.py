# Standard Library
import os
import sys
import json
import datetime as dt

# Third Party
from airflow import DAG
from airflow.utils.helpers import chain
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

# Custom
sys.path.append("/usr/local/airflow/dags/efs")
import redb.scripts.transfer_to_s3 as toS3
import redb.scripts.mdb_to_postgres as mdbToREDB

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
    'start_date': dt.datetime.now(),
    'concurrency': 1,
    'retries': 0,
    'catchup': False
}

# This DAG will execute each of the .sql scripts from the path specified in "template_searchpath"
with DAG('REDB_ELT',
        default_args=default_args,
        template_searchpath="/usr/local/airflow/dags/efs/redb/sql/",
        schedule_interval='@weekly',
        ) as dag:

    # Download and unzip, then upload files to S3.
    SourcesToS3 = PythonOperator(task_id='SourcesToS3',
                            python_callable=toS3.main,
                            op_kwargs={'bucket': BUCKET_NAME,
                                    'aws_access_key_id': AWS_ACCESS_KEY_ID,
                                    'aws_secret_access_key': AWS_SECRET_ACCESS_KEY})
                                    
    # Copy data from mdb files (in S3) to REDB (staging_1).
    MDBtoREDB = PythonOperator(task_id='MDBtoREDB',
                                    python_callable=mdbToREDB.main,
                                    op_kwargs={'bucket': BUCKET_NAME,
                                        'aws_access_key_id': AWS_ACCESS_KEY_ID,
                                        'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
                                        'pg_database': DATABASE_NAME,
                                        'pg_host': DATABASE_HOST,
                                        'pg_user': DATABASE_USER,
                                        'pg_port': DATABASE_PORT,
                                        'pg_password': DATABASE_PASSWORD})

    # Replace functions (weekly) (sql/functions)
    # TODO update function scripts such that they delete function if exists before creating.
    define_format_parcel_address = PostgresOperator(task_id="define_format_parcel_address", postgres_conn_id="redb_postgres", sql="functions/format_parcel_address.sql", database=DATABASE_NAME)
    define_format_parcelId = PostgresOperator(task_id="define_format_parcelId", postgres_conn_id="redb_postgres", sql="functions/format_parcelId.sql", database=DATABASE_NAME)
    define_insert_week1_to_staging1 = PostgresOperator(task_id="define_insert_week1_to_staging1", postgres_conn_id="redb_postgres", sql="functions/insert_week1_to_staging1.sql", database=DATABASE_NAME)
    define_insert_week2_to_staging1 = PostgresOperator(task_id="define_insert_week2_to_staging1", postgres_conn_id="redb_postgres", sql="functions/insert_week2_to_staging1.sql", database=DATABASE_NAME)

    # Compare and Insert (weekly) (sql/inserts)
    insert_address = PostgresOperator(task_id="insert_address", postgres_conn_id="redb_postgres", sql="inserts/address.sql", database=DATABASE_NAME)
    insert_building = PostgresOperator(task_id="insert_building", postgres_conn_id="redb_postgres", sql="inserts/building.sql", database=DATABASE_NAME)
    insert_county_id_mapping_table = PostgresOperator(task_id="insert_county_id_mapping_table", postgres_conn_id="redb_postgres", sql="inserts/county_id_mapping_table.sql", database=DATABASE_NAME)
    insert_legal_entity = PostgresOperator(task_id="insert_legal_entity", postgres_conn_id="redb_postgres", sql="inserts/legal_entity.sql", database=DATABASE_NAME)
    insert_neighborhood = PostgresOperator(task_id="insert_neighborhood", postgres_conn_id="redb_postgres", sql="inserts/neighborhood.sql", database=DATABASE_NAME)
    insert_parcel = PostgresOperator(task_id="insert_parcel", postgres_conn_id="redb_postgres", sql="inserts/parcel.sql", database=DATABASE_NAME)
    insert_unit = PostgresOperator(task_id="insert_unit", postgres_conn_id="redb_postgres", sql="inserts/unit.sql", database=DATABASE_NAME)

    # Update (weekly) (sql/updates)
    update_address = PostgresOperator(task_id="update_address", postgres_conn_id="redb_postgres", sql="updates/address.sql", database=DATABASE_NAME)
    update_building = PostgresOperator(task_id="update_building", postgres_conn_id="redb_postgres", sql="updates/building.sql", database=DATABASE_NAME)
    update_county_id_mapping_table = PostgresOperator(task_id="update_county_id_mapping_table", postgres_conn_id="redb_postgres", sql="updates/county_id_mapping_table.sql", database=DATABASE_NAME)
    update_legal_entity = PostgresOperator(task_id="update_legal_entity", postgres_conn_id="redb_postgres", sql="updates/legal_entity.sql", database=DATABASE_NAME)
    update_neighborhood = PostgresOperator(task_id="update_neighborhood", postgres_conn_id="redb_postgres", sql="updates/neighborhood.sql", database=DATABASE_NAME)
    update_parcel = PostgresOperator(task_id="update_parcel", postgres_conn_id="redb_postgres", sql="updates/parcel.sql", database=DATABASE_NAME)
    update_unit = PostgresOperator(task_id="update_unit", postgres_conn_id="redb_postgres", sql="updates/unit.sql", database=DATABASE_NAME)
    
    
    # staging_1 > staging_2 (weekly) (sql/functions)
    staging_1_to_staging_2 = PostgresOperator(task_id="staging_1_to_staging_2", postgres_conn_id="redb_postgres", sql="functions/staging_1_to_staging_2.sql", database=DATABASE_NAME)


# The "chain" method executes the tasks like normal, but provides a cleaner structure for legibility.
chain(
    SourcesToS3
    , MDBtoREDB
    , define_format_parcel_address
    , define_format_parcelId
    , define_insert_week1_to_staging1
    , define_insert_week2_to_staging1
    , insert_address
    , insert_building
    , insert_county_id_mapping_table
    , insert_legal_entity
    , insert_neighborhood
    , insert_parcel
    , insert_unit
    , update_address
    , update_building
    , update_county_id_mapping_table
    , update_legal_entity
    , update_neighborhood
    , update_parcel
    , update_unit
    , staging_1_to_staging_2
)
