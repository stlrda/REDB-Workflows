import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'Airflow',
    'start_date': dt.datetime(2019, 12, 31)
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}



dag = DAG('retrieveDAG',
        default_args=default_args)

t1 = BashOperator(task_id = 'download_files', bash_command='python /home/okamurads/airflow/dags/retrieve_source_files.py\
    -url https://raw.githubusercontent.com/stlrda/redb_python/master/config/redb_source_databases_urls.csv -b dayne-ccp',dag=dag)