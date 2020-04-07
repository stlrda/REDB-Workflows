import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'Airflow',
    'start_date': dt.datetime(2017, 6, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}


dag = DAG('migrateDAG',
        default_args=default_args)

t1 = BashOperator(task_id = 'upload_files_to_PG', bash_command='python <SCRIPT LOCATION> -b <YOUR BUCKET NAME> -p <YOUR AWS PROFILE>',dag=dag)
