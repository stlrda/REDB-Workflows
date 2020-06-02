# Standard Library 
import datetime as dt

# Third party
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'redb',
    'start_date': dt.datetime.now(),
    'concurrency': 1,
    'retries': 0,
    'catchup': False
}

with DAG('InstallDependencies',
         default_args=default_args,
         schedule_interval='@once',
         ) as dag:

    install_wget = BashOperator(task_id='install_wget', bash_command="pip install wget")

    install_boto3 = BashOperator(task_id='install_boto3', bash_command="pip install boto3")

    update = BashOperator(task_id='update', bash_command="apt-get update")

    install_mdbtools = BashOperator(task_id='install_mdbtools', bash_command="apt-get install mdbtools -y")

install_wget >> install_boto3 >> update >> install_mdbtools