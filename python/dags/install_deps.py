# Standard Library 
import datetime as dt

# Third party
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(dag_id='InstallDependencies', 
        schedule_interval=None,
        start_date=dt.datetime(2020, 4, 10, 15, 00, 00),
        catchup=False) as dag:

    install_wget = BashOperator(task_id='install_wget', bash_command="pip install wget")

    install_boto3 = BashOperator(task_id='install_boto3', bash_command="pip install boto3")

[install_wget, install_boto3]