from random import randint
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _trainingModel():
    return randint

with DAG("httpChartDag",discription='turning http log data usage into chart',start_date=datetime(2022,1,1),schedule_interval='@daily',catchup=False) as dag:
    training_mode_A = PythonOperator(
        task_id="trainingModelA",
        python_callable=_trainingModel
    )