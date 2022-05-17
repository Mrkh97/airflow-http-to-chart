from random import randint
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator


def _trainingModel():
    return randint(1, 10)


def _chooseBestModel(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'trainingModelA',
        'trainingModelB',
        'trainingModelC'
    ])
    bestAccuracy = max(accuracies)
    if(bestAccuracy > 8):
        return 'accurate'
    return 'inaccurate'


with DAG("my_first_dag", start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    training_mode_A = PythonOperator(
        task_id="trainingModelA",
        python_callable=_trainingModel
    )

    training_mode_B = PythonOperator(
        task_id="trainingModelB",
        python_callable=_trainingModel
    )

    training_mode_C = PythonOperator(
        task_id="trainingModelC",
        python_callable=_trainingModel
    )

    chooseBestModel = BranchPythonOperator(
        task_id="chooseBestModel",
        python_callable=_chooseBestModel
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

[training_mode_A,training_mode_B,training_mode_C] >> chooseBestModel >> [accurate,inaccurate]