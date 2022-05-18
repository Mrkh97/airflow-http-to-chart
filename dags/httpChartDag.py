from random import randint
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator


def _parseHttp():
    numberOfLines = 0
    numberOfGet = 0
    numberOfPost = 0
    f = open("/opt/airflow/data/http.log", "r")
    w = open("/opt/airflow/data/result.txt", "w")
    for x in f:
        for index, word in enumerate(x.split()):
            if word == '200':
                print(x)
                if(x.split()[index-3]) == '"GET':
                    w.write("\n"+"GET"+"\n")
                    print(x.split()[index-5])
                    w.write(x.split()[index-5]+"\n")
                    print(x.split()[index+1])
                    w.write(x.split()[index+1]+"\n")

                    numberOfGet += 1
                if(x.split()[index-3]) == '"POST':
                    w.write("POST"+"\n")
                    print(x.split()[index-5])
                    w.write(x.split()[index-5]+"\n")
                    print(x.split()[index+1])
                    w.write(x.split()[index+1]+"\n")

                    numberOfPost += 1
                numberOfLines += 1


    w.write('Total 200 requests={}\n'.format(numberOfLines))
    w.write('GET={}\n'.format(numberOfGet))
    w.write('POST={}\n'.format(numberOfPost))

    print('Total 200 requests={}'.format(numberOfLines))
    print('GET={}'.format(numberOfGet))
    print('POST={}'.format(numberOfPost))

    f.close()
    w.close()


with DAG("httpChartDag", start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    parseHttp = PythonOperator(
        task_id="parseHttp",
        python_callable=_parseHttp
    )
