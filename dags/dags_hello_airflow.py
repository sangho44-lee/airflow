from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

# 실행할 Python 함수
def hello_airflow():
    print("Hello Airflow!")

# DAG 정의
with DAG(
    dag_id='dags_hello_airflow',
    schedule_interval='@daily',  # 매일 1회 실행
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['example'],
) as dag:

    task_hello = PythonOperator(
        task_id='print_hello',
        python_callable=hello_airflow
    )