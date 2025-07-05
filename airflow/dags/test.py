from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='test_logs',
    start_date=datetime(2025, 7, 1),
    schedule='@once',
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Airflow!"'
    )