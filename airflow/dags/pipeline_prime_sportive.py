from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
import sys
import os
from datetime import datetime
import logging

scripts_path = os.path.abspath("/opt/airflow/scripts")
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# === Import de tes scripts ===
from prime_sportive.clean_employes_data import main as clean_employes_data
from prime_sportive.ingest_employes_data import main as ingest_employes_data
from prime_sportive.get_distance_google import main as get_distance
from prime_sportive.calculate_prime import main as calculate_prime
from prime_sportive.gx_run_checkpoint_employe import run_checkpoint_employe

# === DAG Definition ===
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
}

with DAG(
    dag_id='pipeline_prime_sportive',
    start_date= datetime(2025, 7, 1),
    schedule='@monthly',
    catchup=False,
    description="Pipeline mensuel : données RH > distances > prime sportive",
    tags=['prime', 'rh', 'monthly']
) as dag:
    t1_clean_data = PythonOperator(
        task_id='clean_employes_data',
        python_callable=clean_employes_data
    )

    t2_validate_data = PythonOperator(
        task_id='validate_with_great_expectations',
        python_callable=run_checkpoint_employe
    )

    t3_ingest_data = PythonOperator(
        task_id='ingest_employes_data',
        python_callable=ingest_employes_data
    )

    t4_get_distance = PythonOperator(
        task_id='calculate_commute_distance',
        python_callable=get_distance
    )

    t5_calculate_prime = PythonOperator(
        task_id='calculate_prime_sportive',
        python_callable=calculate_prime
    )

    # Dépendances
    t1_clean_data >> t2_validate_data >> t3_ingest_data >> t4_get_distance >> t5_calculate_prime
