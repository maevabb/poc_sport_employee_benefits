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
from bien_etre.calculate_jours_bien_etre import main as calculate_jours_bien_etre
from bien_etre.gx_run_checkpoint_activities import run_checkpoint_activities

# === DAG Definition ===
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
}

with DAG(
    dag_id='pipeline_jours_bien_etre',
    start_date= datetime(2025, 7, 1),
    schedule='@weekly',
    catchup=False,
    description="Pipeline hebdomadaire : validation activités > calcul jours bien-être",
    tags=['congés', 'rh', 'hebdo']
) as dag:
    t1_validate_data = PythonOperator(
        task_id='validate_activities_data_with_gx',
        python_callable=run_checkpoint_activities
    )

    t2_calculate_conges = PythonOperator(
        task_id='calculate_jours_bien_etre',
        python_callable=calculate_jours_bien_etre
    )

    # Dépendances
    t1_validate_data >> t2_calculate_conges
