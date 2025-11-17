from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Path absolut ke folder script Anda (versi Windows)
# INI ADALAH "PENGHUBUNG"-NYA
SCRIPT_PATH = "/opt/airflow/scripts"

with DAG(
    dag_id='data_lake_pipeline',         # Nama pipeline Anda
    start_date=datetime(2025, 11, 16),
    schedule='@daily',
    catchup=False
) as dag:

    # Task 1: Menjalankan script Bronze
    task_run_bronze = BashOperator(
        task_id='run_bronze_ingest',
        bash_command=f"python {SCRIPT_PATH}/bronze_ingest.py"
    )

    # Task 2: Menjalankan script Silver
    task_run_silver = BashOperator(
        task_id='run_silver_transform',
        bash_command=f"python {SCRIPT_PATH}/silver_transform.py"
        # Ganti nama file jika berbeda
    )

    # Task 3: Menjalankan script Gold
    task_run_gold = BashOperator(
        task_id='run_gold_aggregation',
        bash_command=f"python {SCRIPT_PATH}/gold_transform.py"
        # Ganti nama file jika berbeda
    )

    # Menentukan Urutan Pipeline:
    # Bronze -> Silver -> Gold
    task_run_bronze >> task_run_silver >> task_run_gold