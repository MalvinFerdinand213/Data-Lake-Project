from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_data_lake_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily"
):

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command="python /scripts/bronze_ingest.py"
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command="python /scripts/silver_transform.py"
    )

    gold = BashOperator(
        task_id="gold_aggregate",
        bash_command="python /scripts/gold_aggregate.py"
    )

    bronze >> silver >> gold
