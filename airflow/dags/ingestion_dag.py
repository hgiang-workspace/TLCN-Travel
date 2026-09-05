from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "tourism",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingestion_dag",
    default_args=default_args,
    description="Tripadvisor data ingestion pipeline",
    schedule_interval=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["tourism", "ingestion"],
) as dag:
    
    collect = BashOperator(
        task_id="collect_data",
        bash_command="echo 'Collecting Tripadvisor data...'",
    )
    
    validate = BashOperator(
        task_id="validate_schema",
        bash_command="echo 'Validating JSON schema...'",
    )
    
    write_bronze = BashOperator(
        task_id="write_bronze",
        bash_command="echo 'Writing to MinIO Bronze...'",
    )

    collect >> validate >> write_bronze