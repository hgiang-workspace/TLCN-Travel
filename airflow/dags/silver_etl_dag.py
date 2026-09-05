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
    dag_id="silver_etl_dag",
    default_args=default_args,
    description="Bronze to Silver ETL pipeline",
    schedule_interval=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["tourism", "etl", "spark"],
) as dag:

    trigger_spark = BashOperator(
        task_id="trigger_bronze_to_silver",
        bash_command="echo 'Triggering Bronze to Silver ETL...'",
    )
    
    check_status = BashOperator(
        task_id="check_iceberg_tables",
        bash_command="echo 'Checking Iceberg tables...'",
    )

    trigger_spark >> check_status