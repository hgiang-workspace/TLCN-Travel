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
    dag_id="absa_pipeline_dag",
    default_args=default_args,
    description="ABSA pipeline from NLP to Gold tables",
    schedule_interval=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["tourism", "nlp", "absa"],
) as dag:

    entity_processing = BashOperator(
        task_id="entity_processing",
        bash_command="echo 'Running entity processing...'",
    )
    
    build_ml_dataset = BashOperator(
        task_id="build_ml_dataset",
        bash_command="echo 'Building ML dataset...'",
    )
    
    train_model = BashOperator(
        task_id="train_model",
        bash_command="echo 'Training ABSA model...'",
    )
    
    generate_gold = BashOperator(
        task_id="generate_gold",
        bash_command="echo 'Generating Gold tables...'",
    )
    
    quality_check = BashOperator(
        task_id="quality_check",
        bash_command="echo 'Running quality check...'",
    )

    entity_processing >> build_ml_dataset >> train_model >> generate_gold >> quality_check