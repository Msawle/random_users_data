from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'email': ['ms.sawle@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('extract_users_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract_userss.py'
    )

    start_datafusion_pipeline = CloudDataFusionStartPipelineOperator(
    location="us-west1",
    pipeline_name="random_user_wrangler",
    instance_name="data-fusion-demo-1989",
    task_id="start_datafusion_pipeline",
    pipeline_timeout=600,
    success_states=['COMPLETED', 'SUCCESS']
    )


    run_script_task >> start_datafusion_pipeline