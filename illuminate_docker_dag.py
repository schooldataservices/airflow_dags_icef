import os
import pendulum
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowException

# Set the timezone explicitly (e.g., America/Chicago for CST/CDT)
timezone = pendulum.timezone('America/Chicago')

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com'],
    'catchup': False,  # Do not backfill the DAG
}

# Define the DAG
with DAG(
    dag_id='illuminate_docker_dag',
    default_args=default_args,
    description='A Docker-based DAG to handle Illuminate API calls',
    schedule_interval='0 5 * * *',  # Every day at 5:00 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    # Task to run the Dockerized pipeline
    run_docker_pipeline = DockerOperator(
        task_id='run_docker_pipeline',
        image='gcr.io/icef-437920/illuminate-pipeline:latest',  # Pulling from GCR
        api_version='auto',
        tty=True,
        auto_remove=True,  # Automatically remove the container after completion
        network_mode="host",  # Use host networking if required
        command='python /app/illuminate_pipeline.py',
        mounts=[
            {
                "Source": "/home/g2015samtaylor/icef-437920.json",  # Host path for credentials
                "Target": "/app/icef-437920.json",  # Container path for credentials
                "Type": "bind",
            },
        ],
        environment={
            'YEARS_DATA': '24-25',
            'START_DATE': '2025-05-01', #this will change once data starts comig for 25-26
            'GOOGLE_APPLICATION_CREDENTIALS': '/app/icef-437920.json'  # Add the environment variable
        },
        force_pull=True  # Ensures the latest image is pulled
    )
