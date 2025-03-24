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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
        image='illuminate-pipeline:pyspark',  # Pulling local image. #This would pull from the cloud 'gcr.io/icef-437920/illuminate-pipeline:latest'
        api_version='auto',
        auto_remove=True,  # Automatically remove the container after completion
        network_mode="host",  # Use host networking if required
        command='spark-submit /app/illuminate_pipeline.py',
        mounts=[
            {
                "Source": "/home/g2015samtaylor/illuminate",  # Host directory for save_path
                "Target": "/app/illuminate",  # Container path for save_path
                "Type": "bind",
            },
            {
                "Source": "/home/g2015samtaylor/views",  # Host directory for view_path
                "Target": "/app/views",  # Container path for view_path
                "Type": "bind",
            },
            {
                "Source": "/home/g2015samtaylor/airflow/git_directory/Illuminate/modules/illuminate_checkpoint_manual_changes.csv",  # Host file for manual_changes_file_path
                "Target": "/app/illuminate_checkpoint_manual_changes.csv",  # Container path for manual_changes_file_path
                "Type": "bind",
            },
            {
                "Source": "/home/icef/powerschool/Student_Rosters.txt",  # Host file for Student_Rosters.txt
                "Target": "/home/icef/powerschool/Student_Rosters.txt",  # Container path for Student_Rosters.txt
                "Type": "bind",
            },
        ],
        environment={
            'PYSPARK_SUBMIT_ARGS': '--conf spark.pyspark.gateway.timeout=300',
            'SAVE_PATH': '/app/illuminate',
            'VIEW_PATH': '/app/views',
            'MANUAL_CHANGES_FILE_PATH': '/app/illuminate_checkpoint_manual_changes.csv',
            'YEARS_DATA': '24-25',
            'START_DATE': '2024-07-01'
        }
    )
