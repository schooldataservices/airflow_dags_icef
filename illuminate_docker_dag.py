import os
import pendulum
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowException

# Set the timezone explicitly (e.g., America/Chicago for CST/CDT)
timezone = pendulum.timezone('America/Chicago')

# Default arguments
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 28),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com']
}

# Define the DAG
with DAG(
    dag_id='illuminate_docker_dag',
    default_args=args,
    description='A Docker-based DAG to handle Illuminate API calls',
    schedule_interval='0 5 * * 1-5',  # Every weekday (Mon-Fri) at 5:00 AM
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
            {   #Necessary for bringing in for calcs
                "Source": "/home/icef/powerschool/Student_Rosters.txt",  # Host path
                "Target": "/home/icef/powerschool/Student_Rosters.txt",  # Container path
                "Type": "bind"
            },

            {
                "Source": "/home/g2015samtaylor/illuminate",  # Host directory for save_path. Needed to send to dir
                "Target": "/home/g2015samtaylor/illuminate",  # Container path for save_path
                "Type": "bind",
            },

            {
                "Source": "/home/g2015samtaylor/views",  # Host directory for save_path
                "Target": "/home/g2015samtaylor/views",  # Container path for save_path
                "Type": "bind",
            },
        ],
        environment={
            'PYSPARK_SUBMIT_ARGS': '--conf spark.pyspark.gateway.timeout=300',
        }
    )

    
