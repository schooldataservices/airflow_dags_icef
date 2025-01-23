from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import logging
import pandas as pd
import os
import sys
from datetime import datetime, timedelta


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 21),  # Update with your desired start date
    'retries': 1,
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com'],  # Update with your email
    'catchup': False,
}

# Define the DAG
with DAG(
    'star_processing_dag',
    default_args=default_args,
    description='A DAG for processing STAR assessment files',
    schedule_interval='25 3 * * 1',  
    catchup=False,  # Do not backfill
) as dag:
    
    # Define a task to run the Docker container (DockerOperator)
    run_star_processing = DockerOperator(
        task_id='run_star_script_processing',  # Unique task ID
        image='star-processing',  # The image to run
        command='python /app/main.py',  # Command to execute in the container
        mounts=[
            # Bind mount for CSV input files
            {
                'source': '/home/g2015samtaylor/star',
                'target': '/home/g2015samtaylor/star',
                'type': 'bind',
            },
            # Bind mount for output directory
            {
                'source': '/home/g2015samtaylor/views',
                'target': '/home/g2015samtaylor/views',
                'type': 'bind',
            },
        ],
        dag=dag  # Associate the task with the DAG
    )




