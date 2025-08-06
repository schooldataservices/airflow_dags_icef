from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['2015samtaylor@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sftp_operations_docker_dag',
    default_args=default_args,
    description='Pull iready files from their SFTP',
    start_date=datetime(2024, 12, 15),
    schedule_interval='45 4 * * 1-5',  # CRON for Monday-Friday at 4:45 AM
) as dag:
    
    sftp_task = DockerOperator(
        task_id='run_sftp_pipeline',
        image='gcr.io/icef-437920/sftp-migrations:latest',  # The Docker image name
        api_version='auto',
        auto_remove=True,  # Clean up the container after it finishes
        command='python /app/sftp_operations.py',  # Adjust if needed
        docker_url='unix://var/run/docker.sock',  # Default Docker socket
        network_mode='host',
        force_pull=True,

        mounts=[
            {
                "Source": "/home/g2015samtaylor/icef-437920.json",
                "Target": "/app/icef-437920.json",
                "Type": "bind"
            },
        ]
        
    )








