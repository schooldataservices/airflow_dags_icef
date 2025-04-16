from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Default arguments for the DAG
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
    'placeholder_real_names',
    default_args=default_args,
    description='Scrape Google Sheet, and get placeholder names to populate Big Query Table',
    schedule_interval='0 1 * * 1-5',  # Adjust the schedule as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_docker_pipeline = DockerOperator(
        task_id='run_docker_pipeline',
        image='gcr.io/icef-437920/placeholder_real_names:latest',  # Pulling from GCR
        api_version='auto',
        tty=True,
        auto_remove=True,  # Automatically remove the container after completion
        force_pull=True,
        environment={
            'GOOGLE_APPLICATION_CREDENTIALS': '/home/sam/icef-437920.json',  # Set the environment variable
        },
        mounts=[
            {
                'Source': '/home/g2015samtaylor',  # Host directory
                'Target': '/home/sam',  # Container directory
                'Type': 'bind',         # Bind mount type
            },
        ],
        command='python /app/main.py',  # Replace with the command you want to run inside the container
        working_dir='/app',  # Set the working directory inside the container
        docker_url='unix://var/run/docker.sock',  # Docker socket
        network_mode='bridge',  # Use bridge networking
    )
