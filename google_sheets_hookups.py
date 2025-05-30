from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Default arguments for the DAG
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
    'google_sheets_hookups',
    default_args=default_args,
    description='Scrape Google Sheet, and get placeholder names to populate Big Query Table. Also send over Intent to Return',
    schedule_interval='0 1 * * 1-5',  # Adjust the schedule as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_docker_pipeline = DockerOperator(
        task_id='run_sheets_transfers',
        image='gcr.io/icef-437920/google_sheets_hookups:latest',  # Pulling from GCR
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
