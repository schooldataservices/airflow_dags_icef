from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
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
    'state_testing_dag',
    default_args=default_args,
    description='A DAG for processing state testing files',
    schedule_interval='25 4 * * 1',  
    catchup=False,  # Do not backfill
) as dag:
    
    # Task for state testing processing
    run_state_testing_processing = DockerOperator(
        task_id='run_state_testing_processing',  # Unique task ID
        image='gcr.io/icef-437920/sbac-processing:latest',  # The Docker image to run
        environment={
            'GOOGLE_APPLICATION_CREDENTIALS': '/app/credentials.json',  # Path inside the container
        },
        mounts=[
            {
                'Source': '/home/g2015samtaylor/icef-437920.json',  # Path on the host machine
                'Target': '/app/credentials.json',  # Path inside the container
                'Type': 'bind',
            },
        ],
        force_pull=True,  # Ensure the latest image is pulled
        auto_remove=True,  # Automatically remove the container after completion
        api_version='auto',
        tty=True,
        network_mode="host",  # Use host networking if required
        dag=dag  # Associate the task with the DAG
    )

    # Task for SBAC interim processing
    run_sbac_interim_processing = DockerOperator(
        task_id='run_sbac_interim_processing',  # Unique task ID
        image='gcr.io/icef-437920/sbac-processing-interim:latest',  # The Docker image to run
        environment={
            'GOOGLE_APPLICATION_CREDENTIALS': '/app/credentials.json',  # Path inside the container
        },
        mounts=[
            {
                'Source': '/home/g2015samtaylor/icef-437920.json',  # Path on the host machine
                'Target': '/app/credentials.json',  # Path inside the container
                'Type': 'bind',
            },
        ],
        force_pull=True,  # Ensure the latest image is pulled
        auto_remove=True,  # Automatically remove the container after completion
        api_version='auto',
        tty=True,
        network_mode="host",  # Use host networking if required
        dag=dag  # Associate the task with the DAG
    )

run_state_testing_processing
run_sbac_interim_processing