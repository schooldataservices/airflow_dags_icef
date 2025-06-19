from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com']
}

# Initialize the DAG
with DAG(
    dag_id='iready_processing_dag',
    default_args=default_args,
    description='DAG for processing iReady raw data, & create diagnostic_results_tabular table through iReady, Dibels, & Star data',
    schedule_interval='55 4 * * 1-5',  # CRON for Monday-Friday at 4:55 AM
    catchup=False,
    max_active_runs=1,
) as dag:

    iready_processing = DockerOperator(
        task_id='process_iready_data',
        image='gcr.io/icef-437920/iready-processing:latest',  # The Docker image name
        api_version='auto',
        auto_remove=True,
        tty=True,
        force_pull=True,
        mounts=[
            {
                'source': '/home/g2015samtaylor/icef-437920.json',  # Path on the VM
                'target': '/home/sam/icef-437920.json',  # Path inside the container
                'type': 'bind',
            },
        ],
        environment={
            'GOOGLE_APPLICATION_CREDENTIALS': '/home/sam/icef-437920.json'  # Set the environment variable
        },
        dag=dag,
    )

    create_diagnostics_results = DockerOperator(
        task_id='create_diagnostic_results',
        image='gcr.io/icef-437920/create_diagnostic_results:latest',  # The Docker image name for the second container
        api_version='auto',
        auto_remove=True,
        tty=True,
        force_pull=True,
        mounts=[
            {
                'source': '/home/g2015samtaylor/icef-437920.json',  # Path on the VM
                'target': '/home/sam/icef-437920.json',  # Path inside the container
                'type': 'bind',
            },
        ],
        environment={
            'GOOGLE_APPLICATION_CREDENTIALS': '/home/sam/icef-437920.json'  # Set the environment variable
        },
        dag=dag,
    )