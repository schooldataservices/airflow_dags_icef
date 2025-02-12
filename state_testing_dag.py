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
    schedule_interval='25 3 * * 1',  
    catchup=False,  # Do not backfill
) as dag:
    
       # Define a task to run the Docker container (DockerOperator)
    run_state_testing_processing = DockerOperator(
        task_id='run_state_testing_processing',  # Unique task ID
        image='sbac-processing',  # The Docker image to run
        command='python /app/main.py',  # Command to execute inside the container
        mounts=[
            {
                'source': '/home/g2015samtaylor/state_testing',
                'target': '/app/state_testing',
                'type': 'bind',
            },
            {
                'source': '/home/g2015samtaylor/views',
                'target': '/app/views',
                'type': 'bind',
            }
        ],
        dag=dag  # Associate the task with the DAG
    )




