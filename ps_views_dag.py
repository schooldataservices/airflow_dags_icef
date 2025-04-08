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
    'ps_views_dag',
    default_args=default_args,
    description='A DAG for processing ps views',
    schedule_interval='20 3 * * 1-5',  
    catchup=False,  # Do not backfill
) as dag:
    
       # Define a task to run the Docker container (DockerOperator)
    create_student_to_teacher = DockerOperator(
        task_id='student-to-teacher',  # Unique task ID
        image='gcr.io/icef-437920/student-to-teacher:latest',  # The Docker image to run
        force_pull=True,  # Force Docker to pull the latest image. so it does not run a cached image
        command='python /app/main.py',  # Command to execute inside the container
        mounts=[
            {
                'source': '/home/g2015samtaylor/icef-437920.json',
                'target': '/app/credentials.json',
                'type': 'bind',
            },
        ],
        environment={
            'GOOGLE_APPLICATION_CREDENTIALS': '/app/credentials.json'
        },
        dag=dag  # Associate the task with the DAG

    )




