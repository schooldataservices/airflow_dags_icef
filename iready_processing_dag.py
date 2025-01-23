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
    description='DAG for processing iReady data using a Docker container',
    schedule_interval='55 4 * * 1-5',  # CRON for Monday-Friday at 4:55 AM
    catchup=False,
    max_active_runs=1,
) as dag:

    iready_processing_task = DockerOperator(
        task_id='process_iready_data',
        image='iready-processing',  # The Docker image name
        api_version='auto',
        auto_remove=True,
        tty=True,
        mounts=[
            {
                "Source": "/home/local/iready",  # Host directory for input CSVs
                "Target": "/home/local/iready",  # Container path for input
                "Type": "bind",
            },
            {
                "Source": "/home/g2015samtaylor/views",  # Host directory for output CSV
                "Target": "/home/g2015samtaylor/views",  # Container path for output
                "Type": "bind",
            },
        ],
        command=["python", "iready_processing.py"],  # Command to run the script
        dag=dag,
    )

       # Task 2: Run pivot-iready container, reference local file benchmark.csv to ultimately send to views as iready_orp.csv
    pivot_iready_task = DockerOperator(
        task_id='pivot_iready_data',
        image='pivot-iready',  # The Docker image name for the second container
        api_version='auto',
        auto_remove=True,
        mounts=[
            {
                "Source": "/home/g2015samtaylor/views/iready_assessment_results.csv",  # Host file
                "Target": "/app/iready_assessment_results.csv",  # Container file
                "Type": "bind",
            },
            {
                "Source": "/home/g2015samtaylor/views",  # Host directory for output
                "Target": "/app/output_dir",  # Container path for output
                "Type": "bind",
            },
            {
                "Source": "/home/g2015samtaylor/dibels/benchmark.csv",  # Referencing local file
                "Target": "/app/benchmark.csv",  # Referencing container file
                "Type": "bind",
            },


        ],
        command=[
            "--input_iready", "/app/iready_assessment_results.csv",
            "--input_dibels", "/app/benchmark.csv",
            "--output", "/app/output_dir/iready_orp.csv",
        ],
    )

#parallel execution
