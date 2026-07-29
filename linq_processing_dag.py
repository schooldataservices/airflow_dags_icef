from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 21),
    'retries': 1,
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com'],
    'catchup': False,
}

with DAG(
    'linq_processing_dag',
    default_args=default_args,
    description='A DAG for processing LINQ data files',
    schedule_interval='25 4 * * 1-5',
    catchup=False,
    max_active_runs=1,
) as dag:

    run_linq_processing = DockerOperator(
        task_id='run_linq_processing',
        image='gcr.io/icef-437920/linq-processing:latest',
        auto_remove=True,
        tty=True,
        force_pull=True,
        mounts=[
            {
                'source': '/home/g2015samtaylor/icef-437920.json',
                'target': '/app/icef-437920.json',
                'type': 'bind',
            },
        ],
        environment={
            'GOOGLE_APPLICATION_CREDENTIALS': '/app/icef-437920.json',
        },
        dag=dag,
    )
