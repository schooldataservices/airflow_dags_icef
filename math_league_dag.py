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
    'math_league_dag',
    default_args=default_args,
    description='A DAG for processing Math League data',
    schedule_interval='45 5 * * 1-5',  # 5:45 AM Central
    catchup=False,
    max_active_runs=1,
) as dag:

    run_math_league = DockerOperator(
        task_id='run_math_league',
        image='gcr.io/icef-437920/math-league@sha256:5ecb40caa094eaa51722aac0a394180907fc65438b21185d312078ca5a7f0e6e',
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
