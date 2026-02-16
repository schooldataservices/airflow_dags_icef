from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Get the Google credentials path from Airflow Variables
google_application_credentials_path = Variable.get("google_applications_credentials_path")

# Define default arguments
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 28),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com']
} # 'jback@icefps.org'

# Initialize the DAG
dag = DAG(
    'bigquery_upload_dag_singular',
    default_args=args,
    description='A DAG to upload files from SFTP to BigQuery',
    schedule_interval='30 5 * * *',  # Every day at 5:30 AM
    catchup=False,
    max_active_runs=1,
)

# Helper function to create DockerOperator tasks
def create_upload_task(task_id, local_dir=None):
    mounts = [
        {
            "Source": google_application_credentials_path,  # Host path for credentials
            "Target": google_application_credentials_path,  # Container path for credentials
            "Type": "bind",
        },
    ]

    # Add the local_dir mount only if local_dir is provided
    if local_dir:
        mounts.append({
            "Source": local_dir,  # Host path for local directory
            "Target": local_dir,  # Container path for local directory
            "Type": "bind",
        })

    return DockerOperator(
        task_id=task_id,
        image='gcr.io/icef-437920/upload-to-bigquery:latest',  #previous was upload-to-bigquery:dtype-fix
        api_version='auto',
        auto_remove=True,
        tty=True,
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": google_application_credentials_path,
            "dataset_name": "{{ dag_run.conf['datasets_to_run'][0] if dag_run and dag_run.conf.get('datasets_to_run') else 'default_dataset' }}",
            "LOCAL_DIR": local_dir if local_dir else "",  # Pass an empty string if local_dir is None
        },
        mounts=mounts,
        force_pull=True,
        dag=dag,
    )

# Define upload tasks
upload_dataset = create_upload_task(
    task_id='upload_to_bigquery_singular'
)

# Run tasks in parallel
upload_dataset
