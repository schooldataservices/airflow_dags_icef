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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com']
} # 'jback@icefps.org'

# Initialize the DAG
dag = DAG(
    'bigquery_upload_dag',
    default_args=args,
    description='A DAG to upload files from SFTP to BigQuery',
    schedule_interval='30 5 * * *',  # Every day at 5:30 AM
    catchup=False,
    max_active_runs=1,
)

# Helper function to create DockerOperator tasks
def create_upload_task(task_id, dataset_name, local_dir=None):
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
            "dataset_name": dataset_name,
            "LOCAL_DIR": local_dir if local_dir else "",  # Pass an empty string if local_dir is None
        },
        mounts=mounts,
        force_pull=True,
        dag=dag,
    )

# Define upload tasks
upload_illuminate = create_upload_task(
    task_id='upload_to_bigquery_illuminate',
    dataset_name='illuminate',

)

upload_powerschool = create_upload_task(
    task_id='upload_to_bigquery_powerschool',
    dataset_name='powerschool',
)

upload_iready = create_upload_task(
    task_id='upload_to_bigquery_iready',
    dataset_name='iready',
    local_dir='/home/local/iready',
)

#Eventually local folder could be removed
upload_dibels = create_upload_task(
    task_id='upload_to_bigquery_dibels',
    dataset_name='dibels',
    local_dir='/home/g2015samtaylor/dibels',
)

upload_star = create_upload_task(
    task_id='upload_to_bigquery_star',
    dataset_name='star',
)

#Eventually local folder coud be removed
upload_state_testing = create_upload_task(
    task_id='upload_to_bigquery_state_testing',
    dataset_name='state_testing',
    local_dir='/home/g2015samtaylor/state_testing',
)

upload_ixl = create_upload_task(
    task_id='upload_to_bigquery_ixl',
    dataset_name='ixl',
)

upload_enrollment = create_upload_task(
    task_id='upload_to_bigquery_enrollment',
    dataset_name='enrollment',
)

upload_views = create_upload_task(
    task_id='upload_to_bigquery_views',
    dataset_name='views',
)

# Run tasks in parallel
upload_illuminate 
upload_iready
upload_dibels
upload_star
upload_state_testing
upload_ixl
upload_powerschool
upload_enrollment
upload_views