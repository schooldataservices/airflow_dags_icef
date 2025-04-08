from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Get the Google credentials path from Airflow Variables
google_applications_credentials_path = Variable.get("google_applications_credentials_path")

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
}

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
def create_upload_task(task_id, sftp_folder, local_dir):
    return DockerOperator(
        task_id=task_id,
        image='gcr.io/icef-437920/upload-to-bigquery:latest',  #previous was upload-to-bigquery:dtype-fix
        api_version='auto',
        auto_remove=True,
        tty=True,
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": google_applications_credentials_path,
            "SFTP_FOLDER_NAME": sftp_folder,
            "LOCAL_DIR": local_dir,
        },
   
        mounts=[
            {
                "Source": google_applications_credentials_path,  # Host path for credentials
                "Target": google_applications_credentials_path,  # Container path for credentials
                "Type": "bind",
            },
            {
                "Source": local_dir,  # Host path for local directory
                "Target": local_dir,  # Container path for local directory
                "Type": "bind",
            },
        ],
        force_pull=True,
        dag=dag,
    )

# Define upload tasks
upload_illuminate = create_upload_task(
    task_id='upload_to_bigquery_illuminate',
    sftp_folder='illuminate',
    local_dir='/home/g2015samtaylor/illuminate',
)

# upload_powerschool = create_upload_task(
#     task_id='upload_to_bigquery_powerschool',
#     sftp_folder='powerschool',
#     local_dir='/home/icef/powerschool/',
# )


upload_iready = create_upload_task(
    task_id='upload_to_bigquery_iready',
    sftp_folder='iready',
    local_dir='/home/local/iready',
)

upload_python_views = create_upload_task(
    task_id='upload_to_bigquery_python_views',
    sftp_folder='views',
    local_dir='/home/g2015samtaylor/views',
)

upload_dibels = create_upload_task(
    task_id='upload_to_bigquery_dibels',
    sftp_folder='dibels',
    local_dir='/home/g2015samtaylor/dibels',
)

upload_star = create_upload_task(
    task_id='upload_to_bigquery_star',
    sftp_folder='star',
    local_dir='/home/g2015samtaylor/star',
)

upload_state_testing = create_upload_task(
    task_id='upload_to_bigquery_state_testing',
    sftp_folder='state_testing',
    local_dir='/home/g2015samtaylor/state_testing',
)

upload_ixl = create_upload_task(
    task_id='upload_to_bigquery_ixl',
    sftp_folder='ixl',
    local_dir='/home/g2015samtaylor/ixl',
)

# Run tasks in parallel
upload_illuminate 
upload_iready
upload_python_views
upload_dibels
upload_star
upload_state_testing
upload_ixl