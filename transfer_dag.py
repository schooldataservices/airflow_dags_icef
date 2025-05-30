from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 21),  # Update with your desired start date
    'retries': 1,
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com'],
    'catchup': False,  # Do not backfill the DAG
}

# Define the DAG
with DAG(
    'SFTP_to_gcs_transfer',
    default_args=default_args,
    description='Upload the applications and registrations file coming from SchoolMint to a GCS bucket. File is received in Local SFTP at 5 AM Central time. ',
    schedule_interval='10 5 * * 1-5',  
    catchup=False,
) as dag:

    # Task to upload the file to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_sftp_file_to_bucket',
        bucket='enrollmentbucket-icefschools-1',  # Replace with your GCS bucket name
        dst='completed_registrations_2025.csv',  # Name of the file in GCS
        src='/home/local/schoolmint/upload/reports/Back_Jenny_4042676/completed_registrations_2025.csv',  # Local file path
        gcp_conn_id='google_cloud_default',  # Airflow connection ID for GCP
    )

    upload_to_gcs