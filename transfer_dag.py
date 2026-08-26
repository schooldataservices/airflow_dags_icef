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
    description='Upload SchoolMint applications/registrations from local SFTP to GCS. SchoolMint drops files ~5:00 AM Central; this DAG runs at 5:15 AM so the fresh drop is included.',
    schedule_interval='15 5 * * 1-5',  # 5:15 AM Central, after SchoolMint ~5:00 AM drop

    catchup=False,
) as dag:

    # File that is not needed in a local bucket, and can go straight to GCP buckets
    upload_to_gcs_from_school_mint_enroll = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs_from_school_mint_enroll',
        bucket='dbt_historicalbucket-icefschools-1',  # Replace with your GCS bucket name
        dst='',  # Keep original filenames in the bucket root
        src='/home/local/schoolmint/upload/reports/Back_Jenny_4042676/*',  # Upload all files from SchoolMint folder
        gcp_conn_id='google_cloud_default',  # Airflow connection ID for GCP
    )

    iready_ftp_staging = LocalFilesystemToGCSOperator(
    task_id='upload_iready_sftp_files_to_staging_bucket',
    bucket='iready_stagingbucket-icefschools-1',
    dst='',  # Name of the second file in GCS
    src='/home/local/iready/*',  # Local path to the second file
    gcp_conn_id='google_cloud_default',
    )

    iready_diagnostic_results = LocalFilesystemToGCSOperator(
    task_id='upload_iready_sftp_files_to_ireadybucket',
    bucket='ireadybucket-icefschools-1',
    dst='',  # Name of the second file in GCS
    src='/home/local/iready/*',  # Local path to the second file
    gcp_conn_id='google_cloud_default',
    )

upload_to_gcs_from_school_mint_enroll
iready_ftp_staging  
iready_diagnostic_results  # Optional: set task order if needed
    

