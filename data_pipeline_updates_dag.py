from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
from airflow.models import XCom
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from sqlalchemy import create_engine

args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Ensures each run is independent
    'catchup': False,  # Avoids backfilling and only runs from the start date
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com']
}



# Fetch data from MySQL
def fetch_data_from_mysql():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    connection = mysql_hook.get_conn()  # Get the MySQL connection
    engine = create_engine(mysql_hook.get_uri())  # Create an SQLAlchemy engine

    # Use pandas to read the data through the SQLAlchemy engine
    df = pd.read_sql("SELECT SQL_NO_CACHE * FROM data_pipeline_runs", engine)

    logging.info(f"Fetched {len(df)} rows from MySQL.")
    logging.info(df.iloc[0])

    # Ensure that the connection is closed to avoid reuse in the next task
    connection.close()  # Close the connection explicitly

    return df


# Write DataFrame to BigQuery
def write_to_bigquery(project_id, dataset_id, table_id):
    # Fetch the DataFrame directly
    df = fetch_data_from_mysql()

    if df.empty:
        raise ValueError("No data fetched from MySQL!")

    logging.info(df.iloc[0])

    try:
        bigquery_hook = BigQueryHook(gcp_conn_id='google_bigquery_conn', use_legacy_sql=False)
        client = bigquery_hook.get_client()
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        job = client.load_table_from_dataframe(
            dataframe=df,
            destination=table_ref,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        )
        
        job.result()  # Wait for the job to complete
        logging.info(f"Data written to BigQuery table: {table_ref}")
    except Exception as e:
        logging.error(f"Failed to write data to BigQuery: {e}")
        raise



dag = DAG(
    'data_pipeline_metadata',
    default_args=args,
    description='Send over Data Pipeline Metadata',
    schedule_interval='20 5 * * 1-5',
    start_date=datetime(2024, 10, 28),  # Match the start_date here
    catchup=False,  # No backfilling
)

# Load Data into BigQuery
write_to_bq = PythonOperator(
    task_id='write_to_bq',
    python_callable=write_to_bigquery,
    op_kwargs={
        "project_id": "icef-437920",
        "dataset_id": "views",
        "table_id": "data_pipeline_runs",
    },
    provide_context=True,  # Required for XCom
    dag=dag,
)


# Define task dependencies
write_to_bq  
