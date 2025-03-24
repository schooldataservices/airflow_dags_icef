from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
from datetime import datetime


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

# Define the function to interact with the database and write data to a file
def send_data_pipeline_metadata():
    # Create the MySQL hook using the connection ID (the one you created earlier)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')  # Use the ID of your connection
    
    # Run a query and fetch the results as a pandas DataFrame
    df = mysql_hook.get_pandas_df("SELECT * FROM data_pipeline_runs")
    
    # Print the DataFrame to verify
    print(df)
    
    # Define the local file path where you want to save the CSV
    file_path = '/home/g2015samtaylor/views/data_pipeline_runs.csv'
    
    # Write the DataFrame to a CSV file in the specified directory
    df.to_csv(file_path, index=False)
    logging.info(f"Data written to {file_path}")

# Define the DAG
dag = DAG(
    'data_pipeline_metadata', 
    default_args={'owner': 'airflow'},
    description='Send over Data Pipeline Metdata',
    schedule_interval='30 7 * * 1-5',  
    start_date=datetime(2023, 1, 1),
)

# Define the task to execute the function
single_task = PythonOperator(
    task_id='data_pipeline_metdata',
    python_callable=send_data_pipeline_metadata,
    dag=dag,
)

# Set the task in the DAG
single_task
