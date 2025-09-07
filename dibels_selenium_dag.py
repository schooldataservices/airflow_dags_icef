from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import logging
import os
import sys
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule  # Import TriggerRule

working_dir = '/home/g2015samtaylor/git_directory/Dibels'
sys.path.append(working_dir)
from modules.login import *
from modules.utility import *
from modules.file_altering import *


# Set up Chrome options and ChromeDriver path
def setup_chrome_driver(download_directory):
    chrome_options = webdriver.ChromeOptions()
    
    # Define download folder dynamically
    os.makedirs(download_directory, exist_ok=True)
    clear_directory(download_directory)
    
    # Set preferences for automatic downloads
    prefs = {'download.default_directory': download_directory,
             'profile.default_content_setting_values.automatic_downloads': 1,
             'profile.content_settings.exceptions.automatic_downloads.*.setting': 1}
    
    chrome_options.add_experimental_option('prefs', prefs)
    
    # Specify ChromeDriver path and initialize service
    chrome_driver_path = '/home/g2015samtaylor/Desktop/chromedriver-linux64/chromedriver'
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    logging.info('Chrome WebDriver started successfully.')
    return driver

# Main function that will be called in the Airflow task
def run_dibels_script(download_directory, destination_dir):
    driver = setup_chrome_driver(download_directory)
    
    # Log the start of the script
    logging.info('\n\n-------------New Logging Instance')

    # Run your main script
    main(driver, default_wait=30, download_folder=download_directory, destination_dir=destination_dir)

    # Cleanup after completion
    driver.quit()
    logging.info("Dibels script completed successfully.")

# Define the Airflow DAG
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

with DAG(
    'dibels_processing',
    default_args=default_args,
    description='A DAG to run Dibels Selenium script, and file processing',
    schedule_interval='30 3 * * 1-5',  # Every Monday at 3:30 AM
    catchup=False,
) as dag:
    
    # Define parameters for download directory and destination directory
    download_directory = '/home/g2015samtaylor/git_directory/Dibels/downloads'
    destination_dir = '/home/g2015samtaylor/dibels'
    
    # Define a task to run the Dibels script (PythonOperator)
    # run_selenium_downloads = PythonOperator(
    #     task_id='run_dibels_script_downloads',  # Unique task ID
    #     python_callable=run_dibels_script,
    #     op_kwargs={
    #         'download_directory': download_directory,
    #         'destination_dir': destination_dir
    #     },
    #     dag=dag,
    # )

    # Define a task to run the Docker container (DockerOperator)
    create_dibels_assessment_results = DockerOperator(
        task_id='create_dibels_assessment_results',  # Unique task ID
        image='gcr.io/icef-437920/dibels-processing',  # The image to run
        mounts=[
            # Bind mount for CSV input files
            {
                'source': '/home/g2015samtaylor/dibels',
                'target': '/home/g2015samtaylor/dibels',
                'type': 'bind',
            },
            # Bind mount for Google Cloud credentials file
            {
                'source': '/home/g2015samtaylor/icef-437920.json',  # Path on the host
                'target': '/home/g2015samtaylor/icef-437920.json',  # Path inside the container
                'type': 'bind',
            },
        ],
        force_pull=True,
        dag=dag,  # Associate the task with the DAG
    )

      # Define a task to run the Docker container (DockerOperator)
    create_dibels_pm_view = DockerOperator(
        task_id='create_dibels_pm_view',  # Unique task ID
        image='gcr.io/icef-437920/dibels-pm-processing',  # The image to run
        mounts=[
            # Bind mount for CSV input files, & output
            {
                'source': '/home/g2015samtaylor/dibels',  # Path on the host
                'target': '/app/dibels',  # Path inside the container
                'type': 'bind',  # Type of volume (bind mount)
            },
            {
                'source': '/home/g2015samtaylor/git_directory/Dibels/dibels_view',
                'target': '/app/dibels_view',
                'type': 'bind',
            },
            # Bind mount for Google Cloud credentials file
            {
                'source': '/home/g2015samtaylor/icef-437920.json',  # Path on the host
                'target': '/home/sam/icef-437920.json',  # Path inside the container
                'type': 'bind',
            },
        ],
        environment={
            'YEARS_DATA': '25-26',  # Corrected syntax for environment variable
        },
        force_pull=True,
        trigger_rule=TriggerRule.ALL_DONE,  # Ensure that this task runs even if the previous task fails
        dag=dag,  # Associate the task with the DAG
    )

    # Set up the task dependencies (run Selenium first, then run Docker)
    # run_selenium_downloads >> create_dibels_assessment_results >> create_dibels_pm_view
    create_dibels_assessment_results >> create_dibels_pm_view
