import os
import sys
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

working_dir = '/home/g2015samtaylor/git_directory/IXL'
sys.path.append(working_dir)
# Import your custom modules
from modules.login import *
from modules.file_altering import *
from modules.normalization import *
from modules.checks import *
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

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
    'ixl_selenium_dag',
    default_args=default_args,
    description='A simple DAG to run Selenium and Docker tasks',
    schedule_interval='50 3 * * 1-5',  
    catchup=False,
) as dag:
    
    # Define parameters for download directory 
    download_directory = '/home/g2015samtaylor/git_directory/IXL/downloads'
    
    def setup_chrome_driver(download_directory):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.binary_location = "/home/g2015samtaylor/chrome_selenium_binaries/chrome-linux64/chrome"
        
        os.makedirs(download_directory, exist_ok=True)
        prefs = {'download.default_directory': download_directory,
                 'profile.default_content_setting_values.automatic_downloads': 1,
                 'profile.content_settings.exceptions.automatic_downloads.*.setting': 1}
        chrome_options.add_experimental_option('prefs', prefs)
        
        chrome_driver_path = "/home/g2015samtaylor/chrome_selenium_binaries/chromedriver-linux64/chromedriver"
        service = Service(chrome_driver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        return driver
    
    # Define a task to run the Selenium script (PythonOperator)
    def run_ixl_script(download_directory, **kwargs):
        logger = LoggingMixin().log
        logger.info('Starting run_ixl_script task')
        
        driver = setup_chrome_driver(download_directory)
        clear_directory(download_directory)
        
        def process(default_wait=30):

            try:
                login(driver, default_wait)

                teachers = {
                    'Math-Teacher-2-VPHS Placeholder': 'Math',
                    'Math-Teacher-3-VPHS Placeholder': 'Math',
                    'Science-Teach-1-VPHS Placeholder': 'Math',
                    # 'Amir Alhambra': 'Math',
                    'Dameon Turney': 'Math',
                    "Te'a Jones": 'Math',
                    'Zion McCutcheon': 'ELA', #Subject is not showing up for him.
                    'Tiffany Estrada': 'ELA',
                    'Cal Dobbs': 'ELA',
                    'Kennedy Jameison': 'ELA',
                    'Devin Price': 'ELA',
                    'William Fowler': 'ELA'
                }

                for teacher, subject in teachers.items():
                    make_selections(driver, subject, teacher, default_wait=30, download_dir=download_directory)
                
                check_file_count(download_directory, 11)
            finally:
                driver.quit()
                logging.info('Selenium Process has concluded')

            # file_checks(download_directory)
            normalize_files_in_directory(download_directory) 

            parent_dir = os.path.dirname(download_directory)
            normalized_dir = os.path.join(parent_dir, "normalized_files")
            stacked_df = stack_files_in_directory(normalized_dir)

            # ixl_scores_math = stacked_df.loc[stacked_df['subject'].isin(['Algebra 1', 'Algebra 2', 'Geometry'])].reset_index(drop=True)
            
            # send_to_gcs('ixlbucket-icefschools-1', save_path='', frame=ixl_scores_math, frame_name='ixl_scores_math.csv')
            send_to_gcs('ixlbucket-icefschools-1', save_path='', frame=stacked_df, frame_name='ixl_scores.csv')

            return(stacked_df)
        
        df = process()
        return df
    
    run_selenium_downloads = PythonOperator(
        task_id='run_ixl_script',
        python_callable=run_ixl_script,
        op_kwargs={
            'download_directory': download_directory,
        },
        dag=dag,
    )

    # Set task dependencies
    run_selenium_downloads
