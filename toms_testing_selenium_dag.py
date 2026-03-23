import os
import sys
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

working_dir = '/home/g2015samtaylor/git_directory/state_testing/elpac_or_cers_selenium'
sys.path.append(working_dir)
# Import your custom modules
from modules.login import *
from modules.file_altering import *
from modules.schools_and_grade import *
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
    'toms_state_testing_selenium_dag',
    default_args=default_args,
    description='A Dag for State Testing Selenium donwloads ELPAC or CERS files',
    schedule_interval='10 4 * * 1-5',  
    catchup=False,
) as dag:
    
    # Define parameters for download directory and BigQuery
    download_directory = '/home/g2015samtaylor/git_directory/state_testing/elpac_or_cers_selenium/downloads/'
    bq_project_id = 'icef-437920'
    bq_dataset_id = 'state_testing'
    bq_table_id = 'state_testing_continuous_25-26'
    new_rows_dir = '/home/g2015samtaylor/git_directory/state_testing/elpac_or_cers_selenium/new_rows_daily'
    
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
    def run_state_testing_script(download_directory, **kwargs):
        logger = LoggingMixin().log
        logger.info('Starting run_state_testing_script task')

        driver = setup_chrome_driver(download_directory)
        clear_directory(download_directory)
        stacked = None

        try:
            login(driver, default_wait=30)  # This scrapes an old email if time is too short
            automation = SchoolGradeAutomation(driver, default_wait=15)
            school_name_strings = [
                'ICEF Vista Middle Academy', 'ICEF Vista Elementary Academy',
                'ICEF View Park Preparatory High', 'ICEF View Park Preparatory Middle',
                'ICEF View Park Preparatory Elementary', 'ICEF Innovation Los Angeles Charter',
                'ICEF Inglewood Elementary Charter Academy'
            ]
            for school in school_name_strings:
                automation.download_school_reports(school, '2025-26')

            target_dir = os.path.join(working_dir, 'stacked_dir')
            stacked = stack_files(download_directory, target_dir)
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
        finally:
            driver.quit()
            logging.info("Browser window closed.")

        if stacked is None:
            logging.warning("No stacked data; skipping transform and BQ write.")
            return

        # Transform: student number, scalescore, DFS, proficiency
        stacked = bring_in_student_number(stacked)
        stacked = change_scalescore_achievement(stacked)
        stacked['dfs_math'] = stacked.apply(lambda row: calculate_dfs(row, 'Math'), axis=1)
        stacked['dfs_ela'] = stacked.apply(lambda row: calculate_dfs(row, 'ELA'), axis=1)
        stacked['dfs_cast'] = stacked.apply(lambda row: calculate_dfs(row, 'CAST'), axis=1)
        stacked = stacked.apply(adjust_dfs_by_subject, axis=1)
        stacked['proficiency'] = stacked['ScaleScoreAchievementLevel'].apply(calculate_proficiency)

        # Append only new rows to BigQuery; daily stacked is already saved in stacked_dir by stack_files
        n = append_new_rows_to_bq(
            stacked,
            project_id=bq_project_id,
            dataset_id=bq_dataset_id,
            table_id=bq_table_id,
            new_rows_dir=new_rows_dir,
        )
        logger.info(f"Appended {n} new rows to BigQuery {bq_project_id}.{bq_dataset_id}.{bq_table_id}")
    
    run_selenium_downloads = PythonOperator(
        task_id='run_state_testing_script',
        python_callable=run_state_testing_script,
        op_kwargs={'download_directory': download_directory},
        dag=dag,
    )

    # Set task dependencies
    run_selenium_downloads


# <a angulartics2on="click" angularticsaction="AddAssessment" angularticscategory="AssessmentResults" href="javascript:void(0)" 
# class="tag tag-xs maroon ng-star-inserted">ELA Summative Grade 11</a>

#Definitely need to add some extensive checks when running this to see what got donwload for what school
#Maek the log look better on the errors. 