from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import os
import sys
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
working_dir = os.path.join(os.environ['AIRFLOW_HOME'], 'git_directory/Common_Lit')
sys.path.append(working_dir)
from modules.login import *
from modules.downloads import *
from modules.confirming import *
from modules.normalizing import *


def selenium_pipeline():
    download_directory = os.path.join(working_dir, 'downloads')
    backup_directory = os.path.join(working_dir, 'downloads_backups')
    assessment_names_file_path = os.path.join(working_dir, 'assignments_folder', 'assignment_names_commonlit.csv')
    assignment_scrape = os.path.join(working_dir, 'assignments_folder', 'assignment_scrape.csv')

    # Backup and clear downloads directory
    backup_and_clear_directory(download_directory, backup_directory)

    # Initialize WebDriver
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        'download.default_directory': download_directory,
        'profile.default_content_setting_values.automatic_downloads': 1,
    }
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_driver_path = '/home/g2015samtaylor/Desktop/chromedriver-linux64/chromedriver'
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # Login and launch application
        login(driver)
        launch_commonlit(driver)

        # Scrape assignments
        names = get_assessment_list(path=assessment_names_file_path)
        for index, row in names.iterrows():
            id_value = row['id']
            text_value = row['text']
            open_sidebar(driver)
            get_specific_assignment(driver, f'''{text_value}''', id_value)

        # Process downloaded files
        process_missing_files(driver, download_directory, assignment_scrape)
        remove_duplicates(download_directory=download_directory)

        # Normalize and save data
        df = stack(directory=download_directory, all_students_arg=False)
        df = assign_unique_ids(df, 'commonlit_assessments')
        df = apply_unit_assessment_col(df)

        try:
            df.to_csv('/home/g2015samtaylor/common_lit/unit_skills_assessments.csv', index=False)

            source_path = '/home/g2015samtaylor/common_lit/unit_skills_assessments.csv'
            destination_path = '/home/g2015samtaylor/backups/unit_skills_assessments.csv'

            # Copy the file
            shutil.copy(source_path, destination_path)
            logging.info('csv has been written to common_lit directory, and copied to backup dir')

        except Exception as e:
            logging.error(f'Unable to write common_lit csv to directory due to {e}')
            
    finally:
        # Cleanup WebDriver
        driver.quit()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email': ['2015samtaylor@gmail.com']
}

dag = DAG(
    'commonlit_selenium_dag',
    default_args=default_args,
    description='A pipeline to automate Selenium tasks',
    schedule_interval='0 1 * * 1',  # Runs at 1:00 AM on Mondays,
)

# Define tasks
selenium_task = PythonOperator(
    task_id='selenium_pipeline',
    python_callable=selenium_pipeline,
    execution_timeout=timedelta(hours=3),
    dag=dag
)


#In the future could implement some type of bacup that re-launches the window