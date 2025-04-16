from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
path_variable = Variable.get("PATH")

# Define the DAG
with DAG(
    'dbt-test',
    default_args=default_args,
    description='Run the ixl_scores_math dbt model',
    schedule_interval='@daily',  # Adjust the schedule as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:


    dbt_test_ixl = BashOperator(
        task_id='run_ixl_scores_math_model',
        bash_command='cd /home/g2015samtaylor/git_directory/dbt && dbt test --select models/production/ixl/ixl_scores_math.sql',
        env={
            'DBT_PROFILES_DIR': '/home/g2015samtaylor/.dbt',  # Path to dbt profiles
            'HOME': '/home/g2015samtaylor',  # Explicitly set the HOME variable
            'PATH': path_variable
        },
    )

    dbt_test_state_test_continuous = BashOperator(
        task_id='run_state_test_continuous',
        bash_command='cd /home/g2015samtaylor/git_directory/dbt && dbt test --select models/production/state_testing_continuous/state_testing_continuous.sql',
        env={
            'DBT_PROFILES_DIR': '/home/g2015samtaylor/.dbt',  # Path to dbt profiles
            'HOME': '/home/g2015samtaylor',  # Explicitly set the HOME variable
            'PATH': path_variable
        },
    )

# -- create dbt test for main, and move all created views to dbt setup. 