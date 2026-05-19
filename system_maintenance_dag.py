from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email": ["2015samtaylor@gmail.com"],
}


with DAG(
    dag_id="system_maintenance",
    default_args=default_args,
    description="Weekly cleanup of Docker artifacts and old Airflow logs.",
    schedule_interval="15 3 * * 0",  # Sundays at 3:15 AM
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    docker_prune = BashOperator(
        task_id="docker_system_prune",
        bash_command=(
            "docker system prune -a -f --filter 'until=168h' "
            "&& docker system df"
        ),
    )

    cleanup_airflow_logs = BashOperator(
        task_id="cleanup_old_airflow_logs",
        bash_command=(
            "find /home/g2015samtaylor/airflow/logs -type f -mtime +30 -delete "
            "&& find /home/g2015samtaylor/airflow/logs -type d -empty -delete "
            "&& du -sh /home/g2015samtaylor/airflow/logs"
        ),
    )

    docker_prune >> cleanup_airflow_logs
