from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

# DAG
with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    extract_task = BashOperator(
        task_id="extract_data",
        bash_command='python /opt/airflow/dags/extract_data.py'
    )

    transform_task = BashOperator(
        task_id="transform_data",
        bash_command='python /opt/airflow/dags/transform_data.py'
    )

    load_task = BashOperator(
        task_id="load_data",
        bash_command='python /opt/airflow/dags/load_data.py'
    )

    extract_task >> transform_task >> load_task