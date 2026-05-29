from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Define default configuration arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Initialize the DAG context
with DAG(
    dag_id='my_first_simple_dag',       # The unique ID that shows up in your Airflow Web UI
    default_args=default_args,
    description='A basic template for a Bash pipeline',
    schedule_interval=None,             # "None" means it will only run manually when you click 'Trigger'
    start_date=datetime(2026, 1, 1),    # Start date matching the current timeline
    catchup=False                     # Prevents running historical backfills upon activation
    tags=[learning]
) as dag:

    # 3. Define Task 1: Print a text string
    task_one = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello World! Your CI/CD sync is working perfectly!"',
    )

    # 4. Define Task 2: Check system date
    task_two = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # 5. Set the execution order (Dependency)
    # This tells Airflow to run task_one first, and only run task_two if task_one succeeds.
    task_one >> task_two