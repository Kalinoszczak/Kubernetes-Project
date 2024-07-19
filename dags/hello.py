from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definiowanie domyślnych argumentów dla DAG
default_args = {
    'owner': 'Kalinoszczak',
    'start_date': datetime(2024, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Definiowanie DAG
dag = DAG(
    'hello',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
    catchup=False  # To prevent the DAG from running for past dates
)

# Definiowanie zadań
task1 = BashOperator(
    task_id='hello',
    bash_command='echo "Hello Guys"',
    dag=dag
)

task2 = BashOperator(
    task_id='hellov2',
    bash_command='echo "This is my Project"',
    dag=dag
)

# Określenie zależności między zadaniami
task1 >> task2