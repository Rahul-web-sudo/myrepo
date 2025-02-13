from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define a simple Python function
def print_hello():
    print("Hello, World!")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 2, 12),
}

dag = DAG(
    'simple_dag_example',
    default_args=default_args,
    description='A simple DAG with multiple tasks',
    schedule_interval=None,  # Run manually for this example
    access_control={
        'Admin': {'can_read', 'can_edit', 'can_delete'},  # Admin role can read, edit, and delete
        'All': {'can_read'},  # All users can read the DAG
    },
)

# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=print_hello,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=print_hello,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=print_hello,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> task_1 >> task_3 >> end_task
start_task >> task_2 >> task_3 >> end_task
