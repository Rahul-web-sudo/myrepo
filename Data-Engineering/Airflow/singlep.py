from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'k8s_single_pod_dag',
    default_args=default_args,
    description='A simple DAG running all tasks in one Kubernetes pod',
    schedule_interval=None,  # Manually trigger this DAG
)

# Define the common configuration for all tasks running in the same pod
pod_config = {
    'namespace': 'default',  # The namespace in which the pod will run
    'image': 'python:3.8',    # Example container image (change to the one needed for your tasks)
    'name': 'shared-airflow-task-pod',  # Shared pod name across all tasks
    'is_delete_operator_pod': True,  # Automatically delete the pod after the task is done
    'retries': 1,   # Number of retries in case of failure
}

# Task 1
task_1 = KubernetesPodOperator(
    task_id='task_1',  # Explicit task_id here
    name=pod_config['name'],  # Use the same pod name across tasks
    cmds=['python', '-c', 'print("Task 1 running")'],
    **pod_config,  # Pass the pod configuration
    dag=dag
)

# Task 2
task_2 = KubernetesPodOperator(
    task_id='task_2',  # Explicit task_id here
    name=pod_config['name'],  # Use the same pod name across tasks
    cmds=['python', '-c', 'print("Task 2 running")'],
    **pod_config,  # Pass the pod configuration
    dag=dag
)

# Task 3
task_3 = KubernetesPodOperator(
    task_id='task_3',  # Explicit task_id here
    name=pod_config['name'],  # Use the same pod name across tasks
    cmds=['python', '-c', 'print("Task 3 running")'],
    **pod_config,  # Pass the pod configuration
    dag=dag
)

# Set task dependencies so that they run sequentially in the same pod
task_1 >> task_2 >> task_3
