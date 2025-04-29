from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_data(ti):
    ti.xcom_push(key="name",value = "NithishKumar")
    
def pull_data(ti):
    pulled_value = ti.xcom_pull(key="name",task_ids="push_task")
    print(f"Received Message: {pulled_value} ")
    
with DAG(
    dag_id = "xcom_task",
    schedule="@daily",
    start_date= datetime(2025,1,1),
    catchup = False,
) as dag:
    
    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_data
    )
    
    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_data
    )
    
    push_task >> pull_task