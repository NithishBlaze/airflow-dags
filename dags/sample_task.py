from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print('Hii , from airflow !')

def square_task():
    number = 5
    square_value = number**2
    print("The square value of given number is ", square_value)

with DAG(
    dag_id = "first_dag",
    start_date=datetime(2024,1,1),
    schedule='@daily',
    catchup = False,
) as dag:
    
        task1 = PythonOperator(
        task_id ="python_task",
        python_callable = my_task
        
        )
        
        task2 = PythonOperator(
            task_id="task2",
            python_callable=square_task

        )
    
        task2 >> task1