from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

def generate_random(ti):
    number = random.randint(0,100)
    ti.xcom_push(key="random_number", value = number)
    print(f"Generated numer {number}")

def check_odd_even(ti):
    pulled_value = ti.xcom_pull(key='random_number', task_ids = "generate" )
    
    if (pulled_value%2 == 0):
        print(f"{pulled_value} is even !")
    else:
        print(f"{pulled_value} is odd !")
        
with DAG(
    dag_id = "even_odd",
    schedule="@daily",
    start_date=datetime(2025,1,1),
    catchup = False,
) as dag:
    
    generate = PythonOperator(
        task_id="generate",
        python_callable=generate_random
    )
    
    check_number = PythonOperator(
        task_id="check_number",
        python_callable=check_odd_even
    )