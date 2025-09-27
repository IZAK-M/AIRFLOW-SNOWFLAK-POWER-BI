from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print('Today is {}'.format(datetime.today().date()))

def print_random_quote():
    quote = [
        {
            "quote": "I work six months and get three or four with the family. I've stopped racing to get to the red light.",
            "author": "Kyle Chandler",
            "category": "family"
        }
    ]    
    print(quote)

dag = DAG(
    'welcome_dag',
    default_args={'start_date': datetime.now() - timedelta(days=1)},
    schedule ='0 23 * * *',
    catchup=False
)

print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)

print_random_quote = PythonOperator(
    task_id='print_random_quote',
    python_callable=print_random_quote,
    dag=dag
)

# Set the dependencies between the tasks
print_welcome_task >> print_date_task >> print_random_quote