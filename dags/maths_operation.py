from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime



def start_number(**context):
    context['ti'].xcom_push(key='current_value', value=10)
    print('The current value is 10')


def add_five(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='start_number')
    new_value = current_value + 5
    context['ti'].xcom_push(key='current_value', value=new_value)
    print(f'The new value is {new_value}')

def multiply_by_two(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='add_five')
    new_value = current_value * 2
    context['ti'].xcom_push(key='current_value', value=new_value)
    print(f'The new value is {new_value}')

def subtract_three(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='multiply_by_two')
    new_value = current_value - 3
    context['ti'].xcom_push(key='current_value', value=new_value)
    print(f'The new value is {new_value}')

def square_number(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='subtract_three')
    new_value = current_value ** 2
    context['ti'].xcom_push(key='current_value', value=new_value)
    print(f'The new value is {new_value}')

with DAG(
    dag_id='maths_operation',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@once',
    catchup=False,
) as dag:
    
    start_number = PythonOperator(
        task_id='start_number',
        python_callable=start_number,
        provide_context=True
    )

    add_five = PythonOperator(
        task_id='add_five',
        python_callable=add_five,
        provide_context=True
    )

    multiply_by_two = PythonOperator(
        task_id='multiply_by_two',
        python_callable=multiply_by_two,
        provide_context=True
    )

    subtract_three = PythonOperator(
        task_id='subtract_three',
        python_callable=subtract_three,
        provide_context=True
    )

    square_number = PythonOperator(
        task_id='square_number',
        python_callable=square_number,
        provide_context=True
    )

    start_number >> add_five >> multiply_by_two >> subtract_three >> square_number

