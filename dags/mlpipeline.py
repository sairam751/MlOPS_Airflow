from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def preprocess_data():
    print("Data has been preprocessed.")

def train_model():
    print("Model has been trained.")

def evaluate_model():
    print("Model has been evaluated.")

with DAG(
'ML_pipeline',
start_date=datetime(2025, 1, 1),
schedule_interval='@weekly',

) as dag:

    preprocess =  PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data
)
train = PythonOperator(
    task_id='train_model',
    python_callable=train_model
)
evaluate = PythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model
)

preprocess >> train >> evaluate

    