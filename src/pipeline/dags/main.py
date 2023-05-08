import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from ..calculation import download_data, ConvertToStructured, FeatureEngineering
from ..model_training import train_etfs_model, train_stocks_model

# Add the parent directory (project) to the sys.path list
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 4),
    'depends_on_past': False,
}

dag = DAG(
    'Stocks ETFs Pipeline',
    default_args=default_args,
    description='Stocks/Etfs records for model predictions',
    schedule_interval=None,
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable= download_data(),
    dag=dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=ConvertToStructured,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=dag,
)

task_3 = PythonOperator(
    task_id='load_data',
    python_callable=FeatureEngineering,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=dag,
)
task_4 = PythonOperator(
    task_id='load_data',
    python_callable=FeatureEngineering,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=dag,
)
task_5 = PythonOperator(
    task_id='load_data',
    python_callable=train_stocks_model(),
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=dag,
)

task_6 = PythonOperator(
    task_id='load_data',
    python_callable=train_etfs_model(),
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=dag,
)


task_1 >> task_1 >> task_3 >> task_4>> task_5>> task_6
