import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Get the directory containing this file (my_dag.py)
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the parent directory (project) to the sys.path list
project_dir = os.path.join(current_dir, '..')
sys.path.append(project_dir)
# from final import TaskTwo, TaskOne
# Now you can import classes or modules from final.py
from ..sample_file import TaskTwo, TaskOne

dag_path = os.getcwd()
dag = DAG('dag_test', start_date=datetime.now())


def task_one():
    spark = SparkSession.builder.appName("Print PySpark version").getOrCreate()
    # Import necessary libraries and classes
    symbols_meta_file = f"{dag_path}/data/symbols_valid_meta.csv"

    # Instantiate TaskOne class
    t1 = TaskOne(symbols_meta_file)
    output_dir = Path(f'{dag_path}/processed_data/one.parquet')
    output_dir.mkdir(parents=True, exist_ok=True)
    # Process data and save to processed_data folder
    t1.process_data()


def task_two():
    # Import necessary libraries and classes
    from simple_app.final import TaskTwo

    # Instantiate TaskTwo class
    t2 = TaskTwo()

    # Process data and save to final_processed folder
    t2.process_data()


t1 = PythonOperator(
    task_id='task_one',
    python_callable=task_one,
    dag=dag,
)

t2 = PythonOperator(
    task_id='task_two',
    python_callable=task_two,
    dag=dag,
)

# Set task dependencies
t1 >> t2
