import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# Define your Python script function to be executed for analysis
def run_analysis():
    # Replace 'your_analysis_script.py' with the actual path to your analysis script
    os.system("dags/scripts/analysis_report.ipynb")

# ===============================================
default_args = {
    "owner": "sourabh",
    "start_date": datetime(2023, 10, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'email': ['sourabhmehta06@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# Create the DAG
dag = DAG(dag_id="project", schedule_interval="@once", default_args=default_args, catchup=False)

# Define the task to run the analysis script
run_analysis_task = PythonOperator(
    task_id="run_analysis",
    python_callable=run_analysis,
    dag=dag,
)

# Define the existing task for handling missing values
def missing_value(**context):
    path = os.path.join(os.getcwd(), "dags/data/beer_profile_and_ratings.csv")
    df = pd.read_csv(path)
    print(df.columns)
    missing_value = df.isnull().sum() / len(df)
    print(missing_value)
    df.dropna(inplace=True)
    path = os.path.join(os.getcwd(), "dags/data/process.csv")
    df.to_csv(path)

# Define the existing task for handling missing values
missing_value_task = PythonOperator(
    task_id="missing_value",
    python_callable=missing_value,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
missing_value_task >> run_analysis_task