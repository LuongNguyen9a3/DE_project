from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import sqlite3
import os


DATA_PATH = '/home/vboxuser/airflow/dataset/weatherHistory.csv'
DB_PATH = '/home/vboxuser/airflow/dataset/weather_his.db'

default_args = {
    'owner': 'group17',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes(2)),
}

#EXTRACT
def extract_wh(**context):
    context['ti'].xcom_push(key='dataset_path', value=DATA_PATH)

# TRANSFORM:
def transf_wh(**context):
    file_path = context['ti'].xcom_pull(
        key='dataset_path',
        task_ids='extract_weather_history'
    )

    df = pd.read_csv(file_path)

    # Handle missing data
    cols = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Wind Bearing (degrees)', 'Visibility (km)']
    for col in cols:
        if col in df.columns:
            df[col] = df[col].fillna(df['col'].median())
            
    # Convert Formatted Date to a proper date format.
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'])

    # Check for duplicates and remove 
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    if before != after:
        print(f'Removed {before - after} duplicate rows.')