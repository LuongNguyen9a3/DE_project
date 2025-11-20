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