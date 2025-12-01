from datetime import datetime, timedelta
import os
import sqlite3

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

RAW_DATA_DIR = "/home/vboxuser/airflow/datasets"

RAW_DATA_PATH = os.path.join(RAW_DATA_DIR, "weather_data.csv")

DAILY_CSV_PATH = os.path.join(RAW_DATA_DIR, "daily_weather.csv")
MONTHLY_CSV_PATH = os.path.join(RAW_DATA_DIR, "monthly_weather.csv")

DB_PATH = os.path.join(RAW_DATA_DIR, "weather_his.db")

default_args = {
    "owner": "group17",                  
    "depends_on_past": False,            
    "start_date": datetime(2025, 11, 20),
    "retries": 1,                        
    "retry_delay": timedelta(minutes=2),
}

# EXTRACT - DOWNLOAD & UNZIP KAGGLE DATASET
def extract_weather_data(**kwargs):
    ti = kwargs["ti"]
    
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    # 1. Authenticate with Kaggle API using kaggle.json credentials
    api = KaggleApi()
    api.authenticate()

    # 2. Download & unzip the dataset directly into RAW_DATA_DIR
    api.dataset_download_files(
        "muthuj7/weather-dataset",  
        path=RAW_DATA_DIR,         
        force=True,                 
        unzip=True,                
    )

    # 3. Find the exact file 'weatherHistory.csv' first
    target_name = "weatherHistory.csv"
    candidate_path = os.path.join(RAW_DATA_DIR, target_name)

    if not os.path.exists(candidate_path):
        csv_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(
                "No CSV file found after downloading from Kaggle."
            )
        candidate_path = os.path.join(RAW_DATA_DIR, csv_files[0])

    # 4. Normalize the file name to 'weather_data.csv'
    if candidate_path != RAW_DATA_PATH:
        os.rename(candidate_path, RAW_DATA_PATH)

    # 5. Push the final CSV path to XCom for downstream tasks (Transform)
    ti.xcom_push(key="raw_dataset_path", value=RAW_DATA_PATH)

    print(f"[Extract] Completed. CSV available at: {RAW_DATA_PATH}")
