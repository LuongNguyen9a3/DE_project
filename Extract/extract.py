from datetime import datetime, timedelta
import os
import sqlite3

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule




# PATH CONFIGURATION
# Base directory where all data files will be stored
RAW_DATA_DIR = "/home/vboxuser/airflow/datasets"

# Path to the raw CSV file after downloading and renaming
RAW_DATA_PATH = os.path.join(RAW_DATA_DIR, "weather_data.csv")

# Paths for transformed CSV outputs (daily & monthly)
DAILY_CSV_PATH = os.path.join(RAW_DATA_DIR, "daily_weather.csv")
MONTHLY_CSV_PATH = os.path.join(RAW_DATA_DIR, "monthly_weather.csv")

# Path to the SQLite database file
DB_PATH = os.path.join(RAW_DATA_DIR, "weather_his.db")


# DEFAULT ARGS FOR DAG
default_args = {
    "owner": "group17",                  
    "depends_on_past": False,            
    "start_date": datetime(2025, 11, 20),
    "retries": 1,                        
    "retry_delay": timedelta(minutes=2), 
}



# STEP 1: EXTRACT - DOWNLOAD & UNZIP KAGGLE DATASET
def extract_weather_data(**kwargs):
    
    # TaskInstance object, used to interact with XCom
    ti = kwargs["ti"]

    # Ensure the data directory exists (create it if it does not)
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

    # 3. Try to find the exact file 'weatherHistory.csv' first
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
