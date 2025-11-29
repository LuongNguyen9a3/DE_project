from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import sqlite3
import os

# STEP 2: TRANSFORM - CLEAN & AGGREGATE DATA
def categorize_wind_strength(speed_ms: float) -> str | None:
    """
    Map numeric wind speed (m/s) into wind strength categories.
    The categories are based on a Beaufort-like scale.
    """
    if pd.isna(speed_ms):
        return None
    if 0 <= speed_ms <= 1.5:
        return "Calm"
    elif 1.6 <= speed_ms <= 3.3:
        return "Light Air"
    elif 3.4 <= speed_ms <= 5.4:
        return "Light Breeze"
    elif 5.5 <= speed_ms <= 7.9:
        return "Gentle Breeze"
    elif 8.0 <= speed_ms <= 10.7:
        return "Moderate Breeze"
    elif 10.8 <= speed_ms <= 13.8:
        return "Fresh Breeze"
    elif 13.9 <= speed_ms <= 17.1:
        return "Strong Breeze"
    elif 17.2 <= speed_ms <= 20.7:
        return "Near Gale"
    elif 20.8 <= speed_ms <= 24.4:
        return "Gale"
    elif 24.5 <= speed_ms <= 28.4:
        return "Strong Gale"
    elif 28.5 <= speed_ms <= 32.6:
        return "Storm"
    else:
        return "Violent Storm"


def transform_weather_data(**kwargs):
    """
    Step 2: Transform
    - Read the raw CSV file from the Extract step.
    - Clean the data (convert date, handle missing values, drop duplicates).
    - Create a 'wind_strength' feature based on wind speed.
    - Aggregate data to daily and monthly levels.
    - Save daily and monthly tables as CSV files.
    - Push CSV paths to XCom for the Validate step.
    """
    ti = kwargs["ti"]

    # 1. Get raw CSV path from XCom (output of extract_weather_data)
    raw_csv_path = ti.xcom_pull(
        key="raw_dataset_path", task_ids="extract_weather_data"
    )
    if not raw_csv_path or not os.path.exists(raw_csv_path):
        raise FileNotFoundError("Raw CSV path from XCom is invalid.")

    # 2. Load data from the CSV file
    df = pd.read_csv(raw_csv_path)

    # 3. Convert 'Formatted Date' to timezone-naive datetime and drop invalid rows
    df["Formatted Date"] = pd.to_datetime(
    df["Formatted Date"], errors="coerce", utc=True  # parse as UTC tz-aware
    )
    # Remove timezone info to get plain datetime64[ns]
    df["Formatted Date"] = df["Formatted Date"].dt.tz_localize(None)
    df = df.dropna(subset=["Formatted Date"])
    # 4. Fill missing values in important numeric columns using the median
    numeric_cols = [
        "Temperature (C)",
        "Apparent Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)",
        "Visibility (km)",
        "Pressure (millibars)",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # 5. Remove any duplicated rows to avoid double-counting
    df = df.drop_duplicates()
