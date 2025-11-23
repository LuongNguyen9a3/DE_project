
# LIBRARIES
from datetime import datetime, timedelta
import os
import sqlite3

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule



# PATH CONFIGURATION
# Base directory where all data files will be stored
RAW_DATA_DIR = "/home/thuypham/airflow/datasets"

# Path to the raw CSV file after downloading and renaming
RAW_DATA_PATH = os.path.join(RAW_DATA_DIR, "weather_data.csv")

# Paths for transformed CSV outputs (daily & monthly)
DAILY_CSV_PATH = os.path.join(RAW_DATA_DIR, "daily_weather.csv")
MONTHLY_CSV_PATH = os.path.join(RAW_DATA_DIR, "monthly_weather.csv")

# Path to the SQLite database file
DB_PATH = os.path.join(RAW_DATA_DIR, "weather_his.db")



# DEFAULT ARGS FOR DAG
default_args = {
    "owner": "group17",                  # Owner name shown in Airflow UI
    "depends_on_past": False,            # Each run is independent of previous runs
    "start_date": datetime(2025, 11, 20),
    "retries": 1,                        # Number of retries if a task fails
    "retry_delay": timedelta(minutes=2), # Delay between retries
}



# STEP 1: EXTRACT - DOWNLOAD & UNZIP KAGGLE DATASET
def extract_weather_data(**kwargs):
    """
    Step 1: Extract
    - Authenticate with Kaggle API.
    - Download and unzip dataset 'muthuj7/weather-dataset' to RAW_DATA_DIR.
    - Look for 'weatherHistory.csv' (main file of the dataset).
    - If it cannot be found by name, fall back to the first CSV file in the folder.
    - Rename the selected CSV file to 'weather_data.csv'.
    - Push the CSV path to XCom so the Transform step can read it.
    """
    # TaskInstance object, used to interact with XCom
    ti = kwargs["ti"]

    # Ensure the data directory exists (create it if it does not)
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    # 1. Authenticate with Kaggle API using kaggle.json credentials
    api = KaggleApi()
    api.authenticate()

    # 2. Download & unzip the dataset directly into RAW_DATA_DIR
    #    unzip=True means the ZIP file will be extracted automatically
    api.dataset_download_files(
        "muthuj7/weather-dataset",  # Dataset slug from Kaggle URL
        path=RAW_DATA_DIR,          # Target folder for download
        force=True,                 # Overwrite any existing files
        unzip=True,                 # Automatically unzip the dataset
    )

    # 3. Try to find the exact file 'weatherHistory.csv' first
    target_name = "weatherHistory.csv"
    candidate_path = os.path.join(RAW_DATA_DIR, target_name)

    if not os.path.exists(candidate_path):
        # If 'weatherHistory.csv' is not found, look for any CSV file
        csv_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]
        if not csv_files:
            # If no CSV at all, fail early
            raise FileNotFoundError(
                "No CSV file found after downloading from Kaggle."
            )
        # Use the first CSV file found as a fallback
        candidate_path = os.path.join(RAW_DATA_DIR, csv_files[0])

    # 4. Normalize the file name to 'weather_data.csv'
    #    This makes it easier for the next steps to always use the same path
    if candidate_path != RAW_DATA_PATH:
        os.rename(candidate_path, RAW_DATA_PATH)

    # 5. Push the final CSV path to XCom for downstream tasks (Transform)
    ti.xcom_push(key="raw_dataset_path", value=RAW_DATA_PATH)

    print(f"[Extract] Completed. CSV available at: {RAW_DATA_PATH}")



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

    # 6. Create wind speed in m/s and the corresponding wind_strength category
    df["Wind Speed (m/s)"] = df["Wind Speed (km/h)"] / 3.6
    df["wind_strength"] = df["Wind Speed (m/s)"].apply(
        categorize_wind_strength
    )

    # DAILY AGGREGATION
    # Use the datetime column as index to resample by day
    df_daily = df.set_index("Formatted Date")
    
    def mode_precip(series: pd.Series):
        """
        Helper: return the most frequent precipitation type within a group.
        If there is no clear mode, return None.
        """
        modes = series.mode()
        if modes.empty:
            return None
        return modes.iloc[0]

    # Resample hourly data to daily and compute averages / modes
    daily = df_daily.resample("D").agg(
        {
            "Temperature (C)": "mean",
            "Apparent Temperature (C)": "mean",
            "Humidity": "mean",
            "Wind Speed (km/h)": "mean",
            "Visibility (km)": "mean",
            "Pressure (millibars)": "mean",
            "Precip Type": mode_precip,
            "Wind Speed (m/s)": "mean",
        }
    )

    # Categorize daily average wind speed
    daily["wind_strength"] = daily["Wind Speed (m/s)"].apply(
        categorize_wind_strength
    )

    # Reset index to get 'Formatted Date' back as a normal column
    # and rename columns to match the project schema
    daily = daily.reset_index().rename(
        columns={
            "Formatted Date": "formatted_date",
            "Temperature (C)": "temperature_c",
            "Apparent Temperature (C)": "apparent_temperature_c",
            "Humidity": "humidity",
            "Wind Speed (km/h)": "wind_speed_kmh",
            "Visibility (km)": "visibility_km",
            "Pressure (millibars)": "pressure_millibars",
            "Precip Type": "precip_type",
        }
    )

    # Remove helper column no longer needed in the final table
    daily = daily.drop(columns=["Wind Speed (m/s)"])


    # Create avg_* columns (required for the daily_weather table schema)
    daily["avg_temperature_c"] = daily["temperature_c"]
    daily["avg_humidity"] = daily["humidity"]
    daily["avg_wind_speed_kmh"] = daily["wind_speed_kmh"]

    # MONTHLY AGGREGATION 
    # Create a "month" period column (e.g., 2006-04)
    df["month"] = df["Formatted Date"].dt.to_period("M")

    # Aggregate numeric values by month (monthly averages)
    monthly_num = df.groupby("month").agg(
        {
            "Temperature (C)": "mean",
            "Apparent Temperature (C)": "mean",
            "Humidity": "mean",
            "Wind Speed (km/h)": "mean",
            "Visibility (km)": "mean",
            "Pressure (millibars)": "mean",
        }
    )

    # Compute the monthly mode of precipitation type
    monthly_precip = df.groupby("month")["Precip Type"].agg(mode_precip)

    # Combine numeric aggregates with precipitation mode
    monthly = monthly_num.join(monthly_precip).reset_index().rename(
        columns={
            "Temperature (C)": "avg_temperature_c",
            "Apparent Temperature (C)": "avg_apparent_temperature_c",
            "Humidity": "avg_humidity",
            "Wind Speed (km/h)": "avg_wind_speed_kmh",
            "Visibility (km)": "avg_visibility_km",
            "Pressure (millibars)": "avg_pressure_millibars",
            "Precip Type": "mode_precip_type",
        }
    )
    # Convert Period type to string for easier storage
    monthly["month"] = monthly["month"].astype(str)

    # 7. Save daily and monthly tables to CSV files
    daily.to_csv(DAILY_CSV_PATH, index=False)
    monthly.to_csv(MONTHLY_CSV_PATH, index=False)

    # 8. Push output file paths to XCom for the Validate step
    ti.xcom_push(key="daily_csv_path", value=DAILY_CSV_PATH)
    ti.xcom_push(key="monthly_csv_path", value=MONTHLY_CSV_PATH)

    print("[Transform] Completed. Daily and monthly CSV files created.")



# STEP 3: VALIDATE - DATA QUALITY CHECKS
def validate_weather_data(**kwargs):
    """
    Step 3: Validate
    - Load daily and monthly CSV files.
    - Make sure there are no missing values in critical fields.
    - Check that values are within a reasonable range.
    - Detect temperature outliers with a simple z-score.
    - Push validated paths forward for the Load step.
    """
    ti = kwargs["ti"]

    # Retrieve file paths from XCom
    daily_path = ti.xcom_pull(
        key="daily_csv_path", task_ids="transform_weather_data"
    )
    monthly_path = ti.xcom_pull(
        key="monthly_csv_path", task_ids="transform_weather_data"
    )

    # Basic existence checks
    if not os.path.exists(daily_path):
        raise FileNotFoundError("Daily CSV not found.")
    if not os.path.exists(monthly_path):
        raise FileNotFoundError("Monthly CSV not found.")

    # Load data
    daily = pd.read_csv(daily_path)
    monthly = pd.read_csv(monthly_path)

    # 1. Missing value checks in critical columns
    daily_critical = ["temperature_c", "humidity", "wind_speed_kmh"]
    monthly_critical = [
        "avg_temperature_c",
        "avg_humidity",
        "avg_wind_speed_kmh",
    ]

    if daily[daily_critical].isna().any().any():
        raise ValueError("Missing values detected in daily critical fields.")
    if monthly[monthly_critical].isna().any().any():
        raise ValueError("Missing values detected in monthly critical fields.")

    # 2. Range checks to catch invalid values
    #    Temperature between -50 and 50 °C
    if not daily["temperature_c"].between(-50, 50).all():
        raise ValueError("Daily temperature out of expected range [-50, 50].")

    #    Humidity between 0 and 1
    if not daily["humidity"].between(0, 1).all():
        raise ValueError("Daily humidity out of expected range [0, 1].")

    #    Wind speed must be non-negative
    if (daily["wind_speed_kmh"] < 0).any():
        raise ValueError("Negative wind speed found in daily data.")

    # 3. Outlier detection using z-score on temperature
    temp_mean = daily["temperature_c"].mean()
    temp_std = daily["temperature_c"].std()

    if temp_std > 0:
        z_scores = (daily["temperature_c"] - temp_mean) / temp_std
        outliers = daily[abs(z_scores) > 4]  # temperatures far from the mean
        if not outliers.empty:
            print("[Validate] Warning: temperature outliers detected:")
            print(outliers[["formatted_date", "temperature_c"]])

    # 4. Push validated paths to XCom for the Load step
    ti.xcom_push(key="validated_daily_path", value=daily_path)
    ti.xcom_push(key="validated_monthly_path", value=monthly_path)

    print("[Validate] Completed. Data quality checks passed.")



# STEP 4: LOAD - WRITE DATA INTO SQLITE DATABASE
def load_weather_data(**kwargs):
    """
    Step 4: Load

    - Read validated daily and monthly CSV files.
    - Write them into two SQLite tables:
      * daily_weather
      * monthly_weather
    """
    ti = kwargs["ti"]

    # Retrieve validated CSV paths from XCom
    daily_path = ti.xcom_pull(
        key="validated_daily_path", task_ids="validate_weather_data"
    )
    monthly_path = ti.xcom_pull(
        key="validated_monthly_path", task_ids="validate_weather_data"
    )

    daily = pd.read_csv(daily_path)
    monthly = pd.read_csv(monthly_path)

    # Connect to SQLite database (it will be created if it does not exist)
    conn = sqlite3.connect(DB_PATH)

    # Write daily data into table 'daily_weather'
    daily.to_sql("daily_weather", conn, if_exists="replace", index=False)

    # Write monthly data into table 'monthly_weather'
    monthly.to_sql("monthly_weather", conn, if_exists="replace", index=False)

    # Always close the connection
    conn.close()

    print(f"[Load] Completed. Data written to SQLite DB at {DB_PATH}.")



# DAG DEFINITION - ORCHESTRATE ALL STEPS
with DAG(
    dag_id="weather_history_etl_group17",         # Name of the DAG in Airflow UI
    default_args=default_args,            # Default task arguments
    schedule_interval="@daily",           # Run once per day
    catchup=False,                        # Do not backfill past dates
    description="ETL pipeline for historical weather data (group17)",
) as dag:

    # Task 1: Extract from Kaggle
    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
    )

    # Task 2: Transform into daily & monthly tables
    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    # Task 3: Validate data before loading
    validate_task = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_weather_data,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # run only if previous tasks succeeded
    )

    # Task 4: Load into SQLite database
    load_task = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_weather_data,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Define the ETL workflow: Extract -> Transform -> Validate -> Load
    extract_task >> transform_task >> validate_task >> load_task
