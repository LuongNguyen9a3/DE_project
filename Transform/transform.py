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
