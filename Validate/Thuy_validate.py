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

