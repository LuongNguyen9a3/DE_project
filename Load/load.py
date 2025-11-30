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