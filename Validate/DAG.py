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
