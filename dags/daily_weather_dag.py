from airflow.decorators import dag, task
from datetime import datetime, timedelta

from weather_etl import run_weather_etl

@dag(
    dag_id='weather_etl',
    # schedule_interval="@daily",
    schedule_interval="30 1 * * *",
    start_date=datetime(2024, 9, 10),
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=30),
    },
    tags=['weather', 'daily', 'production']
)

def daily_weather_etl():
    
    @task
    def weather_data():
        """
        TASK: weather_data
        DESCRIPTION: This task is responsible for transforming weather data.
        OPERATIONS:
        1. Loads raw weather data.
        2. Applies necessary transformations for analysis, such as data cleaning and aggregation.
        3. Stores the transformed data in a suitable format for further analysis or visualization.
        """

        run_weather_etl()
        
    weather_data = weather_data()

daily_weather_etl()
