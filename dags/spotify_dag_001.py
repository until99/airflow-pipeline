from airflow import DAG
from airflow.decorators import task

from datetime import timedelta, datetime

with DAG(
    dag_id='spotify_dag_001',
    description='Spotify ETL data',
    
    start_date=datetime(2024, 9, 8),
    schedule="0 0 * * *",
    
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['gabriel.kasten@outlook.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    
    tags=['spotify', 'homologation', 'test_dag']

) as dag:
    
    @task()
    def extract_spotify_data():
        """
        TASK: extract_spotify_data
        DESCRIPTION: TASK DOCUMENTATION
        KNOW PROBLEMS: TASK MOST COMMOM PROBLEMS
        """
        # Imported the taks inside it call for optimal request,
        # this way the function is only readed when the task is gonna be run, not when the dag is scaned.
        # If some error happens, we don't lose performance importing a function that will not be loaded
        # from steps.spotify_etl import run_spotify_etl
        

        # from steps.spotify_etl import run_spotify_etl
        # spotify_api_response = run_spotify_etl
        
        print('extract')
        # raise Exception('fudeo')

    @task()
    def trasnform_spotify_data():
        """
        TASK: trasnform_spotify_data
        DESCRIPTION: TASK DOCUMENTATION
        KNOW PROBLEMS: TASK MOST COMMOM PROBLEMS
        """
        print('transform')

    extract_spotify_data() >> trasnform_spotify_data()