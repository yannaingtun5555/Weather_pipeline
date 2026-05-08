from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from pipeline.extract import extract
from pipeline.transform import transform_data
from pipeline.load import load_data
from pipeline.table_create import create_table

@dag(
    dag_id='etl_pipeline_taskflow',
    default_args={
        'owner': 'data_team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2024, 1, 1),
    schedule_interval='* * * * *',
    catchup=False,
    tags=['etl'],
)
def etl_pipeline():

    @task
    def create_table_task():
        if not create_table():
            raise AirflowFailException("Table creation failed")
        return "Table ready"

    @task
    def extract_task():
        data = extract()
        if not data:
            raise ValueError("No data extracted")
        return data

    @task
    def transform_task(data):
        return transform_data(data)

    @task
    def load_task(transformed_data):
        load_data(transformed_data)
        return "Load finished"

    # Workflow definition
    create_table_task() >> load_task(transform_task(extract_task()))

etl_pipeline_dag = etl_pipeline()