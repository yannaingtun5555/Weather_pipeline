from airflow.decorators import dag, task
from datetime import datetime
from utilis.broker_check import check_kafka_health
from pipeline.extract import extract as extract_func
from pipeline.transform import transform_and_stream
from pipeline.load import load_data
from pipeline.table_create import create_table
from utilis.db_loaded_check import verify_database_insertion
@dag(
    dag_id="kafka_health",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def weather_pipeline():

    @task
    def run_kafka_check():
        return check_kafka_health(
            bootstrap_servers="kafka1:9092,kafka2:9092"
        )

    @task
    def table_check():
        return create_table()
    
    @task
    def extract_task():
        return extract_func()   # ✔ correct

    @task
    def transform_task():
        return transform_and_stream()
 
    @task
    def load_task():
        return load_data()
    
    @task
    def db_check_task():
        return verify_database_insertion()

    check = run_kafka_check()
    create_tab = table_check()
    extract_t = extract_task()
    transform_t = transform_task()
    load_t = load_task()
    db_check = db_check_task()
    check >> create_tab >> extract_t >> transform_t >> load_t >> db_check


dag = weather_pipeline()