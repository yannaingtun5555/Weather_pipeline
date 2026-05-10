from airflow.decorators import dag, task
from datetime import datetime
from utilis.broker_check import check_kafka_health
from pipeline.extract import extract as extract_func
from pipeline.transform import transform_and_stream

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
    def extract_task():
        return extract_func()   # ✔ correct

    @task
    def transform_task():
        return transform_and_stream()

    check = run_kafka_check()
    extract_t = extract_task()
    transform_t = transform_task()

    check >> extract_t >> transform_t


dag = weather_pipeline()