from airflow.decorators import dag, task
from datetime import datetime,timedelta
from utilis.broker_check import check_kafka_health
from pipeline.extract import extract as extract_func
from pipeline.transform import transform_and_stream
from pipeline.load import load_data
from pipeline.table_create import create_table
from utilis.db_loaded_check import verify_database_insertion
from utilis.raw_topic_test import run_all_tests 
from utilis.transformed_topic_test import check_transformed_stream

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['yannainghtun565@gmail.com'], 
    'retries': 3,  
    'retry_delay': timedelta(minutes=5), 
    'retry_exponential_backoff': True,  
    'max_retry_delay': timedelta(minutes=30), 
}

@dag(
    dag_id="kafka_health",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly', 
    catchup=False,  
    max_active_runs=1,  
    tags=['kafka', 'health', 'monitoring'],
    description='Kafka health and data quality checks',
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
    def raw_topic_test():
        return run_all_tests()

    @task
    def transform_task():
        return transform_and_stream()
 
    @task
    def transformed_topic_test():
        return check_transformed_stream()

    @task
    def load_task():
        return load_data()
    
    @task
    def db_check_task():
        return verify_database_insertion()

    check = run_kafka_check()
    create_tab = table_check()
    extract_t = extract_task()
    raw_t = raw_topic_test()
    transform_t = transform_task()
    transformed_t = transformed_topic_test()
    load_t = load_task()
    db_check = db_check_task()


    check >> create_tab >> extract_t >> raw_t >>transform_t >> transformed_t >> load_t >> db_check


dag = weather_pipeline()